from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import pymysql
import sys
import os
import time
from functools import wraps


airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if airflow_dir not in sys.path:
    sys.path.insert(0, airflow_dir)

if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())


from config import config as airflow_config

from config import config as airflow_config
from config.azure_config import (
    AZURE_STORAGE_ACCOUNTS,
    DB_CONFIG,
    get_storage_location_json,
    get_azure_connections_from_db,
)
from utils.azure_blob_client import AzureBlobClient, create_azure_blob_client
from utils.metadata_extractor import extract_file_metadata, generate_file_hash, generate_schema_hash
from utils.deduplication import check_file_exists, check_asset_exists, should_update_or_insert, get_db_connection
from utils.email_notifier import notify_new_discoveries

logger = logging.getLogger(__name__)


def retry_db_operation(max_retries: int = None, base_delay: float = 1.0, max_delay: float = 60.0, max_total_time: float = 3600.0):

    if max_retries is None:
        max_retries = airflow_config.DB_RETRY_MAX_ATTEMPTS

        if max_retries == 0:
            max_retries = -1
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            start_time = time.time()
            attempt = 0
            
            while True:

                elapsed_time = time.time() - start_time
                if elapsed_time >= max_total_time:
                    logger.error('FN:retry_db_operation max_total_time:{} attempt:{}'.format(max_total_time, attempt))
                    if last_exception:
                        raise last_exception
                    raise TimeoutError(f"Operation timed out after {max_total_time}s")
                

                if max_retries > 0 and attempt >= max_retries:
                    logger.error('FN:retry_db_operation max_retries:{} attempt:{} error:{}'.format(max_retries, attempt, str(last_exception) if last_exception else 'unknown error'))
                    if last_exception:
                        raise last_exception
                    raise Exception("Max retries exceeded")
                
                try:
                    return func(*args, **kwargs)
                except (pymysql.Error, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    error_code = getattr(e, 'args', [0])[0] if hasattr(e, 'args') and e.args else None
                    

                    retryable_errors = [
                        2006,
                        2013,
                        1205,
                        1213,
                        1040,
                    ]
                    

                    if error_code not in retryable_errors:
                        logger.error('FN:retry_db_operation error_code:{} error:{}'.format(error_code, str(e)))
                        raise
                    

                    delay = min(base_delay * (2 ** min(attempt, 10)), max_delay)
                    

                    if elapsed_time + delay >= max_total_time:
                        logger.error('FN:retry_db_operation max_total_time:{} elapsed_time:{} delay:{}'.format(max_total_time, elapsed_time, delay))
                        raise
                    
                    retry_info = f"attempt {attempt + 1}"
                    if max_retries > 0:
                        retry_info += f"/{max_retries}"
                    else:
                        retry_info += " (unlimited, max 1h timeout)"
                    
                    logger.warning('FN:retry_db_operation retry_info:{} error:{} delay:{} elapsed_time:{}'.format(retry_info, str(e), delay, elapsed_time))
                    time.sleep(delay)
                    attempt += 1
                    
                except Exception as e:

                    logger.error('FN:retry_db_operation error:{}'.format(str(e)))
                    raise
            
            if last_exception:
                raise last_exception
        return wrapper
    return decorator





def discover_azure_blobs(**context):
    dag_run = context['dag_run']
    run_id = dag_run.run_id
    discovery_batch_id = f"batch-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    batch_start_time = datetime.utcnow()
    
    logger.info('FN:discover_azure_blobs discovery_batch_id:{} run_id:{}'.format(discovery_batch_id, run_id))
    
    all_new_discoveries = []
    

    try:
        db_connections = get_azure_connections_from_db()
    except Exception as e:
        logger.error('FN:discover_azure_blobs error_getting_connections:{}'.format(str(e)))

        db_connections = []
        logger.warning('FN:discover_azure_blobs falling_back_to_env_vars')
    

    for db_conn in db_connections:
        connection_id = db_conn["id"]
        connection_name = db_conn["name"]
        config_data = json.loads(db_conn["config"]) if isinstance(db_conn["config"], str) else db_conn["config"]

        connection_string = config_data.get("connection_string")
        account_name = config_data.get("account_name", connection_name)
        tenant_id = config_data.get("tenant_id")
        client_id = config_data.get("client_id")
        client_secret = config_data.get("client_secret")
        storage_type = config_data.get("storage_type", "blob")
        containers = config_data.get("containers", [])
        folder_path = config_data.get("folder_path", "")
        folders = [folder_path] if folder_path else [""]
        

        client_config = {
            "connection_string": connection_string,
            "account_name": account_name,
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "storage_type": storage_type,
        }
        

        if not containers:
            try:
                blob_client_temp = create_azure_blob_client(client_config)
                containers_list = blob_client_temp.list_containers()
                containers = [c["name"] for c in containers_list]
                logger.info('FN:discover_azure_blobs connection_id:{} connection_name:{} auto_discovered_containers:{}'.format(
                    connection_id, connection_name, containers))
            except Exception as e:
                logger.warning('FN:discover_azure_blobs connection_id:{} connection_name:{} error_discovering_containers:{}'.format(
                    connection_id, connection_name, str(e)))
                continue
        
        if not containers:
            logger.warning('FN:discover_azure_blobs connection_id:{} connection_name:{} no_containers_available'.format(connection_id, connection_name))
            continue
        
        logger.info('FN:discover_azure_blobs connection_id:{} connection_name:{} account_name:{} containers:{}'.format(
            connection_id, connection_name, account_name, containers))
        
        try:
            blob_client = create_azure_blob_client(client_config)
            
            for container_name in containers:
                logger.info('FN:discover_azure_blobs container_name:{}'.format(container_name))
                
                for folder_path in folders:
                    logger.info('FN:discover_azure_blobs folder_path:{}'.format(folder_path))
                    
                    try:

                        is_datalake = storage_type == 'datalake' or config_data.get('use_dfs_endpoint', False)
                        if is_datalake and hasattr(blob_client, 'list_datalake_files'):

                            blobs = blob_client.list_datalake_files(
                                file_system_name=container_name,
                                path=folder_path,
                                file_extensions=None
                            )
                            logger.info('FN:discover_azure_blobs container_name:{} message:Using Data Lake Gen2 API'.format(container_name))
                        else:

                            blobs = blob_client.list_blobs(
                                container_name=container_name,
                                folder_path=folder_path,
                                file_extensions=None
                            )
                            logger.info('FN:discover_azure_blobs container_name:{} message:Using Blob Storage API'.format(container_name))
                        
                        logger.info('FN:discover_azure_blobs container_name:{} folder_path:{} blob_count:{}'.format(container_name, folder_path, len(blobs)))
                        


                        batch_size = 500
                        
                        for batch_start in range(0, len(blobs), batch_size):

                            batch_processed = 0
                            batch_skipped = 0
                            batch_new = 0
                            batch_end = min(batch_start + batch_size, len(blobs))
                            batch = blobs[batch_start:batch_end]
                            
                            logger.info('FN:discover_azure_blobs processing_batch:{}-{} of {}'.format(batch_start, batch_end, len(blobs)))
                            
                            for blob_info in batch:
                                try:
                                    blob_path = blob_info["full_path"]
                                    connector_id = f"azure_blob_{connection_name}"
                                    

                                    existing_record = retry_db_operation(max_retries=None, base_delay=1.0, max_delay=60.0, max_total_time=3600.0)(
                                        check_asset_exists
                                    )(
                                        connector_id=connector_id,
                                        storage_path=blob_path
                                    )
                                    

                                    file_size = blob_info.get("size", 0)
                                    etag = blob_info.get("etag", "").strip('"')
                                    last_modified = blob_info.get("last_modified")
                                    

                                    composite_string = f"{etag}_{file_size}_{last_modified.isoformat() if last_modified else ''}"
                                    file_hash = generate_file_hash(composite_string.encode('utf-8'))
                                    



                                    file_sample = None
                                    file_extension = blob_info["name"].split(".")[-1].lower() if "." in blob_info["name"] else ""
                                    
                                    try:
                                        if file_extension == "parquet":

                                            file_sample = blob_client.get_blob_tail(container_name, blob_path, max_bytes=8192)
                                            logger.info('FN:discover_azure_blobs blob_path:{} file_extension:{} sample_bytes:{}'.format(blob_path, file_extension, len(file_sample)))
                                        else:

                                            file_sample = blob_client.get_blob_sample(container_name, blob_path, max_bytes=1024)
                                            logger.info('FN:discover_azure_blobs blob_path:{} file_extension:{} sample_bytes:{}'.format(blob_path, file_extension, len(file_sample)))
                                    except Exception as e:
                                        logger.warning('FN:discover_azure_blobs blob_path:{} error:{}'.format(blob_path, str(e)))
                                    


                                    if file_sample:
                                        metadata = extract_file_metadata(blob_info, file_sample)
                                        schema_hash = metadata.get("schema_hash", generate_schema_hash({}))
                                    else:

                                        schema_hash = generate_schema_hash({})
                                        metadata = {
                                            "file_metadata": {
                                                "basic": {
                                                    "name": blob_info["name"],
                                                    "extension": "." + blob_info["name"].split(".")[-1] if "." in blob_info["name"] else "",
                                                    "format": blob_info["name"].split(".")[-1].lower() if "." in blob_info["name"] else "unknown",
                                                    "size_bytes": file_size,
                                                    "content_type": blob_info.get("content_type", "application/octet-stream"),
                                                    "mime_type": blob_info.get("content_type", "application/octet-stream")
                                                },
                                                "hash": {
                                                    "algorithm": "shake128_etag_composite",
                                                    "value": file_hash,
                                                    "computed_at": datetime.utcnow().isoformat() + "Z",
                                                    "source": "etag_composite"
                                                },
                                                "timestamps": {
                                                    "created_at": blob_info["created_at"].isoformat() if blob_info.get("created_at") else None,
                                                    "last_modified": blob_info["last_modified"].isoformat() if blob_info.get("last_modified") else None
                                                }
                                            },
                                            "schema_json": {},
                                            "schema_hash": schema_hash,
                                            "file_hash": file_hash,
                                            "storage_metadata": {
                                                "azure": {
                                                    "type": blob_info.get("blob_type", "Block blob"),
                                                    "etag": etag,
                                                    "access_tier": blob_info.get("access_tier"),
                                                    "creation_time": blob_info["created_at"].isoformat() if blob_info.get("created_at") else None,
                                                    "last_modified": blob_info["last_modified"].isoformat() if blob_info.get("last_modified") else None,
                                                    "lease_status": blob_info.get("lease_status"),
                                                    "content_encoding": blob_info.get("content_encoding"),
                                                    "content_language": blob_info.get("content_language"),
                                                    "cache_control": blob_info.get("cache_control"),
                                                    "metadata": blob_info.get("metadata", {})
                                                }
                                            }
                                        }
                                    

                                    if "file_hash" not in metadata:
                                        metadata["file_hash"] = file_hash
                                    
                                    file_metadata = metadata.get("file_metadata")
                                    
                                    should_update, schema_changed = should_update_or_insert(existing_record, file_hash, schema_hash)
                                    

                                    if existing_record:
                                        if not should_update:
                                            batch_skipped += 1
                                            if batch_skipped % 50 == 0:
                                                logger.info('FN:discover_azure_blobs blob_path:{} existing_asset_id:{} skipped_count:{} message:Skipping unchanged asset'.format(blob_path, existing_record.get('id'), batch_skipped))
                                            continue
                                        else:
                                            logger.info('FN:discover_azure_blobs blob_path:{} existing_asset_id:{} schema_changed:{} message:Updating existing asset'.format(blob_path, existing_record.get('id'), schema_changed))
                                    
                                    if not should_update and not existing_record:

                                        logger.warning('FN:discover_azure_blobs blob_path:{} should_update:{} existing_record:{} message:Unexpected state'.format(blob_path, should_update, bool(existing_record)))
                                        continue
                                    
                                    storage_location = get_storage_location_json(
                                        account_name=account_name,
                                        container=container_name,
                                        blob_path=blob_path
                                    )
                                    
                                    discovery_info = {
                                        "batch": {
                                            "id": discovery_batch_id,
                                            "started_at": batch_start_time.isoformat() + "Z"
                                        },
                                        "source": {
                                            "type": "airflow_dag",
                                            "name": "azure_blob_discovery_dag",
                                            "run_id": run_id
                                        },
                                        "scan": {
                                            "container": container_name,
                                            "folder": folder_path
                                        }
                                    }
                                    

                                    def _execute_db_write():
                                        nonlocal batch_new, batch_processed
                                        conn = None
                                        try:
                                            conn = get_db_connection()
                                            with conn.cursor() as cursor:
                                                file_extension = blob_info["name"].split(".")[-1].lower() if "." in blob_info["name"] else ""
                                                asset_id = f"azure_blob_{connection_name}_{blob_path.replace('/', '_').replace(' ', '_')}_{int(datetime.now().timestamp())}"
                                                current_date = datetime.utcnow().isoformat()
                                                
                                                technical_metadata = {
                                                    "asset_id": asset_id,
                                                    "asset_type": file_extension or "blob",
                                                    "format": file_extension or "unknown",
                                                    "content_type": blob_info.get("content_type", "application/octet-stream"),
                                                    "size_bytes": blob_info.get("size", 0),
                                                    "location": blob_path,
                                                    "container": container_name,
                                                    "storage_account": account_name,
                                                    "created_at": current_date,
                                                    "file_extension": f".{file_extension}" if file_extension else "",
                                                    "file_hash": file_hash,
                                                    "schema_hash": schema_hash,
                                                    **metadata.get("file_metadata", {}).get("format_specific", {})
                                                }
                                                
                                                operational_metadata = {
                                                    "owner": "system",
                                                    "created_by": "airflow_dag",
                                                    "last_updated_by": "airflow_dag",
                                                    "last_updated_at": current_date,
                                                    "access_level": "internal",
                                                    "approval_status": "pending_review",
                                                }
                                                
                                                business_metadata = {
                                                    "description": f"Azure Blob Storage file: {blob_info['name']}",
                                                    "data_type": file_extension or "unknown",
                                                    "business_owner": "system",
                                                    "department": "Data Engineering",
                                                    "classification": "internal",
                                                    "sensitivity_level": "medium",
                                                    "tags": [],
                                                }
                                                
                                                if existing_record:
                                                    if schema_changed:

                                                        update_sql = """
                                                            UPDATE assets
                                                            SET name = %s,
                                                                type = %s,
                                                                technical_metadata = %s,
                                                                columns = %s,
                                                                operational_metadata = %s
                                                            WHERE id = %s
                                                        INSERT INTO assets (
                                                            id, name, type, catalog, connector_id, discovered_at,
                                                            technical_metadata, operational_metadata, business_metadata, columns
                                                        ) VALUES (
                                                            %s, %s, %s, %s, %s, NOW(),
                                                            %s, %s, %s, %s
                                                        )
                                            INSERT INTO assets (name, type, catalog, connector_id, storage_location, columns, business_metadata, technical_metadata, created_at, updated_at)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                            INSERT INTO data_discovery (asset_id, storage_location, file_metadata, schema_json, schema_hash, status, approval_status, discovered_at, folder_path, data_source_type, environment, discovery_info)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                                    INSERT INTO assets (name, type, catalog, connector_id, storage_location, columns, business_metadata, technical_metadata, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                    INSERT INTO data_discovery (asset_id, storage_location, file_metadata, schema_json, schema_hash, status, approval_status, discovered_at, folder_path, data_source_type, environment, discovery_info)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                                    INSERT INTO assets (name, type, catalog, connector_id, storage_location, columns, business_metadata, technical_metadata, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                    INSERT INTO data_discovery (asset_id, storage_location, file_metadata, schema_json, schema_hash, status, approval_status, discovered_at, folder_path, data_source_type, environment, discovery_info)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)