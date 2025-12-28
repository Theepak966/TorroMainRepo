"""
Discovery runner that uses the same logic as Airflow DAG
Runs discovery for a specific connection
"""
import os
import sys
import logging
import json
import pymysql
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger(__name__)

try:
    # Import from backend utils
    from backend.utils.azure_blob_client import AzureBlobClient
    from backend.utils.metadata_extractor import extract_file_metadata, generate_file_hash, generate_schema_hash
    from backend.utils.asset_deduplication import check_asset_exists, should_update_or_insert
    from backend.database import SessionLocal
    from backend.models import Connection, Asset
    from dotenv import load_dotenv
    from pathlib import Path
    import os as os_module
    
    # Load .env from backend directory
    backend_dir = Path(__file__).parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    
    # Get DB config from environment
    DB_CONFIG = {
        'host': os_module.getenv('DB_HOST', 'localhost'),
        'port': int(os_module.getenv('DB_PORT', 3306)),
        'user': os_module.getenv('DB_USER', 'root'),
        'password': os_module.getenv('DB_PASSWORD', ''),
        'database': os_module.getenv('DB_NAME', 'torroforexcel'),
        'charset': 'utf8mb4'
    }
    
    def get_db_connection():
        """Get database connection"""
        return pymysql.connect(**DB_CONFIG)
    
    DISCOVERY_AVAILABLE = True
except ImportError as e:
    logger.error('FN:__init__ message:Discovery utilities not available error:{}'.format(str(e)))
    DISCOVERY_AVAILABLE = False


def run_discovery_for_connection(connection_id: int):
    """
    Run discovery for a specific connection using Airflow DAG logic
    This is the same discovery logic that runs in the Airflow DAG
    """
    if not DISCOVERY_AVAILABLE:
        logger.error('FN:run_discovery_for_connection message:Discovery utilities not available')
        return
    
    conn = None
    try:
        # Get connection from database
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
                SELECT id, name, connector_type, connection_type, config, status
                FROM connections
                WHERE id = %s AND connector_type = 'azure_blob' AND status = 'active'
            """
            cursor.execute(sql, (connection_id,))
            db_conn = cursor.fetchone()
            
            if not db_conn:
                logger.warning('FN:run_discovery_for_connection connection_id:{} message:Connection not found or not active'.format(connection_id))
                return
            
            connection_name = db_conn["name"]
            config_data = json.loads(db_conn["config"]) if isinstance(db_conn["config"], str) else db_conn["config"]
            account_name = config_data.get("account_name", connection_name)
            containers = config_data.get("containers", [])
            folder_path = config_data.get("folder_path", "")
            folders = [folder_path] if folder_path else [""]
            
            # Create Azure Blob Client (supports both connection string and service principal)
            try:
                from backend.utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(config_data)
            except ValueError as e:
                logger.warning('FN:run_discovery_for_connection connection_id:{} message:Invalid config error:{}'.format(connection_id, str(e)))
                return
            except Exception as e:
                logger.error('FN:run_discovery_for_connection connection_id:{} message:Error creating blob client error:{}'.format(connection_id, str(e)))
                return
            
            # Auto-discover containers if not specified
            if not containers:
                try:
                    containers_list = blob_client.list_containers()
                    containers = [c["name"] for c in containers_list]
                    logger.info('FN:run_discovery_for_connection connection_id:{} auto_discovered_containers_count:{}'.format(connection_id, len(containers)))
                except Exception as e:
                    logger.error('FN:run_discovery_for_connection connection_id:{} message:Error discovering containers error:{}'.format(connection_id, str(e)))
                    return
            
            if not containers:
                logger.warning('FN:run_discovery_for_connection connection_id:{} message:No containers available'.format(connection_id))
                return
            
            logger.info('FN:run_discovery_for_connection connection_id:{} connection_name:{} message:Starting discovery'.format(connection_id, connection_name))
            connector_id = f"azure_blob_{connection_name}"
            total_new = 0
            total_updated = 0
            total_skipped = 0
            
            for container_name in containers:
                logger.info('FN:run_discovery_for_connection container_name:{} message:Discovering container'.format(container_name))
                
                for folder_path in folders:
                    try:
                        blobs = blob_client.list_blobs(
                            container_name=container_name,
                            folder_path=folder_path,
                            file_extensions=None
                        )
                        
                        logger.info('FN:run_discovery_for_connection container_name:{} folder_path:{} blob_count:{}'.format(container_name, folder_path, len(blobs)))
                        
                        for blob_info in blobs:
                            try:
                                blob_path = blob_info["full_path"]
                                
                                # Check if asset exists (deduplication)
                                existing_record = None
                                try:
                                    # Use database session for check_asset_exists
                                    db_session = SessionLocal()
                                    try:
                                        existing_asset = check_asset_exists(db_session, connector_id, blob_path)
                                        if existing_asset:
                                            # Convert Asset object to dict format expected by should_update_or_insert
                                            tech_meta = existing_asset.technical_metadata or {}
                                            existing_record = {
                                                "id": existing_asset.id,
                                                "file_hash": tech_meta.get("file_hash") or tech_meta.get("hash", {}).get("value") if isinstance(tech_meta.get("hash"), dict) else None,
                                                "schema_hash": tech_meta.get("schema_hash")
                                            }
                                    finally:
                                        db_session.close()
                                except Exception as e:
                                    logger.warning('FN:run_discovery_for_connection blob_path:{} message:Error checking asset existence treating as new error:{}'.format(blob_path, str(e)))
                                    existing_record = None
                                
                                # Get file sample for metadata
                                file_extension = blob_info["name"].split(".")[-1].lower() if "." in blob_info["name"] else ""
                                file_sample = None
                                
                                try:
                                    if file_extension == "parquet":
                                        file_sample = blob_client.get_blob_tail(container_name, blob_path, max_bytes=8192)
                                    else:
                                        file_sample = blob_client.get_blob_sample(container_name, blob_path, max_bytes=1024)
                                except Exception as e:
                                    logger.warning('FN:run_discovery_for_connection container_name:{} blob_path:{} message:Could not get sample error:{}'.format(container_name, blob_path, str(e)))
                                
                                # Extract metadata
                                if file_sample:
                                    metadata = extract_file_metadata(blob_info, file_sample)
                                else:
                                    metadata = extract_file_metadata(blob_info, None)
                                
                                # Get hashes
                                file_hash = metadata.get("file_hash", generate_file_hash(b""))
                                schema_hash = metadata.get("schema_hash", generate_schema_hash({}))
                                
                                # Check if should update
                                should_update, schema_changed = should_update_or_insert(
                                    existing_record,
                                    file_hash,
                                    schema_hash
                                )
                                
                                if not should_update and existing_record:
                                    total_skipped += 1
                                    continue
                                
                                # Prepare asset data
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
                                
                                # Save to database
                                with conn.cursor() as write_cursor:
                                    if existing_record and schema_changed:
                                        # Update existing
                                        update_sql = """
                                            UPDATE assets
                                            SET name = %s,
                                                type = %s,
                                                technical_metadata = %s,
                                                columns = %s,
                                                operational_metadata = %s
                                            WHERE id = %s
                                        """
                                        write_cursor.execute(update_sql, (
                                            blob_info["name"],
                                            file_extension or "blob",
                                            json.dumps(technical_metadata),
                                            json.dumps(metadata.get("schema_json", {}).get("columns", [])),
                                            json.dumps(operational_metadata),
                                            existing_record["id"]
                                        ))
                                        total_updated += 1
                                    else:
                                        # Insert new
                                        insert_sql = """
                                            INSERT INTO assets (
                                                id, name, type, catalog, connector_id, discovered_at,
                                                technical_metadata, operational_metadata, business_metadata, columns
                                            ) VALUES (
                                                %s, %s, %s, %s, %s, NOW(),
                                                %s, %s, %s, %s
                                            )
                                        """
                                        write_cursor.execute(insert_sql, (
                                            asset_id,
                                            blob_info["name"],
                                            file_extension or "blob",
                                            connection_name,
                                            connector_id,
                                            json.dumps(technical_metadata),
                                            json.dumps(operational_metadata),
                                            json.dumps(business_metadata),
                                            json.dumps(metadata.get("schema_json", {}).get("columns", [])),
                                        ))
                                        total_new += 1
                                
                                conn.commit()
                                
                            except Exception as e:
                                logger.error('FN:run_discovery_for_connection container_name:{} blob_name:{} error:{}'.format(container_name, blob_info.get('name', 'unknown'), str(e)))
                                continue
                    
                    except Exception as e:
                        logger.error('FN:run_discovery_for_connection container_name:{} folder_path:{} message:Error listing blobs error:{}'.format(container_name, folder_path, str(e)))
                        continue
            
            logger.info('FN:run_discovery_for_connection connection_id:{} new_count:{} updated_count:{} skipped_count:{} message:Discovery complete'.format(connection_id, total_new, total_updated, total_skipped))
            
    except Exception as e:
        logger.error('FN:run_discovery_for_connection connection_id:{} error:{}'.format(connection_id, str(e)), exc_info=True)
    finally:
        if conn:
            conn.close()

