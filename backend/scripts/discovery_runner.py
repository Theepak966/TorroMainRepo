import os
import sys
import logging
import json
import pymysql
from datetime import datetime
from typing import List, Dict, Optional


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger(__name__)

try:

    from backend.utils.azure_blob_client import AzureBlobClient
    from backend.utils.metadata_extractor import extract_file_metadata, generate_file_hash, generate_schema_hash
    from backend.utils.asset_deduplication import check_asset_exists, should_update_or_insert
    from backend.database import SessionLocal
    from backend.models import Connection, Asset
    from dotenv import load_dotenv
    from pathlib import Path
    import os as os_module
    

    backend_dir = Path(__file__).parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    

    DB_CONFIG = {
        'host': os_module.getenv('DB_HOST', ''),
        'port': int(os_module.getenv('DB_PORT', 3306)),
        'user': os_module.getenv('DB_USER', 'root'),
        'password': os_module.getenv('DB_PASSWORD', ''),
        'database': os_module.getenv('DB_NAME', 'torroforairflow'),
        'charset': 'utf8mb4'
    }
    
    def get_db_connection():
        return pymysql.connect(**DB_CONFIG)
    
    DISCOVERY_AVAILABLE = True
except ImportError as e:
    logger.error('FN:__init__ message:Discovery utilities not available error:{}'.format(str(e)))
    DISCOVERY_AVAILABLE = False


def run_discovery_for_connection(connection_id: int):
    if not DISCOVERY_AVAILABLE:
        logger.error('FN:run_discovery_for_connection message:Discovery utilities not available')
        return
    
    conn = None
    try:

        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
                SELECT id, name, connector_type, connection_type, config, status
                FROM connections
                WHERE id = %s AND status = 'active'
            """
            
            cursor.execute(sql, (connection_id,))
            result = cursor.fetchone()
            
            if not result:
                logger.warning(f'FN:run_discovery_for_connection connection_id:{connection_id} message:Connection not found or not active')
                return
            
            connection_id_db, name, connector_type, connection_type, config, status = result
            
            # Handle Oracle DB discovery
            if connector_type == 'oracle_db':
                try:
                    from backend.utils.oracle_db_client import OracleDBClient
                    from backend.utils.asset_deduplication import check_asset_exists, should_update_or_insert
                    from backend.models import Asset
                    
                    db = SessionLocal()
                    try:
                        client = OracleDBClient(config)
                        schemas = client.list_schemas()
                        
                        total_processed = 0
                        total_new = 0
                        total_updated = 0
                        total_skipped = 0
                        
                        for schema in schemas:
                            tables = client.list_tables(schema)
                            
                            for table_info in tables:
                                try:
                                    table_name = table_info['table_name']
                                    asset_id = f"oracle_db_{name}_{schema}.{table_name}"
                                    existing_asset = check_asset_exists(db, f"oracle_db_{name}", f"{schema}.{table_name}")
                                    
                                    metadata = client.get_table_metadata(schema, table_name)
                                    columns = metadata.get('columns', [])
                                    schema_hash = hash(str(sorted([c['name'] for c in columns])))
                                    
                                    should_update, schema_changed = should_update_or_insert(
                                        existing_asset,
                                        None,
                                        schema_hash
                                    )
                                    
                                    if not should_update:
                                        total_skipped += 1
                                        continue
                                    
                                    asset_data = {
                                        "id": asset_id,
                                        "name": table_name,
                                        "type": "table",
                                        "catalog": schema,
                                        "connector_id": f"oracle_db_{name}",
                                        "technical_metadata": {
                                            "database_type": "oracle",
                                            "schema": schema,
                                            "table_name": table_name,
                                            "row_count": metadata.get('row_count', 0),
                                            "column_count": metadata.get('column_count', 0),
                                            "tablespace": metadata.get('tablespace'),
                                            "last_analyzed": metadata.get('last_analyzed'),
                                        },
                                        "operational_metadata": {"approval_status": "pending_review"},
                                        "business_metadata": {},
                                        "columns": columns
                                    }
                                    
                                    if existing_asset:
                                        existing_asset.name = asset_data["name"]
                                        existing_asset.type = asset_data["type"]
                                        existing_asset.technical_metadata = asset_data["technical_metadata"]
                                        existing_asset.columns = asset_data["columns"]
                                        total_updated += 1
                                    else:
                                        new_asset = Asset(**asset_data)
                                        db.add(new_asset)
                                        total_new += 1
                                    
                                    total_processed += 1
                                    
                                except Exception as e:
                                    logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} schema:{schema} table:{table_info.get("table_name", "unknown")} error:{str(e)}')
                                    continue
                            
                            db.commit()
                        
                        client.close()
                        logger.info(f'FN:run_discovery_for_connection connection_id:{connection_id} connector_type:oracle_db total_processed:{total_processed} total_new:{total_new} total_updated:{total_updated} total_skipped:{total_skipped}')
                        
                    except Exception as e:
                        if db:
                            db.rollback()
                        logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} connector_type:oracle_db error:{str(e)}', exc_info=True)
                    finally:
                        if db:
                            db.close()
                        if 'client' in locals():
                            client.close()
                
                except ImportError as e:
                    logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} connector_type:oracle_db import_error:{str(e)}')
                return
            
            # Azure Blob discovery (existing logic)
            if connector_type != 'azure_blob':
                logger.warning(f'FN:run_discovery_for_connection connection_id:{connection_id} connector_type:{connector_type} message:Connector type not supported')
                return
            
            db = SessionLocal()
            try:
                connection = db.query(Connection).filter(Connection.id == connection_id).first()
                if not connection:
                    logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} message:Connection not found in database')
                    return
                
                blob_client = AzureBlobClient(config)
                containers = blob_client.list_containers()
                
                total_processed = 0
                total_new = 0
                total_updated = 0
                total_skipped = 0
                
                for container_name in containers:
                    blobs = blob_client.list_blobs(container_name)
                    
                    for blob_info in blobs:
                        try:
                            blob_path = blob_info.get("full_path", blob_info.get("name", ""))
                            existing_asset = check_asset_exists(db, f"azure_blob_{name}", blob_path)
                            
                            file_extension = blob_info.get("name", "").split(".")[-1].lower() if "." in blob_info.get("name", "") else "blob"
                            
                            metadata = extract_file_metadata(blob_info, None)
                            file_hash = metadata.get("file_hash", generate_file_hash(b""))
                            schema_hash = metadata.get("schema_hash", generate_schema_hash({}))
                            
                            should_update, schema_changed = should_update_or_insert(
                                existing_asset,
                                file_hash,
                                schema_hash
                            )
                            
                            if not should_update:
                                total_skipped += 1
                                continue
                            
                            asset_data = {
                                "id": f"azure_blob_{name}_{blob_path}",
                                "name": blob_info.get("name", ""),
                                "type": file_extension,
                                "catalog": container_name,
                                "connector_id": f"azure_blob_{name}",
                                "technical_metadata": metadata.get("technical_metadata", {}),
                                "operational_metadata": {"approval_status": "pending_review"},
                                "business_metadata": {},
                                "columns": metadata.get("schema_json", {}).get("columns", [])
                            }
                            
                            if existing_asset:
                                existing_asset.name = asset_data["name"]
                                existing_asset.type = asset_data["type"]
                                existing_asset.technical_metadata = asset_data["technical_metadata"]
                                existing_asset.columns = asset_data["columns"]
                                existing_asset.operational_metadata = asset_data["operational_metadata"]
                                total_updated += 1
                            else:
                                new_asset = Asset(**asset_data)
                                db.add(new_asset)
                                total_new += 1
                            
                            total_processed += 1
                            
                        except Exception as e:
                            logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} blob_path:{blob_info.get("name", "unknown")} error:{str(e)}')
                            continue
                
                db.commit()
                logger.info(f'FN:run_discovery_for_connection connection_id:{connection_id} total_processed:{total_processed} total_new:{total_new} total_updated:{total_updated} total_skipped:{total_skipped}')
                
            except Exception as e:
                if db:
                    db.rollback()
                logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} error:{str(e)}', exc_info=True)
            finally:
                if db:
                    db.close()
    except Exception as e:
        logger.error(f'FN:run_discovery_for_connection connection_id:{connection_id} error:{str(e)}', exc_info=True)
    finally:
        if conn:
            conn.close()