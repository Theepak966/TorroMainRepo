import os
import sys
from typing import Dict, Optional


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config


airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
utils_dir = os.path.join(airflow_dir, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

try:
    from utils.storage_path_parser import parse_storage_path
    PARSER_AVAILABLE = True
except ImportError:
    PARSER_AVAILABLE = False
    import logging
    logger = logging.getLogger(__name__)
    logger.warning('FN:azure_config storage_path_parser not available, using legacy path handling')


AZURE_STORAGE_ACCOUNTS = config.AZURE_STORAGE_ACCOUNTS
DISCOVERY_CONFIG = config.DISCOVERY_CONFIG
DB_CONFIG = config.DB_CONFIG
AZURE_AI_LANGUAGE_CONFIG = config.AZURE_AI_LANGUAGE_CONFIG





def get_storage_location_json(account_name: str, container: str, blob_path: str, 
                              connection_string: Optional[str] = None) -> Dict:

    if PARSER_AVAILABLE:
        try:
            parsed = parse_storage_path(blob_path, account_name, container)
            


            result = {
                "type": parsed.get("type", "azure_blob"),
                "path": parsed.get("path", blob_path),
                "connection": parsed.get("connection", {}),
                "container": parsed.get("container_info", {}),
                "metadata": parsed.get("metadata", {})
            }
            

            if connection_string:
                result["connection"]["connection_string"] = connection_string
            elif not result["connection"].get("connection_string"):

                result["connection"]["connection_string"] = config.AZURE_STORAGE_CONNECTION_STRING
            

            if not result["connection"].get("account_name"):
                result["connection"]["account_name"] = parsed.get("account_name") or account_name
            

            if parsed.get("protocol"):
                result["metadata"]["protocol"] = parsed.get("protocol")
            if parsed.get("full_url"):
                result["metadata"]["full_url"] = parsed.get("full_url")
            
            return result
        except (ValueError, Exception) as e:

            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f'FN:get_storage_location_json path:{blob_path} parser_failed:{str(e)} using_legacy')
    

    return {
        "type": "azure_blob",
        "path": blob_path,
        "connection": {
            "method": "connection_string",
            "connection_string": connection_string or config.AZURE_STORAGE_CONNECTION_STRING,
            "account_name": account_name
        },
        "container": {
            "name": container,
            "type": "blob_container"
        },
        "metadata": {}
    }


def get_azure_connections_from_db():
    import pymysql
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    
    db_conn = None
    try:
        db_conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8mb4'
        )
        with db_conn.cursor() as cursor:
            sql = """
                SELECT id, name, connector_type, connection_type, config, status
                FROM connections
                WHERE connector_type = 'azure_blob' AND status = 'active'