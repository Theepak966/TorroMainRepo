import pymysql
import logging
import json
from typing import Dict, Optional, Tuple
import sys
import os
import time
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.azure_config import DB_CONFIG

logger = logging.getLogger(__name__)


def retry_db_operation(max_retries: int = None, base_delay: float = 1.0, max_delay: float = 60.0, max_total_time: float = 3600.0):

    if max_retries is None:

        import sys
        airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if airflow_dir not in sys.path:
            sys.path.insert(0, airflow_dir)
        from config import config
        max_retries = config.DB_RETRY_MAX_ATTEMPTS

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


@retry_db_operation(max_retries=None, base_delay=1.0, max_delay=60.0, max_total_time=3600.0)
def get_db_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8mb4'
    )


@retry_db_operation(max_retries=None, base_delay=1.0, max_delay=60.0, max_total_time=3600.0)
def check_file_exists(
    storage_type: str,
    storage_identifier: str,
    storage_path: str
) -> Optional[Dict]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = 
                SELECT id, technical_metadata, operational_metadata
                FROM assets
                WHERE connector_id = %s
    Determine if we should insert/update a record.
    Returns: (should_insert_or_update, schema_changed)
    - Only update full record if schema actually changed, not for metadata-only updates
    - For new records, always insert
    - For existing records with only file_hash change, just update last_checked_at (not full record)