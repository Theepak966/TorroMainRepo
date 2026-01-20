import pymysql
import logging
from typing import Dict, Optional, Tuple
import sys
import os
import time
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# OPTIMIZATION 6: Import database session manager for pooled connections
try:
    from database import get_db_session
    from sqlalchemy import text
    POOLED_CONNECTIONS_AVAILABLE = True
except ImportError:
    POOLED_CONNECTIONS_AVAILABLE = False

try:
    from config import config
    DB_CONFIG = {
        'host': config.get('DB_HOST', ''),
        'port': int(config.get('DB_PORT', 3306)),
        'user': config.get('DB_USER', 'root'),
        'password': config.get('DB_PASSWORD', ''),
        'database': config.get('DB_NAME', 'torroforairflow'),
        'charset': 'utf8mb4'
    }
except ImportError:

    import os
    from dotenv import load_dotenv
    from pathlib import Path
    

    backend_dir = Path(__file__).parent.parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', ''),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'torroforairflow'),
        'charset': 'utf8mb4'
    }

logger = logging.getLogger(__name__)


def retry_db_operation(max_retries: int = None, base_delay: float = 1.0, max_delay: float = 60.0, max_total_time: float = 3600.0):

    if max_retries is None:
        env_value = os.getenv("DB_RETRY_MAX_ATTEMPTS", "20")
        max_retries = int(env_value) if env_value else 20

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


@retry_db_operation(max_retries=3, base_delay=1.0, max_delay=10.0, max_total_time=30.0)
def check_file_exists(
    storage_type: str,
    storage_identifier: str,
    storage_path: str
) -> Optional[Dict]:
    # OPTIMIZATION 6: Use pooled connections instead of raw pymysql
    if POOLED_CONNECTIONS_AVAILABLE:
        try:
            with get_db_session() as db:  # Uses connection pool - better management
                result = db.execute(
                    text("""
                        SELECT id, file_hash, schema_hash
                        FROM data_discovery
                        WHERE storage_type = :storage_type
                          AND storage_identifier = :storage_identifier
                          AND storage_path = :storage_path
                        LIMIT 1
                    """),
                    {
                        "storage_type": storage_type,
                        "storage_identifier": storage_identifier,
                        "storage_path": storage_path
                    }
                ).fetchone()
                
                if result:
                    return {
                        "id": result[0],
                        "file_hash": result[1],
                        "schema_hash": result[2]
                    }
                return None
        except Exception as e:
            logger.error(f'FN:check_file_exists error:{str(e)}')
            raise
    else:
        # Fallback to raw pymysql if pooled connections not available
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                sql = """
                    SELECT id, file_hash, schema_hash
                    FROM data_discovery
                    WHERE storage_type = %s
                      AND storage_identifier = %s
                      AND storage_path = %s
                    LIMIT 1
                """
                cursor.execute(sql, (storage_type, storage_identifier, storage_path))
                result = cursor.fetchone()
                return result
        finally:
            if conn:
                conn.close()