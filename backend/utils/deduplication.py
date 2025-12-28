import pymysql
import logging
from typing import Dict, Optional, Tuple
import sys
import os
import time
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Import DB config from backend config
try:
    from config import config
    DB_CONFIG = {
        'host': config.get('DB_HOST', 'localhost'),
        'port': int(config.get('DB_PORT', 3306)),
        'user': config.get('DB_USER', 'root'),
        'password': config.get('DB_PASSWORD', ''),
        'database': config.get('DB_NAME', 'torroforexcel'),
        'charset': 'utf8mb4'
    }
except ImportError:
    # Fallback if config not available
    import os
    from dotenv import load_dotenv
    from pathlib import Path
    
    # Load .env from backend directory
    backend_dir = Path(__file__).parent.parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'torroforexcel'),
        'charset': 'utf8mb4'
    }

logger = logging.getLogger(__name__)


def retry_db_operation(max_retries: int = None, base_delay: float = 1.0, max_delay: float = 60.0, max_total_time: float = 3600.0):
    """
    Retry decorator for database operations with exponential backoff.
    Handles connection errors, timeouts, and rate limiting.
    
    Args:
        max_retries: Maximum number of retries (None = unlimited, but limited by max_total_time)
        base_delay: Initial delay in seconds (exponential backoff: 1s, 2s, 4s, 8s...)
        max_delay: Maximum delay between retries (caps exponential backoff)
        max_total_time: Maximum total time to spend retrying (safety timeout in seconds)
    """
    # Get retry config from environment or use defaults
    if max_retries is None:
        env_value = os.getenv("DB_RETRY_MAX_ATTEMPTS", "20")
        max_retries = int(env_value) if env_value else 20
        # 0 means unlimited retries (only limited by max_total_time)
        if max_retries == 0:
            max_retries = -1  # Use -1 internally to represent unlimited
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            start_time = time.time()
            attempt = 0
            
            while True:
                # Check total time limit (safety timeout)
                elapsed_time = time.time() - start_time
                if elapsed_time >= max_total_time:
                    logger.error('FN:retry_db_operation max_total_time:{} attempt:{}'.format(max_total_time, attempt))
                    if last_exception:
                        raise last_exception
                    raise TimeoutError(f"Operation timed out after {max_total_time}s")
                
                # Check retry limit (if set, -1 means unlimited)
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
                    
                    # Check if it's a retryable error
                    retryable_errors = [
                        2006,  # MySQL server has gone away
                        2013,  # Lost connection to MySQL server
                        1205,  # Lock wait timeout
                        1213,  # Deadlock found
                        1040,  # Too many connections
                    ]
                    
                    # Only retry if it's a retryable error
                    if error_code not in retryable_errors:
                        logger.error('FN:retry_db_operation error_code:{} error:{}'.format(error_code, str(e)))
                        raise
                    
                    # Calculate delay with exponential backoff (capped at max_delay)
                    delay = min(base_delay * (2 ** min(attempt, 10)), max_delay)  # Cap exponential at 2^10
                    
                    # Check if we have time for another retry
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
                    # Non-retryable errors (syntax errors, etc.)
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
    except Exception as e:
        logger.error('FN:check_file_exists storage_type:{} storage_identifier:{} storage_path:{} error:{}'.format(storage_type, storage_identifier, storage_path, str(e)))
        raise
    finally:
        if conn:
            conn.close()


def compare_hashes(existing_record: Dict, new_file_hash: str, new_schema_hash: str) -> Tuple[bool, bool]:
    existing_file_hash = existing_record.get("file_hash")
    existing_schema_hash = existing_record.get("schema_hash")
    
    file_changed = existing_file_hash != new_file_hash
    schema_changed = existing_schema_hash != new_schema_hash
    
    return file_changed, schema_changed


def should_update_or_insert(existing_record: Optional[Dict], new_file_hash: str, new_schema_hash: str) -> Tuple[bool, bool]:
    """
    Determine if we should insert/update a record.
    Returns: (should_insert_or_update, schema_changed)
    - Only update full record if schema actually changed, not for metadata-only updates
    - For new records, always insert
    - For existing records with only file_hash change, just update last_checked_at (not full record)
    """
    if not existing_record:
        return True, False
    
    file_changed, schema_changed = compare_hashes(existing_record, new_file_hash, new_schema_hash)
    
    # Only update full record if schema changed (not just file hash or metadata)
    if schema_changed:
        logger.info('FN:should_update_or_insert schema_changed:{} existing_record_id:{}'.format(schema_changed, existing_record.get('id')))
        return True, True
    
    # File hash changed but schema didn't - don't update full record, just update last_checked_at
    if file_changed:
        logger.info('FN:should_update_or_insert file_changed:{} schema_changed:{} existing_record_id:{}'.format(file_changed, schema_changed, existing_record.get('id')))
        return False, False
    
    logger.info('FN:should_update_or_insert file_changed:{} schema_changed:{} existing_record_id:{}'.format(file_changed, schema_changed, existing_record.get('id')))
    return False, False
