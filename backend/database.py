from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import os
from urllib.parse import quote_plus
from contextlib import contextmanager


try:
    from config import config

    env = os.getenv("FLASK_ENV", "development")
    active_config = config.get(env, config["default"])
    
    DB_HOST = active_config.DB_HOST
    DB_PORT = active_config.DB_PORT
    DB_USER = active_config.DB_USER
    DB_PASSWORD = active_config.DB_PASSWORD
    DB_NAME = active_config.DB_NAME
    DB_POOL_SIZE = active_config.DB_POOL_SIZE
    DB_MAX_OVERFLOW = active_config.DB_MAX_OVERFLOW
    DB_POOL_RECYCLE = active_config.DB_POOL_RECYCLE
except ImportError:

    from dotenv import load_dotenv
    from pathlib import Path
    
    backend_dir = Path(__file__).parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    

    DB_HOST = os.getenv("DB_HOST", "")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "torroforexcel")
    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "75"))
    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "75"))
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))
    

    if not DB_HOST:
        raise ValueError("DB_HOST must be set in backend/.env file. Cannot use default 'localhost'.")

DB_DRIVER = os.getenv("DB_DRIVER", "pymysql")

if DB_PASSWORD:

    encoded_password = quote_plus(DB_PASSWORD)
    DATABASE_URL = f"mysql+{DB_DRIVER}://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_URL = f"mysql+{DB_DRIVER}://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

if os.getenv("DATABASE_URL"):
    DATABASE_URL = os.getenv("DATABASE_URL")






POOL_SIZE = DB_POOL_SIZE
MAX_OVERFLOW = DB_MAX_OVERFLOW
POOL_RECYCLE = DB_POOL_RECYCLE

# Enhanced connection pool settings to prevent exhaustion
# pool_size: number of connections to maintain persistently
# max_overflow: additional connections that can be created beyond pool_size
# pool_recycle: time in seconds before a connection is recycled (prevents stale connections)
# pool_pre_ping: test connections before using them (detects stale connections)
# pool_reset_on_return: reset connection state when returned to pool
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=True,  # Test connections before using them
    pool_reset_on_return='commit',  # Reset connection state on return
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    connect_args={
        "connect_timeout": 10,
        "read_timeout": 120,
        "write_timeout": 180
    },
    # Additional pool settings for better connection management
    pool_timeout=30,  # Seconds to wait before giving up on getting a connection
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Generator function for dependency injection (Flask/Django style)"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@contextmanager
def get_db_session():
    """
    Context manager for database sessions - ensures proper cleanup
    Use this in discovery processes and threaded operations to prevent connection exhaustion
    
    Example:
        with get_db_session() as db:
            asset = db.query(Asset).filter(Asset.id == asset_id).first()
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

