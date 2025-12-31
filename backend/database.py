from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os
from urllib.parse import quote_plus


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

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=True,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    connect_args={
        "connect_timeout": 10,
        "read_timeout": 120,
        "write_timeout": 180
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

