
import os
from dotenv import load_dotenv
from pathlib import Path


backend_dir = Path(__file__).parent
env_path = backend_dir / '.env'
load_dotenv(env_path)

class Config:
    
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
    FLASK_ENV = os.getenv("FLASK_ENV", "development")
    DEBUG = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "*")
    if allowed_origins_str == "*":
        ALLOWED_ORIGINS = ["*"]
    else:
        ALLOWED_ORIGINS = allowed_origins_str.split(",")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "app.log")


    DB_HOST = os.getenv("DB_HOST", "")
    if not DB_HOST:
        raise ValueError("DB_HOST must be set in backend/.env file. Cannot use default 'localhost'.")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "torroforexcel")
    




    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "75"))
    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "75"))
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))
    


    DISCOVERY_BATCH_SIZE = int(os.getenv("DISCOVERY_BATCH_SIZE", "1500"))
    


    DISCOVERY_SKIP_FILE_SAMPLES = os.getenv("DISCOVERY_SKIP_FILE_SAMPLES", "false").lower() == "true"
    


    DISCOVERY_MAX_SAMPLE_SIZE = int(os.getenv("DISCOVERY_MAX_SAMPLE_SIZE", str(100 * 1024 * 1024)))

    API_VERSION = os.getenv("API_VERSION", "v1")
    

    FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    FLASK_PORT = int(os.getenv("FLASK_PORT", "8099"))
    

    AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
    AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

    _backend_dir = Path(__file__).parent
    _project_root = _backend_dir.parent
    _default_airflow_home = str(_project_root / "airflow")
    AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", _default_airflow_home)
    

    AZURE_AI_LANGUAGE_ENDPOINT = os.getenv("AZURE_AI_LANGUAGE_ENDPOINT", "")
    AZURE_AI_LANGUAGE_KEY = os.getenv("AZURE_AI_LANGUAGE_KEY", "")

class DevelopmentConfig(Config):
    
    DEBUG = True
    LOG_LEVEL = "DEBUG"

class ProductionConfig(Config):
    
    DEBUG = False
    LOG_LEVEL = "INFO"

class TestingConfig(Config):
    
    DEBUG = True
    TESTING = True
    DB_NAME = os.getenv("TEST_DB_NAME", "torroforexcel_test")

config = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
    "default": DevelopmentConfig
}

