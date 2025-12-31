import sys
import os


try:
    import flask
except ImportError:
    print("=" * 70)
    print("ERROR: Flask is not installed!")
    print("=" * 70)
    print("You have two options:")
    print("")
    print("Option 1: Use virtual environment (RECOMMENDED):")
    print("  cd backend")
    print("  source venv/bin/activate  # On Linux/Mac")
    print("  # OR: venv\\Scripts\\activate  # On Windows")
    print("  python main.py")
    print("")
    print("  If venv doesn't exist, create it first:")
    print("  python3 -m venv venv")
    print("  source venv/bin/activate")
    print("  pip install -r requirements.txt")
    print("")
    print("Option 2: Install dependencies globally (NOT RECOMMENDED):")
    print("  cd backend")
    print("  pip3 install -r requirements.txt")
    print("  python3 main.py")
    print("")
    print("=" * 70)
    sys.exit(1)

from flask import Flask, jsonify, request, Response, copy_current_request_context, has_request_context
from flask_cors import CORS
from sqlalchemy.orm.attributes import flag_modified



sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))



from config import config
from database import engine, Base, SessionLocal
from models import Asset, Connection, LineageRelationship, LineageHistory, SQLQuery, DataDiscovery
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps
from datetime import datetime, timedelta, timezone
from sqlalchemy import func
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from utils.azure_blob_client import AzureBlobClient
    from utils.metadata_extractor import extract_file_metadata, generate_file_hash, generate_schema_hash
    from utils.asset_deduplication import check_asset_exists, should_update_or_insert
    from utils.sql_lineage_extractor import extract_lineage_from_sql, get_lineage_extractor
    from utils.ml_lineage_inference import infer_relationships_ml, fuzzy_column_match
    from utils.data_quality_integration import calculate_asset_quality_score, propagate_quality_through_lineage
    AZURE_AVAILABLE = True
    logger.info('FN:__init__ message:Azure utilities loaded successfully')
except ImportError as e:
    logger.warning('FN:__init__ message:Azure utilities not available error:{}'.format(str(e)))
    logger.warning('FN:__init__ message:Import error details error:{}'.format(str(e)))
    AZURE_AVAILABLE = False

    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("azure_blob_client", os.path.join(os.path.dirname(__file__), "utils", "azure_blob_client.py"))
        if spec and spec.loader:
            azure_blob_client = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(azure_blob_client)
            AzureBlobClient = azure_blob_client.AzureBlobClient
            AZURE_AVAILABLE = True
            logger.info('FN:__init__ message:Azure utilities loaded via importlib')
    except Exception as e2:
        logger.warning('FN:__init__ message:Could not load Azure utilities via importlib error:{}'.format(str(e2)))

app = Flask(__name__)

env = os.getenv("FLASK_ENV", "development")
app.config.from_object(config.get(env, config["default"]))

if app.config["ALLOWED_ORIGINS"] == ["*"]:
    CORS(app, resources={r"/api/*": {"origins": "*", "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"], "allow_headers": ["Content-Type", "Authorization"], "supports_credentials": True}})
else:
    CORS(
        app,
        resources={
            r"/api/*": {
                "origins": app.config["ALLOWED_ORIGINS"],
                "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                "allow_headers": ["Content-Type", "Authorization"],
                "supports_credentials": True
            }
        }
    )

logging.basicConfig(
    level=getattr(logging, app.config["LOG_LEVEL"]),
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    handlers=[
        RotatingFileHandler(
            app.config["LOG_FILE"],
            maxBytes=10000000,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:


    from sqlalchemy import inspect
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    

    Base.metadata.create_all(bind=engine, checkfirst=True)
    

    new_tables = inspector.get_table_names()
    created_tables = [t for t in new_tables if t not in existing_tables]
    
    if created_tables:
        logger.info('FN:__init__ message:Created new database tables: {}'.format(', '.join(created_tables)))
    else:
        logger.info('FN:__init__ message:All database tables already exist')
    
    logger.info('FN:__init__ message:Database tables initialized successfully')
except Exception as e:
    error_msg = str(e)

    if 'already exists' in error_msg.lower() or 'duplicate' in error_msg.lower():
        logger.warning('FN:__init__ message:Some tables may already exist, continuing: {}'.format(error_msg))
    elif 'foreign key' in error_msg.lower() or '3780' in error_msg or 'incompatible' in error_msg.lower():


        logger.warning('FN:__init__ message:Foreign key constraint issue (tables may already exist): {}'.format(error_msg))
        logger.info('FN:__init__ message:Continuing with existing database schema')
    else:

        logger.error('FN:__init__ message:Error initializing database tables error:{}'.format(error_msg))
        raise

def handle_error(f):
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error('FN:handle_error function_name:{} error:{}'.format(f.__name__, str(e)), exc_info=True)
            if app.config.get("DEBUG"):
                return jsonify({"error": str(e)}), 500
            else:
                return jsonify({"error": "An internal error occurred"}), 500
    return decorated_function

@app.route('/health', methods=['GET'])
def health_simple():
    return "healthy\n", 200

@app.route('/api/health', methods=['GET'])
def health():
    
    return jsonify({
        "status": "ok",
        "message": "Backend is running",
        "environment": env
    })

@app.route('/api/connections', methods=['GET'])
@handle_error
def get_connections():
    
    db = SessionLocal()
    try:
        connections = db.query(Connection).all()
        return jsonify([{
            "id": conn.id,
            "name": conn.name,
            "connector_type": conn.connector_type,
            "connection_type": conn.connection_type,
            "config": conn.config,
            "status": conn.status,
            "created_at": conn.created_at.isoformat() if conn.created_at else None
        } for conn in connections])
    finally:
        db.close()

@app.route('/api/connections', methods=['POST'])
@handle_error
def create_connection():
    
    db = SessionLocal()
    try:
        data = request.json

        if not data:
            return jsonify({"error": "Request body is required"}), 400
        if not data.get('name'):
            return jsonify({"error": "Connection name is required"}), 400
        if not data.get('connector_type'):
            return jsonify({"error": "Connector type is required"}), 400


        existing_connection = db.query(Connection).filter(Connection.name == data['name']).first()
        if existing_connection:
            db.close()
            return jsonify({
                "error": f"A connection with the name '{data['name']}' already exists. Please use a different name or update the existing connection."
            }), 409

        connection = Connection(
            name=data['name'],
            connector_type=data['connector_type'],
            connection_type=data.get('connection_type'),
            config=data.get('config', {}),
            status=data.get('status', 'active')
        )
        db.add(connection)
        db.commit()
        db.refresh(connection)

        logger.info('FN:create_connection connection_name:{} connection_id:{}'.format(connection.name, connection.id))

        return jsonify({
            "id": connection.id,
            "name": connection.name,
            "connector_type": connection.connector_type,
            "connection_type": connection.connection_type,
            "config": connection.config,
            "status": connection.status,
            "created_at": connection.created_at.isoformat() if connection.created_at else None
        }), 201
    except Exception as e:
        db.rollback()
        logger.error('FN:create_connection error:{}'.format(str(e)), exc_info=True)

        error_str = str(e)
        if "Duplicate entry" in error_str or "1062" in error_str or "UNIQUE constraint" in error_str:
            db.close()
            return jsonify({
                "error": f"A connection with the name '{data.get('name', '')}' already exists. Please use a different name or update the existing connection."
            }), 409
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create connection"}), 400
    finally:
        db.close()

@app.route('/api/connections/<int:connection_id>', methods=['PUT'])
@handle_error
def update_connection(connection_id):
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        

        if 'connection_type' in data:
            connection.connection_type = data['connection_type']
        if 'config' in data:
            connection.config = data['config']
        if 'status' in data:
            connection.status = data['status']
        
        db.commit()
        db.refresh(connection)
        
        logger.info('FN:update_connection connection_id:{} connection_name:{}'.format(connection_id, connection.name))
        
        return jsonify({
            "id": connection.id,
            "name": connection.name,
            "connector_type": connection.connector_type,
            "connection_type": connection.connection_type,
            "config": connection.config,
            "status": connection.status,
            "created_at": connection.created_at.isoformat() if connection.created_at else None
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:update_connection connection_id:{} error:{}'.format(connection_id, str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to update connection"}), 400
    finally:
        db.close()

@app.route('/api/connections/<int:connection_id>', methods=['DELETE'])
@handle_error
def delete_connection(connection_id):
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404

        # Build connector_id pattern
        if connection.connector_type == 'azure_blob':
            connector_id_pattern = f"azure_blob_{connection.name}"
        else:
            connector_id_pattern = f"{connection.connector_type}_{connection.name}"
        
        # OPTIMIZED: Get count first (for response), then use bulk delete
        asset_count = db.query(Asset).filter(Asset.connector_id == connector_id_pattern).count()
        
        # OPTIMIZED: Use bulk delete instead of loading all assets into memory
        # Lineage relationships will be automatically deleted via CASCADE
        # DataDiscovery records will also be automatically deleted via CASCADE
        # LineageHistory will be automatically deleted via CASCADE
        
        # Delete assets in bulk (much faster than individual deletes)
        deleted_assets_count = db.query(Asset).filter(
            Asset.connector_id == connector_id_pattern
        ).delete(synchronize_session=False)
        
        # Delete the connection
        connection_name = connection.name
        db.delete(connection)
        
        # Single commit for all deletions
        db.commit()
        
        logger.info('FN:delete_connection connection_name:{} connection_id:{} deleted_assets_count:{}'.format(
            connection_name, connection_id, deleted_assets_count
        ))
        
        return jsonify({
            "message": "Connection and associated assets deleted successfully",
            "deleted_assets": deleted_assets_count,
            "connection_name": connection_name
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:delete_connection connection_id:{} error:{}'.format(connection_id, str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to delete connection"}), 400
    finally:
        db.close()

@app.route('/api/assets/<asset_id>/quality', methods=['GET'])
@handle_error
def get_asset_quality(asset_id):
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        
        asset_dict = {
            'id': asset.id,
            'name': asset.name,
            'columns': asset.columns,
            'last_modified': asset.discovered_at.isoformat() if asset.discovered_at else None,
            'technical_metadata': asset.technical_metadata,
            'operational_metadata': asset.operational_metadata
        }
        
        quality = calculate_asset_quality_score(asset_dict)
        
        return jsonify({
            'asset_id': asset_id,
            'asset_name': asset.name,
            **quality
        }), 200
    finally:
        db.close()

@app.route('/api/assets', methods=['GET'])
@handle_error
def get_assets():
    db = SessionLocal()
    try:
        discovery_id = request.args.get('discovery_id', type=int)
        
        if discovery_id:

            discovery = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
            if not discovery:
                return jsonify({"error": "Discovery record not found"}), 404
            
            if discovery.asset_id:
                asset = db.query(Asset).filter(Asset.id == discovery.asset_id).first()
                if not asset:
                    return jsonify({"error": "Asset not found for this discovery_id"}), 404
                
                return jsonify([{
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog,
                    "connector_id": asset.connector_id,
                    "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
                    "technical_metadata": asset.technical_metadata,
                    "operational_metadata": asset.operational_metadata,
                    "business_metadata": asset.business_metadata,
                    "columns": asset.columns,
                    "discovery_id": discovery.id,
                    "discovery_status": discovery.status,
                    "discovery_approval_status": discovery.approval_status
                }])
            else:
                return jsonify({"error": "No asset linked to this discovery_id"}), 404
        
        # Pagination parameters
        page = request.args.get('page', type=int, default=1)
        per_page = min(request.args.get('per_page', type=int, default=100), 500)  # Max 500 per page
        offset = (page - 1) * per_page

        from sqlalchemy import case, func
        
        # Get total count first
        total_count = db.query(Asset).count()
        
        # Get latest discovery IDs for all assets (for joining)
        latest_discovery_subq = db.query(
            DataDiscovery.asset_id,
            func.max(DataDiscovery.id).label('latest_discovery_id')
        ).filter(
            DataDiscovery.asset_id.isnot(None)
        ).group_by(DataDiscovery.asset_id).subquery()
        
        # Get assets with pagination
        assets_with_discovery = db.query(Asset, DataDiscovery).outerjoin(
            latest_discovery_subq, Asset.id == latest_discovery_subq.c.asset_id
        ).outerjoin(
            DataDiscovery, DataDiscovery.id == latest_discovery_subq.c.latest_discovery_id
        ).order_by(
            Asset.discovered_at.desc(),
            case((DataDiscovery.id.is_(None), 1), else_=0),
            DataDiscovery.id.desc()
        ).limit(per_page).offset(offset).all()
        
        result = []
        seen_asset_ids = set()
        for asset, discovery in assets_with_discovery:

            if asset.id in seen_asset_ids:
                continue
            seen_asset_ids.add(asset.id)
            asset_data = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "technical_metadata": asset.technical_metadata,
            "operational_metadata": asset.operational_metadata,
            "business_metadata": asset.business_metadata,
            "columns": asset.columns
            }
            if discovery:
                asset_data["discovery_id"] = discovery.id
                asset_data["discovery_status"] = discovery.status
                asset_data["discovery_approval_status"] = discovery.approval_status
            result.append(asset_data)
        
        total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
        
        return jsonify({
            "assets": result,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
        })
    finally:
        db.close()

@app.route('/api/discovery/<int:discovery_id>', methods=['GET'])
@handle_error
def get_discovery_by_id(discovery_id):
    db = SessionLocal()
    try:
        discovery = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
        if not discovery:
            return jsonify({"error": "Discovery record not found"}), 404
        

        file_metadata = discovery.file_metadata or {}
        file_basic = file_metadata.get("basic", {})
        file_hash_obj = file_metadata.get("hash", {})
        file_timestamps = file_metadata.get("timestamps", {})
        

        storage_location = discovery.storage_location or {}
        storage_connection = storage_location.get("connection", {})
        storage_container = storage_location.get("container", {})
        

        storage_metadata = discovery.storage_metadata or {}
        azure_storage_metadata = storage_metadata.get("azure", {})
        

        def format_rfc2822(dt):
            if not dt:
                return None
            if isinstance(dt, str):
                try:

                    if 'T' in dt:
                        if dt.endswith('Z'):
                            dt = dt.replace('Z', '+00:00')
                        dt = datetime.fromisoformat(dt)
                    else:

                        try:
                            dt = datetime.strptime(dt, '%Y-%m-%d')
                        except (ValueError, TypeError):
                            return dt
                except (ValueError, TypeError):
                    return dt
            if hasattr(dt, 'strftime'):

                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')
            return None
        

        result = {
            "id": discovery.id,
            "additional_metadata": discovery.additional_metadata,
            "approval_status": discovery.approval_status or "pending_review",
            "approval_workflow": discovery.approval_workflow,
            "created_at": format_rfc2822(discovery.created_at),
            "created_by": discovery.created_by or "api_trigger",
            "data_publishing_id": discovery.data_publishing_id,
            "data_quality_score": float(discovery.data_quality_score) if discovery.data_quality_score else None,
            "data_source_type": discovery.data_source_type or file_basic.get("format", ""),
            "deleted_at": format_rfc2822(discovery.deleted_at),
            "discovered_at": format_rfc2822(discovery.discovered_at),
            "discovery_info": discovery.discovery_info or {},
            "env_type": discovery.env_type or "production",
            "environment": discovery.environment or "prod",
            "file_hash": file_hash_obj.get("value", ""),
            "file_last_modified": format_rfc2822(file_timestamps.get("last_modified")) or format_rfc2822(azure_storage_metadata.get("last_modified")),
            "file_metadata": file_metadata,
            "file_name": file_basic.get("name", ""),
            "file_size_bytes": file_basic.get("size_bytes", 0),
            "folder_path": discovery.folder_path or "",
            "is_active": 1 if discovery.is_active else 0,
            "is_visible": 1 if discovery.is_visible else 0,
            "last_checked_at": format_rfc2822(discovery.last_checked_at),
            "notification_recipients": discovery.notification_recipients,
            "notification_sent_at": format_rfc2822(discovery.notification_sent_at),
            "published_at": format_rfc2822(discovery.published_at),
            "published_to": discovery.published_to,
            "schema_hash": discovery.schema_hash or "",
            "schema_json": discovery.schema_json or {"columns": [], "num_columns": 0},
            "schema_version": discovery.schema_version,
            "status": discovery.status or "pending",
            "storage_data_metadata": discovery.storage_data_metadata or {},
            "storage_identifier": storage_connection.get("account_name", ""),
            "storage_location": storage_location,
            "storage_metadata": storage_metadata,
            "storage_path": storage_location.get("path", ""),
            "storage_type": storage_location.get("type", "azure_blob"),
            "tags": discovery.tags,
            "updated_at": format_rfc2822(discovery.updated_at),
            "validated_at": format_rfc2822(discovery.validated_at),
            "validation_errors": discovery.validation_errors,
            "validation_status": discovery.validation_status
        }
        

        if not result["schema_json"] or not isinstance(result["schema_json"], dict):
            result["schema_json"] = {"columns": [], "num_columns": 0}
        
        schema_json = result["schema_json"]
        

        if "delimiter" not in schema_json:

            format_specific = file_metadata.get("format_specific", {})
            csv_info = format_specific.get("csv", {})
            if csv_info:
                schema_json["delimiter"] = csv_info.get("delimiter", ",")
                schema_json["has_header"] = csv_info.get("has_header", True)
            else:

                file_format = file_basic.get("format", "").lower()
                if file_format == "csv":
                    schema_json["delimiter"] = ","
                    schema_json["has_header"] = True
                else:
                    schema_json["delimiter"] = None
                    schema_json["has_header"] = None
        

        if "has_header" not in schema_json:
            schema_json["has_header"] = None
        

        if "num_rows" not in schema_json:
            schema_json["num_rows"] = None
        if "sample_rows_count" not in schema_json:

            columns = schema_json.get("columns", [])
            if columns and len(columns) > 0:
                first_col = columns[0]
                sample_values = first_col.get("sample_values", [])
                schema_json["sample_rows_count"] = len(sample_values) if sample_values else None
            else:
                schema_json["sample_rows_count"] = None
        

        if "num_columns" not in schema_json:
            schema_json["num_columns"] = len(schema_json.get("columns", []))
        

        if not result["file_metadata"] or not isinstance(result["file_metadata"], dict):
            result["file_metadata"] = {}
        
        file_meta = result["file_metadata"]
        if "format_specific" not in file_meta:

            file_format = file_basic.get("format", "").lower()
            format_specific = {}
            if file_format == "csv":
                format_specific["csv"] = {
                    "delimiter": ",",
                    "encoding": "utf-8",
                    "has_header": True
                }
            elif file_format in ["parquet", "json", "avro", "excel", "xml", "orc", "delta_lake"]:
                format_specific[file_format] = {}
            if format_specific:
                file_meta["format_specific"] = format_specific
            else:
                file_meta["format_specific"] = {}
        

        if result["storage_location"] and isinstance(result["storage_location"], dict):
            if "metadata" not in result["storage_location"]:
                result["storage_location"]["metadata"] = {}
        


        if not result["discovery_info"] or not isinstance(result["discovery_info"], dict):
            result["discovery_info"] = {}
        
        return jsonify(result), 200
    except Exception as e:
        logger.error('FN:get_discovery_by_id discovery_id:{} error:{}'.format(discovery_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/discovery', methods=['GET'])
@handle_error
def list_discoveries():
    db = SessionLocal()
    try:
        status_filter = request.args.get('status')
        approval_status_filter = request.args.get('approval_status')
        limit = request.args.get('limit', type=int, default=100)
        offset = request.args.get('offset', type=int, default=0)
        
        query = db.query(DataDiscovery)
        
        if status_filter:
            query = query.filter(DataDiscovery.status == status_filter)
        if approval_status_filter:
            query = query.filter(DataDiscovery.approval_status == approval_status_filter)
        
        total = query.count()
        discoveries = query.order_by(DataDiscovery.discovered_at.desc()).limit(limit).offset(offset).all()
        
        result = []
        for discovery in discoveries:
            discovery_data = {
                "discovery_id": discovery.id,
                "asset_id": discovery.asset_id,
                "status": discovery.status,
                "approval_status": discovery.approval_status,
                "discovered_at": discovery.discovered_at.isoformat() if discovery.discovered_at else None,
                "file_name": discovery.file_metadata.get("basic", {}).get("name") if discovery.file_metadata else None,
                "storage_type": discovery.storage_location.get("type") if discovery.storage_location else None,
                "storage_path": discovery.storage_location.get("path") if discovery.storage_location else None,
            }
            

            if discovery.asset_id:
                asset = db.query(Asset).filter(Asset.id == discovery.asset_id).first()
                if asset:
                    discovery_data["asset"] = {
                        "id": asset.id,
                        "name": asset.name,
                        "type": asset.type
                    }
            
            result.append(discovery_data)
        
        return jsonify({
            "total": total,
            "limit": limit,
            "offset": offset,
            "discoveries": result
        }), 200
    except Exception as e:
        logger.error('FN:list_discoveries error:{}'.format(str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/discovery/<int:discovery_id>/approve', methods=['PUT'])
@handle_error
def approve_discovery(discovery_id):
    db = SessionLocal()
    try:
        discovery = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
        if not discovery:
            return jsonify({"error": "Discovery record not found"}), 404
        
        approval_time = datetime.utcnow()
        discovery.approval_status = "approved"
        discovery.status = "approved"
        if not discovery.approval_workflow:
            discovery.approval_workflow = {}
        discovery.approval_workflow["approved_at"] = approval_time.isoformat()
        discovery.approval_workflow["approved_by"] = "user"
        flag_modified(discovery, "approval_workflow")
        

        if discovery.asset_id:
            asset = db.query(Asset).filter(Asset.id == discovery.asset_id).first()
            if asset:
                if not asset.operational_metadata:
                    asset.operational_metadata = {}
                asset.operational_metadata["approval_status"] = "approved"
                asset.operational_metadata["approved_at"] = approval_time.isoformat()
                asset.operational_metadata["approved_by"] = "user"
                flag_modified(asset, "operational_metadata")
        
        db.commit()
        db.refresh(discovery)
        
        logger.info('FN:approve_discovery discovery_id:{} approval_status:{} saved_to_db:True'.format(
            discovery_id, discovery.approval_status
        ))
        
        return jsonify({
            "discovery_id": discovery.id,
            "status": "approved",
            "approval_status": "approved",
            "updated_at": approval_time.isoformat()
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:approve_discovery discovery_id:{} error:{}'.format(discovery_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/discovery/<int:discovery_id>/reject', methods=['PUT'])
@handle_error
def reject_discovery(discovery_id):
    db = SessionLocal()
    try:
        discovery = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
        if not discovery:
            return jsonify({"error": "Discovery record not found"}), 404
        
        data = request.json or {}
        reason = data.get('reason', 'No reason provided')
        
        rejection_time = datetime.utcnow()
        discovery.approval_status = "rejected"
        discovery.status = "rejected"
        if not discovery.approval_workflow:
            discovery.approval_workflow = {}
        discovery.approval_workflow["rejected_at"] = rejection_time.isoformat()
        discovery.approval_workflow["rejected_by"] = "user"
        discovery.approval_workflow["rejection_reason"] = reason
        flag_modified(discovery, "approval_workflow")
        

        if discovery.asset_id:
            asset = db.query(Asset).filter(Asset.id == discovery.asset_id).first()
            if asset:
                if not asset.operational_metadata:
                    asset.operational_metadata = {}
                asset.operational_metadata["approval_status"] = "rejected"
                asset.operational_metadata["rejected_at"] = rejection_time.isoformat()
                asset.operational_metadata["rejected_by"] = "user"
                asset.operational_metadata["rejection_reason"] = reason
                flag_modified(asset, "operational_metadata")
        
        db.commit()
        db.refresh(discovery)
        
        logger.info('FN:reject_discovery discovery_id:{} approval_status:{} saved_to_db:True'.format(
            discovery_id, discovery.approval_status
        ))
        
        return jsonify({
            "discovery_id": discovery.id,
            "status": "rejected",
            "approval_status": "rejected",
            "rejection_reason": reason,
            "updated_at": rejection_time.isoformat()
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:reject_discovery discovery_id:{} error:{}'.format(discovery_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/discovery/stats', methods=['GET'])
@handle_error
def get_discovery_stats():
    db = SessionLocal()
    try:
        total = db.query(DataDiscovery).count()
        by_status = {}
        by_approval_status = {}
        

        statuses = db.query(DataDiscovery.status, func.count(DataDiscovery.id)).group_by(DataDiscovery.status).all()
        for status, count in statuses:
            by_status[status or "unknown"] = count
        

        approval_statuses = db.query(DataDiscovery.approval_status, func.count(DataDiscovery.id)).group_by(DataDiscovery.approval_status).all()
        for approval_status, count in approval_statuses:
            by_approval_status[approval_status or "unknown"] = count
        

        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        recent_count = db.query(DataDiscovery).filter(DataDiscovery.discovered_at >= seven_days_ago).count()
        
        return jsonify({
            "total": total,
            "by_status": by_status,
            "by_approval_status": by_approval_status,
            "recent_discoveries_7_days": recent_count
        }), 200
    except Exception as e:
        logger.error('FN:get_discovery_stats error:{}'.format(str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/discovery/trigger', methods=['POST'])
@handle_error
def trigger_discovery():
    try:
        data = request.json or {}
        connection_id = data.get('connection_id')
        

        airflow_triggered = False
        try:
            airflow_base_url = app.config.get("AIRFLOW_BASE_URL")
            if not airflow_base_url:
                return jsonify({
                    "error": "AIRFLOW_BASE_URL not configured",
                    "message": "Cannot trigger Airflow DAG without AIRFLOW_BASE_URL"
                }), 400
            
            dag_id = "azure_blob_discovery"
            import requests
            from requests.auth import HTTPBasicAuth
            

            import subprocess
            import os as os_module
            
            note = f"Triggered from refresh button"
            if connection_id:
                note += f" for connection_id: {connection_id}"
            


            default_airflow_home = os.path.join(os.path.dirname(os.path.dirname(__file__)), "airflow")
            airflow_home = app.config.get("AIRFLOW_HOME", default_airflow_home)
            airflow_bin = os.path.join(airflow_home, "venv", "bin", "airflow")
            env = os_module.environ.copy()
            env["AIRFLOW_HOME"] = airflow_home
            

            conf_json = f'{{"note": "{note}"}}'
            result = subprocess.run(
                [airflow_bin, "dags", "trigger", dag_id, "--conf", conf_json],
                cwd=airflow_home,
                env=env,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                airflow_triggered = True
                logger.info(f'FN:trigger_discovery airflow_dag_triggered:{dag_id} connection_id:{connection_id}')
            else:
                logger.warning(f'FN:trigger_discovery airflow_trigger_failed:returncode:{result.returncode} stderr:{result.stderr}')

                airflow_triggered = False
        except Exception as e:
            logger.error(f'FN:trigger_discovery airflow_trigger_error:{str(e)}')
            return jsonify({
                "error": f"Failed to trigger Airflow DAG: {str(e)}"
            }), 400
        
        return jsonify({
            "success": True,
            "message": "Airflow DAG triggered successfully",
            "dag_id": dag_id,
            "airflow_triggered": airflow_triggered
        }), 200
    except Exception as e:
        logger.error('FN:trigger_discovery error:{}'.format(str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400

@app.route('/api/assets', methods=['POST'])
@handle_error
def create_assets():
    
    db = SessionLocal()
    try:
        data = request.json

        if not data:
            return jsonify({"error": "Request body is required"}), 400

        assets_data = data if isinstance(data, list) else [data]
        created_assets = []
        skipped_assets = []

        for asset_data in assets_data:
            if not asset_data.get('id'):
                return jsonify({"error": "Asset ID is required"}), 400
            if not asset_data.get('name'):
                return jsonify({"error": "Asset name is required"}), 400
            if not asset_data.get('type'):
                return jsonify({"error": "Asset type is required"}), 400


            existing_asset = db.query(Asset).filter(Asset.id == asset_data['id']).first()
            if existing_asset:
                logger.warning('FN:create_assets asset_id:{} message:Asset already exists, skipping'.format(asset_data['id']))
                skipped_assets.append(asset_data['id'])
                continue

            asset = Asset(
                id=asset_data['id'],
                name=asset_data['name'],
                type=asset_data['type'],
                catalog=asset_data.get('catalog'),
                connector_id=asset_data.get('connector_id'),
                technical_metadata=asset_data.get('technical_metadata', {}),
                operational_metadata=asset_data.get('operational_metadata', {}),
                business_metadata=asset_data.get('business_metadata', {}),
                columns=asset_data.get('columns', [])
            )
            db.add(asset)
            created_assets.append(asset)

        db.commit()
        for asset in created_assets:
            db.refresh(asset)

        logger.info('FN:create_assets created_count:{} skipped_count:{}'.format(len(created_assets), len(skipped_assets)))

        response_data = {
            "created": [{
                "id": asset.id,
                "name": asset.name,
                "type": asset.type,
                "catalog": asset.catalog,
                "connector_id": asset.connector_id,
                "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
                "technical_metadata": asset.technical_metadata,
                "operational_metadata": asset.operational_metadata,
                "business_metadata": asset.business_metadata,
                "columns": asset.columns
            } for asset in created_assets],
            "skipped": skipped_assets,
            "message": f"Created {len(created_assets)} asset(s), skipped {len(skipped_assets)} duplicate(s)"
        }
        
        return jsonify(response_data), 201
    except Exception as e:
        db.rollback()
        logger.error('FN:create_assets error:{}'.format(str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create assets"}), 400
    finally:
        db.close()

@app.route('/api/assets/<asset_id>', methods=['GET'])
@handle_error
def get_asset_by_id(asset_id):
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        

        discovery = db.query(DataDiscovery).filter(
            DataDiscovery.asset_id == asset_id
        ).order_by(DataDiscovery.id.desc()).first()
        
        result = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "columns": asset.columns or [],
            "technical_metadata": asset.technical_metadata or {},
            "operational_metadata": asset.operational_metadata or {},
            "business_metadata": asset.business_metadata or {}
        }
        

        if discovery:
            result["discovery_id"] = discovery.id
            result["discovery_status"] = discovery.status
            result["discovery_approval_status"] = discovery.approval_status
            result["discovery_info"] = discovery.discovery_info
            result["storage_location"] = discovery.storage_location
            result["file_metadata"] = discovery.file_metadata
            result["schema_json"] = discovery.schema_json
            result["schema_hash"] = discovery.schema_hash
            result["folder_path"] = discovery.folder_path
            result["data_source_type"] = discovery.data_source_type
            result["environment"] = discovery.environment
            result["env_type"] = discovery.env_type
            result["tags"] = discovery.tags
            result["storage_metadata"] = discovery.storage_metadata
            result["storage_data_metadata"] = discovery.storage_data_metadata
            result["additional_metadata"] = discovery.additional_metadata
            result["data_quality_score"] = float(discovery.data_quality_score) if discovery.data_quality_score else None
            result["validation_status"] = discovery.validation_status
            result["validated_at"] = discovery.validated_at.isoformat() if discovery.validated_at else None
        
        return jsonify(result), 200
    except Exception as e:
        logger.error('FN:get_asset_by_id asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/assets/<asset_id>', methods=['PUT'])
@handle_error
def update_asset(asset_id):
    
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404

        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400

        if 'business_metadata' in data:
            asset.business_metadata = data['business_metadata']
        if 'technical_metadata' in data:
            asset.technical_metadata = data['technical_metadata']
        if 'operational_metadata' in data:
            asset.operational_metadata = data['operational_metadata']
        if 'columns' in data:
            asset.columns = data['columns']

        db.commit()
        db.refresh(asset)

        logger.info('FN:update_asset asset_id:{}'.format(asset_id))

        return jsonify({
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "technical_metadata": asset.technical_metadata,
            "operational_metadata": asset.operational_metadata,
            "business_metadata": asset.business_metadata,
            "columns": asset.columns
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:update_asset error:{}'.format(str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to update asset"}), 400
    finally:
        db.close()

@app.errorhandler(404)
def not_found(error):
    
    return jsonify({"error": "Resource not found"}), 404

@app.route('/api/connections/<int:connection_id>/list-files', methods=['GET'])
@handle_error
def list_connection_files(connection_id):
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        config_data = connection.config or {}
        

        container_name = request.args.get('container')
        share_name = request.args.get('share')
        folder_path = request.args.get('folder_path', '')
        file_extensions = request.args.get('file_extensions')
        file_extensions_list = [ext.strip() for ext in file_extensions.split(',')] if file_extensions else None
        

        if AZURE_AVAILABLE:
            try:
                from utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(config_data)
                

                if not container_name and not share_name:
                    containers = blob_client.list_containers()
                    file_shares = blob_client.list_file_shares()
                    return jsonify({
                        "success": True,
                        "containers": [c["name"] for c in containers],
                        "file_shares": [s["name"] for s in file_shares],
                        "message": "Specify 'container' or 'share' parameter to list files. Available services listed above."
                    }), 200
                

                if share_name:
                    files = blob_client.list_file_share_files(
                        share_name=share_name,
                        directory_path=folder_path,
                        file_extensions=file_extensions_list
                    )
                else:

                    is_datalake = config_data.get('storage_type') == 'datalake' or config_data.get('use_dfs_endpoint', False)
                    

                    if is_datalake and hasattr(blob_client, 'list_datalake_files'):

                        files = blob_client.list_datalake_files(
                            file_system_name=container_name,
                            path=folder_path,
                            file_extensions=file_extensions_list
                        )
                    else:

                        files = blob_client.list_blobs(
                            container_name=container_name,
                            folder_path=folder_path,
                            file_extensions=file_extensions_list
                        )
                

                files_list = []
                for file_info in files:
                    files_list.append({
                        "name": file_info.get("name"),
                        "full_path": file_info.get("full_path"),
                        "size": file_info.get("size", 0),
                        "content_type": file_info.get("content_type"),
                        "last_modified": file_info.get("last_modified").isoformat() if file_info.get("last_modified") else None,
                        "created_at": file_info.get("created_at").isoformat() if file_info.get("created_at") else None,
                        "etag": file_info.get("etag"),
                        "blob_type": file_info.get("blob_type")
                    })
                
                return jsonify({
                    "success": True,
                    "connection_id": connection_id,
                    "connection_name": connection.name,
                    "container": container_name,
                    "folder_path": folder_path,
                    "file_count": len(files_list),
                    "files": files_list
                }), 200
                
            except Exception as e:
                error_msg = str(e)
                if "AuthorizationFailure" in error_msg:
                    return jsonify({
                        "success": False,
                        "error": "Authorization Failure: The service principal does not have the required permissions.",
                        "message": "Required Azure RBAC Roles: Storage Blob Data Contributor or Storage Blob Data Reader"
                    }), 403
                return jsonify({
                    "success": False,
                    "error": f"Failed to list files: {error_msg}"
                }), 400
        else:
            return jsonify({
                "success": False,
                "error": "Azure utilities not available"
            }), 500
    except Exception as e:
        logger.error('FN:list_connection_files connection_id:{} error:{}'.format(connection_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/connections/test-config', methods=['GET', 'POST'])
@handle_error
def test_connection_config():
    if not AZURE_AVAILABLE:
        return jsonify({"error": "Azure utilities not available"}), 503
    
    try:
        config_data = {}
        
        if request.method == 'POST':
            data = request.json
            if not data:
                return jsonify({"error": "Request body is required"}), 400
            config_data = data.get('config', {})
        else:


            config_json_str = request.args.get('config')
            if config_json_str:
                try:
                    import json
                    config_data = json.loads(config_json_str)
                except json.JSONDecodeError:
                    return jsonify({"error": "Invalid JSON in 'config' query parameter"}), 400
            else:

                config_data = {
                    'connection_string': request.args.get('connection_string'),
                    'account_name': request.args.get('account_name'),
                    'tenant_id': request.args.get('tenant_id'),
                    'client_id': request.args.get('client_id'),
                    'client_secret': request.args.get('client_secret'),
                    'folder_path': request.args.get('folder_path'),
                    'storage_type': request.args.get('storage_type'),
                    'use_dfs_endpoint': request.args.get('use_dfs_endpoint', '').lower() == 'true',
                }

                config_data = {k: v for k, v in config_data.items() if v is not None}
        
        if not config_data:
            return jsonify({"error": "Config is required. For GET, provide 'config' query param as JSON string or individual params"}), 400
        

        try:
            from utils.azure_blob_client import create_azure_blob_client
            blob_client = create_azure_blob_client(config_data)
            test_result = blob_client.test_connection()
            

            return jsonify(test_result), 200
        except ValueError as e:
            return jsonify({"success": False, "message": str(e)}), 200
        except Exception as e:
            logger.error('FN:test_connection_config error:{}'.format(str(e)), exc_info=True)
            return jsonify({"success": False, "message": f"Connection test failed: {str(e)}"}), 200
    except Exception as e:
        logger.error('FN:test_connection_config error:{}'.format(str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400

@app.route('/api/connections/<int:connection_id>/test', methods=['POST'])
@handle_error
def test_connection(connection_id):
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        config_data = connection.config or {}
        

        if AZURE_AVAILABLE:
            try:
                from utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(config_data)
                test_result = blob_client.test_connection()
                
                if not test_result.get("success"):
                    return jsonify(test_result), 200
                

                airflow_triggered = False
                try:

                    airflow_base_url = app.config.get("AIRFLOW_BASE_URL")
                    if not airflow_base_url:
                        logger.warning('FN:test_connection AIRFLOW_BASE_URL not set, skipping Airflow trigger')
                        airflow_triggered = False
                        return jsonify({
                            "success": True,
                            "message": "Connection successful. Airflow DAG will run on next scheduled run.",
                            "container_count": test_result.get("container_count", 0),
                            "airflow_triggered": False,
                            "note": "AIRFLOW_BASE_URL not configured"
                        }), 200
                    dag_id = "azure_blob_discovery"
                    

                    import subprocess
                    import os as os_module
                    


                    default_airflow_home = os.path.join(os.path.dirname(os.path.dirname(__file__)), "airflow")
                    airflow_home = app.config.get("AIRFLOW_HOME", default_airflow_home)
                    airflow_bin = os.path.join(airflow_home, "venv", "bin", "airflow")
                    env = os_module.environ.copy()
                    env["AIRFLOW_HOME"] = airflow_home
                    

                    conf_json = f'{{"note": "Triggered from connection test: {connection_id}"}}'
                    result = subprocess.run(
                        [airflow_bin, "dags", "trigger", dag_id, "--conf", conf_json],
                        cwd=airflow_home,
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    if result.returncode == 0:
                        airflow_triggered = True
                        logger.info(f'FN:test_connection connection_id:{connection_id} airflow_dag_triggered:{dag_id}')
                    else:
                        logger.warning(f'FN:test_connection connection_id:{connection_id} airflow_trigger_failed:returncode:{result.returncode} stderr:{result.stderr}')
                except Exception as e:
                    logger.warning(f'FN:test_connection connection_id:{connection_id} airflow_trigger_error:{str(e)}')

                    try:
                        import threading
                        import importlib.util
                        discovery_runner_path = os.path.join(os.path.dirname(__file__), 'discovery_runner.py')
                        if os.path.exists(discovery_runner_path):
                            spec = importlib.util.spec_from_file_location("discovery_runner", discovery_runner_path)
                            discovery_runner = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(discovery_runner)
                            run_discovery_for_connection = discovery_runner.run_discovery_for_connection
                            
                            thread = threading.Thread(
                                target=run_discovery_for_connection,
                                args=(connection_id,),
                                daemon=True
                            )
                            thread.start()
                            logger.info(f'FN:test_connection connection_id:{connection_id} discovery_runner_triggered')
                    except Exception as e2:
                        logger.error(f'FN:test_connection connection_id:{connection_id} discovery_runner_error:{str(e2)}')
                
                return jsonify({
                    "success": True,
                    "message": "Connection successful. Airflow DAG triggered for discovery." if airflow_triggered else "Connection successful. Discovery will run on next scheduled run.",
                    "container_count": test_result.get("container_count", 0),
                    "airflow_triggered": airflow_triggered
                }), 200
            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": str(e),
                    "container_count": 0
                }), 200
        else:

            import threading
            import importlib.util
            
            discovery_runner_path = os.path.join(os.path.dirname(__file__), 'discovery_runner.py')
            if os.path.exists(discovery_runner_path):
                spec = importlib.util.spec_from_file_location("discovery_runner", discovery_runner_path)
                discovery_runner = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(discovery_runner)
                run_discovery_for_connection = discovery_runner.run_discovery_for_connection
                
                thread = threading.Thread(
                    target=run_discovery_for_connection,
                    args=(connection_id,),
                    daemon=True
                )
                thread.start()
            else:
                logger.warning(f"Discovery runner not found at {discovery_runner_path}")
            
            return jsonify({
                "success": True,
                "message": "Discovery started in background (Azure utilities check skipped)",
                "container_count": 0,
                "discovery_triggered": True
            }), 200
    finally:
        db.close()

@app.route('/api/connections/<int:connection_id>/containers', methods=['GET'])
@handle_error
def list_containers(connection_id):
    if not AZURE_AVAILABLE:
        return jsonify({"error": "Azure utilities not available"}), 503
    
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        config_data = connection.config or {}
        
        try:
            from utils.azure_blob_client import create_azure_blob_client
            blob_client = create_azure_blob_client(config_data)
            

            containers = blob_client.list_containers()
            file_shares = blob_client.list_file_shares()
            queues = blob_client.list_queues()
            tables = blob_client.list_tables()
            
            return jsonify({
                "containers": containers,
                "file_shares": file_shares,
                "queues": queues,
                "tables": tables
            }), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 400
    finally:
        db.close()

def clean_for_json(obj):
    import base64
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, (bytes, bytearray)):

        return base64.b64encode(obj).decode('utf-8')
    elif isinstance(obj, (datetime,)):
        return obj.isoformat()
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:

        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

def build_technical_metadata(asset_id, blob_info, file_extension, blob_path, container_name, storage_account, file_hash, schema_hash, metadata, current_date):
    created_at = blob_info.get("created_at")
    if created_at and hasattr(created_at, 'isoformat'):
        created_at = created_at.isoformat()
    elif created_at:
        created_at = str(created_at)
    
    last_modified = blob_info.get("last_modified")
    if last_modified and hasattr(last_modified, 'isoformat'):
        last_modified = last_modified.isoformat()
    elif last_modified:
        last_modified = str(last_modified)
    

    azure_metadata_dict = blob_info.get("metadata", {})
    if not isinstance(azure_metadata_dict, dict):
        azure_metadata_dict = {}

    azure_metadata_dict = clean_for_json(azure_metadata_dict)
    

    file_hash_str = str(file_hash) if file_hash else ""
    schema_hash_str = str(schema_hash) if schema_hash else ""
    


    size_bytes = blob_info.get("size") or blob_info.get("size_bytes") or 0

    if size_bytes is None or size_bytes == "":
        size_bytes = 0
    try:
        size_bytes = int(size_bytes)

        if size_bytes == 0:
            logger.warning('FN:build_technical_metadata blob_path:{} message:Size is 0, may indicate missing size from Azure'.format(blob_path))
    except (ValueError, TypeError) as e:
        logger.warning('FN:build_technical_metadata blob_path:{} size_value:{} message:Could not convert size to int error:{}'.format(blob_path, size_bytes, str(e)))
        size_bytes = 0
    

    format_value = file_extension or "unknown"
    if format_value == "unknown" or not format_value:

        content_type = blob_info.get("content_type", "")
        if content_type and "/" in content_type:
            format_value = content_type.split("/")[-1]
        elif content_type:
            format_value = content_type
    

    tech_meta = {
        "asset_id": asset_id,
        "asset_type": file_extension or "blob",
        "format": format_value,
        "content_type": blob_info.get("content_type", "application/octet-stream"),
        "size_bytes": size_bytes,
        "size": size_bytes,
        "location": blob_path,
        "container": container_name,
        "storage_account": storage_account,
        "created_at": created_at or current_date,
        "last_modified": last_modified or current_date,
        "file_extension": f".{file_extension}" if file_extension else "",
        "file_hash": file_hash_str,
        "schema_hash": schema_hash_str,

        "etag": blob_info.get("etag", "").strip('"') if blob_info.get("etag") else None,
        "blob_type": blob_info.get("blob_type", "Block blob"),
        "access_tier": blob_info.get("access_tier"),
        "lease_status": blob_info.get("lease_status"),
        "lease_state": blob_info.get("lease_state"),
        "content_encoding": blob_info.get("content_encoding"),
        "content_language": blob_info.get("content_language"),
        "cache_control": blob_info.get("cache_control"),
        "content_md5": blob_info.get("content_md5"),
        "content_disposition": blob_info.get("content_disposition"),

        "azure_metadata": azure_metadata_dict,

        **metadata.get("file_metadata", {}).get("format_specific", {}),

        "azure_storage_metadata": metadata.get("storage_metadata", {}).get("azure", {})
    }
    

    return clean_for_json(tech_meta)

def build_operational_metadata(azure_properties, current_date):

    owner = azure_properties.get("metadata", {}).get("owner") if azure_properties else None
    if not owner:
        owner = "system"
    

    access_level = "internal"
    if azure_properties:
        lease_status = azure_properties.get("lease_status")
        if lease_status and isinstance(lease_status, str):
            lease_status = lease_status.lower()
        if lease_status == "locked":
            access_level = "restricted"
        elif azure_properties.get("access_tier") == "Archive":
            access_level = "archived"
    

    return clean_for_json({
        "owner": str(owner),
        "created_by": str(azure_properties.get("metadata", {}).get("created_by", "azure_blob_discovery") if azure_properties else "azure_blob_discovery"),
        "last_updated_by": str(azure_properties.get("metadata", {}).get("last_updated_by", "azure_blob_discovery") if azure_properties else "azure_blob_discovery"),
        "last_updated_at": current_date,
        "access_level": access_level,
        "approval_status": "pending_review",

        "lease_status": azure_properties.get("lease_status") if azure_properties else None,
        "access_tier": azure_properties.get("access_tier") if azure_properties else None,
        "etag": azure_properties.get("etag", "").strip('"') if azure_properties and azure_properties.get("etag") else None
    })

def build_business_metadata(blob_info, azure_properties, file_extension, container_name):
    azure_metadata = azure_properties.get("metadata", {}) if azure_properties else {}
    

    azure_metadata = clean_for_json(azure_metadata)
    

    description = azure_metadata.get("description") or f"Azure Blob Storage file: {blob_info.get('name', 'unknown')}"
    business_owner = azure_metadata.get("business_owner") or azure_metadata.get("owner") or "system"
    department = azure_metadata.get("department") or "Data Engineering"
    classification = azure_metadata.get("classification") or "internal"
    sensitivity_level = azure_metadata.get("sensitivity_level") or azure_metadata.get("sensitivity") or "medium"
    

    tags = []
    if azure_metadata.get("tags"):
        tags_value = azure_metadata["tags"]
        if isinstance(tags_value, str):
            tags = [t.strip() for t in tags_value.split(",")]
        elif isinstance(tags_value, list):
            tags = [str(t) for t in tags_value]
    

    if container_name and container_name not in tags:
        tags.append(container_name)
    

    return clean_for_json({
        "description": str(description),
        "data_type": file_extension or "unknown",
        "business_owner": str(business_owner),
        "department": str(department),
        "classification": str(classification),
        "sensitivity_level": str(sensitivity_level),
        "tags": tags,

        "container": container_name,
        "content_language": azure_properties.get("content_language") if azure_properties else None,
        "azure_metadata_tags": azure_metadata
    })

@app.route('/api/connections/<int:connection_id>/discover-stream', methods=['POST'])
@handle_error
def discover_assets_stream(connection_id):
    """Stream discovery progress using Server-Sent Events (SSE)"""
    if not AZURE_AVAILABLE:
        return jsonify({"error": "Azure utilities not available"}), 503
    
    # Capture request data and connection info before generator (Flask context issue fix)
    request_data = request.json or {}
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        # Extract all needed data before generator
        connector_type = connection.connector_type
        config_data = connection.config or {}
    finally:
        db.close()
    
    def generate():
        import json
        # No Flask context needed - we've already captured all data
        # No database session needed - all data is pre-captured
        try:
            # Step 1: Parse folder path if needed
            containers = request_data.get('containers', config_data.get('containers', []))
            folder_path = request_data.get('folder_path', config_data.get('folder_path', ''))
            
            parsed_container = None
            parsed_path = folder_path
            parsed_account_name = None
            
            # Create a copy of config_data to modify
            working_config = config_data.copy()
            
            if folder_path and (folder_path.startswith('abfs://') or folder_path.startswith('abfss://') or folder_path.startswith('https://') or folder_path.startswith('http://')):
                try:
                    from utils.storage_path_parser import parse_storage_path
                    parsed = parse_storage_path(folder_path)
                    parsed_container = parsed.get('container')
                    parsed_path = parsed.get('path', '')
                    parsed_account_name = parsed.get('account_name')
                    parsed_storage_type = parsed.get('type')
                    
                    # Update config if account_name is different or if it's a Data Lake URL
                    if parsed_account_name and parsed_account_name != working_config.get('account_name'):
                        working_config['account_name'] = parsed_account_name
                    
                    # If it's a Data Lake URL (abfs/abfss), ensure use_dfs_endpoint is set
                    if parsed_storage_type == 'azure_datalake' or folder_path.startswith(('abfs://', 'abfss://')):
                        working_config['use_dfs_endpoint'] = True
                        working_config['storage_type'] = 'datalake'
                    
                    # ALWAYS use the container from the URL if it's specified in the path
                    # This overrides any containers passed from the frontend
                    if parsed_container:
                        containers = [parsed_container]
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'Using container from URL: {parsed_container}', 'container': parsed_container})}\n\n"
                    folder_path = parsed_path
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'warning', 'message': f'Failed to parse storage URL: {str(e)}'})}\n\n"
            
            # Step 2: Authenticate FIRST
            yield f"data: {json.dumps({'type': 'progress', 'message': 'Authenticating with Azure...', 'step': 'auth'})}\n\n"
            
            try:
                from utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(working_config)
                yield f"data: {json.dumps({'type': 'progress', 'message': 'Authentication successful', 'step': 'auth_complete'})}\n\n"
            except ValueError as e:
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                return
            
            # Step 3: Discover containers (if not provided)
            if not containers:
                yield f"data: {json.dumps({'type': 'progress', 'message': 'Discovering containers...', 'step': 'containers'})}\n\n"
                try:
                    containers_list = blob_client.list_containers()
                    containers = [c["name"] for c in containers_list]
                    yield f"data: {json.dumps({'type': 'progress', 'message': f'Found {len(containers)} container(s)', 'containers': containers})}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Failed to discover containers: {str(e)}'})}\n\n"
                    return
            
            if not containers:
                yield f"data: {json.dumps({'type': 'error', 'message': 'No containers found in storage account'})}\n\n"
                return
            
            # Step 4: Process each container sequentially
            total_discovered = 0
            total_updated = 0
            total_skipped = 0
            
            for container_idx, container_name in enumerate(containers):
                yield f"data: {json.dumps({'type': 'container', 'container': container_name, 'message': f'Processing container {container_idx + 1}/{len(containers)}: {container_name}', 'container_index': container_idx + 1, 'total_containers': len(containers)})}\n\n"
                
                try:
                    # Discover all files recursively in this container
                    is_datalake = working_config.get('storage_type') == 'datalake' or working_config.get('use_dfs_endpoint', False)
                    
                    # Log what we're about to do
                    path_display = folder_path if folder_path else "root"
                    yield f"data: {json.dumps({'type': 'progress', 'message': f'Listing files in {container_name} (path: {path_display})...', 'container': container_name})}\n\n"
                    
                    if is_datalake and hasattr(blob_client, 'list_datalake_files'):
                        blobs = blob_client.list_datalake_files(
                            file_system_name=container_name,
                            path=folder_path,
                            file_extensions=None
                        )
                    else:
                        blobs = blob_client.list_blobs(
                            container_name=container_name,
                            folder_path=folder_path,
                            file_extensions=None
                        )
                    
                    yield f"data: {json.dumps({'type': 'progress', 'message': f'Found {len(blobs)} files in {container_name} (including all subfolders)', 'file_count': len(blobs), 'container': container_name})}\n\n"
                    
                    if len(blobs) > 0:
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'Processing {len(blobs)} files from {container_name}...', 'step': 'processing', 'container': container_name})}\n\n"
                        
                        # Show all files (for small datasets) or every Nth file (for large datasets)
                        for i, blob_info in enumerate(blobs):
                            blob_name = blob_info.get("name", "unknown")
                            full_path = blob_info.get("full_path", blob_name)
                            
                            # Show all files if < 100, otherwise show first 20 and every 50th
                            if len(blobs) < 100 or i < 20 or (i + 1) % 50 == 0:
                                yield f"data: {json.dumps({'type': 'file', 'file': blob_name, 'full_path': full_path, 'container': container_name, 'index': i+1, 'total': len(blobs)})}\n\n"
                            
                            total_discovered += 1
                            
                            # Progress update every 100 files or at milestones
                            if (i + 1) % 100 == 0 or (i + 1) == len(blobs):
                                percentage = round((i+1)/len(blobs)*100) if len(blobs) > 0 else 0
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'Processed {i+1}/{len(blobs)} files in {container_name} ({percentage}%)', 'processed': i+1, 'total': len(blobs), 'percentage': percentage, 'container': container_name})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'No files found in {container_name}', 'container': container_name})}\n\n"
                    
                except Exception as e:
                    import traceback
                    error_details = str(e)
                    error_trace = traceback.format_exc()
                    print(f'FN:discover_assets_stream container:{container_name} path:{folder_path} error:{error_details}')
                    print(f'FN:discover_assets_stream traceback:{error_trace}')
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Error processing container {container_name}: {error_details}', 'container': container_name})}\n\n"
                    continue
            
            yield f"data: {json.dumps({'type': 'complete', 'message': 'Discovery complete', 'discovered': total_discovered, 'updated': total_updated, 'skipped': total_skipped})}\n\n"
            
        except Exception as e:
            error_msg = str(e)
            # Use print instead of logger to avoid context issues
            print(f'FN:discover_assets_stream error:{error_msg}')
            yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
    })

@app.route('/api/connections/<int:connection_id>/discover', methods=['POST'])
@handle_error
def discover_assets(connection_id):
    if not AZURE_AVAILABLE:
        return jsonify({"error": "Azure utilities not available"}), 503
    
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        data = request.json or {}
        config_data = connection.config or {}
        containers = data.get('containers', config_data.get('containers', []))
        folder_path = data.get('folder_path', config_data.get('folder_path', ''))
        skip_deduplication = data.get('skip_deduplication', False)  # Skip deduplication for test discoveries
        
        parsed_container = None
        parsed_path = folder_path
        parsed_account_name = None
        
        if folder_path and (folder_path.startswith('abfs://') or folder_path.startswith('abfss://') or folder_path.startswith('https://') or folder_path.startswith('http://')):
            try:
                from utils.storage_path_parser import parse_storage_path
                parsed = parse_storage_path(folder_path)
                parsed_container = parsed.get('container')
                parsed_path = parsed.get('path', '')
                parsed_account_name = parsed.get('account_name')
                parsed_storage_type = parsed.get('type')
                
                logger.info('FN:discover_assets parsed_storage_url:{} container:{} path:{} account_name:{} type:{}'.format(
                    folder_path, parsed_container, parsed_path, parsed_account_name, parsed_storage_type
                ))
                
                # Update config if account_name is different or if it's a Data Lake URL
                if parsed_account_name and parsed_account_name != config_data.get('account_name'):
                    logger.info('FN:discover_assets message:Using account_name from URL:{}'.format(parsed_account_name))
                    config_data['account_name'] = parsed_account_name
                
                # If it's a Data Lake URL (abfs/abfss), ensure use_dfs_endpoint is set
                if parsed_storage_type == 'azure_datalake' or folder_path.startswith(('abfs://', 'abfss://')):
                    config_data['use_dfs_endpoint'] = True
                    config_data['storage_type'] = 'datalake'
                    logger.info('FN:discover_assets message:Detected Data Lake URL, enabling DFS endpoint')
                
                # ALWAYS use the container from the URL if it's specified in the path
                # This overrides any containers passed from the frontend
                if parsed_container:
                    containers = [parsed_container]
                    logger.info('FN:discover_assets message:Using container from URL:{} (overriding provided containers)'.format(parsed_container))
                folder_path = parsed_path
            except Exception as e:
                logger.warning('FN:discover_assets failed_to_parse_storage_url:{} error:{}'.format(folder_path, str(e)))
                parsed_container = None
                parsed_path = folder_path

        try:
            from utils.azure_blob_client import create_azure_blob_client
            blob_client = create_azure_blob_client(config_data)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        

        if not containers:
            try:
                containers_list = blob_client.list_containers()
                containers = [c["name"] for c in containers_list]
                logger.info('FN:discover_assets connection_id:{} auto_discovered_containers_count:{}'.format(connection_id, len(containers)))
            except Exception as e:
                logger.error('FN:discover_assets connection_id:{} message:Error discovering containers error:{}'.format(connection_id, str(e)))
                return jsonify({"error": f"Failed to discover containers: {str(e)}"}), 400
        
        if not containers:
            return jsonify({"error": "No containers found in storage account"}), 400
        
        try:
            discovered_assets = []
            folders_found = {}
            assets_by_folder = {}
            discovered_assets_lock = Lock()
            folders_lock = Lock()
            
            def process_container(container_name):
                container_discovered_assets = []
                container_folders_found = set()
                container_assets_by_folder = {}
                container_skipped_count = 0
                
                # OPTIMIZATION: Pre-load all existing assets for this connector into memory (one query instead of N queries)
                existing_assets_map = {}
                connector_id = f"azure_blob_{connection.name}"
                if AZURE_AVAILABLE and not skip_deduplication:
                    try:
                        preload_db = SessionLocal()
                        try:
                            existing_assets = preload_db.query(Asset).filter(
                                Asset.connector_id == connector_id
                            ).all()
                            
                            from utils.asset_deduplication import normalize_path
                            for asset in existing_assets:
                                tech_meta = asset.technical_metadata or {}
                                stored_location = tech_meta.get('location') or tech_meta.get('storage_path') or ""
                                normalized_path_key = normalize_path(stored_location)
                                if normalized_path_key:
                                    existing_assets_map[normalized_path_key] = asset
                            
                            logger.info('FN:discover_assets connector_id:{} container_name:{} message:Pre-loaded {} existing assets into memory for fast lookup'.format(
                                connector_id, container_name, len(existing_assets_map)
                            ))
                        finally:
                            preload_db.close()
                    except Exception as e:
                        logger.warning('FN:discover_assets connector_id:{} container_name:{} message:Failed to pre-load existing assets, will use per-file queries error:{}'.format(
                            connector_id, container_name, str(e)
                        ))
                        existing_assets_map = {}
                
                try:
                    logger.info('FN:discover_assets container_name:{} folder_path:{} message:Listing files'.format(container_name, folder_path))
                    

                    is_datalake = config_data.get('storage_type') == 'datalake' or config_data.get('use_dfs_endpoint', False)
                    if is_datalake and hasattr(blob_client, 'list_datalake_files'):

                        blobs = blob_client.list_datalake_files(
                            file_system_name=container_name,
                            path=folder_path,
                            file_extensions=None
                        )
                        logger.info('FN:discover_assets container_name:{} message:Using Data Lake Gen2 API'.format(container_name))
                    else:

                        blobs = blob_client.list_blobs(
                            container_name=container_name,
                            folder_path=folder_path,
                            file_extensions=None
                        )
                        logger.info('FN:discover_assets container_name:{} message:Using Blob Storage API'.format(container_name))
                    
                    logger.info('FN:discover_assets container_name:{} blob_count:{}'.format(container_name, len(blobs)))
                    

                    folders_in_container = set()
                    assets_in_folders = {}
                    
                    for blob_info in blobs:
                        blob_path = blob_info["full_path"]

                        if "/" in blob_path:
                            folder = "/".join(blob_path.split("/")[:-1])
                        else:
                            folder = ""
                        
                        folders_in_container.add(folder)
                        if folder not in assets_in_folders:
                            assets_in_folders[folder] = []
                        assets_in_folders[folder].append(blob_info)
                    
                    container_folders_found = list(folders_in_container)
                    container_assets_by_folder = assets_in_folders
                    
                    if len(blobs) == 0:
                        logger.warning('FN:discover_assets container_name:{} folder_path:{} message:No blobs found'.format(container_name, folder_path))
                    else:
                        sample_names = [b.get('name', 'unknown') for b in blobs[:5]]
                        logger.info('FN:discover_assets container_name:{} sample_blob_names:{}'.format(container_name, sample_names))
                    



                    if len(blobs) > 2000:
                        max_workers = 60
                    elif len(blobs) > 500:
                        max_workers = 50
                    else:
                        max_workers = 20

                    # Avoid exhausting DB connections (each worker may open a DB session for dedup checks)
                    try:
                        max_workers_cap = int(os.getenv("DISCOVERY_MAX_WORKERS", "20"))
                        if max_workers_cap > 0:
                            max_workers = min(max_workers, max_workers_cap)
                    except Exception:
                        max_workers = min(max_workers, 20)
                    logger.info('FN:discover_assets container_name:{} total_blobs:{} message:Processing with {} concurrent workers'.format(container_name, len(blobs), max_workers))
                    
                    def process_blob(blob_info):
                        try:
                            blob_path = blob_info["full_path"]
                            blob_name = blob_info.get("name", "")
                            file_extension = blob_name.split(".")[-1].lower() if blob_name and "." in blob_name else ""
                            connector_id = f"azure_blob_{connection.name}"
                            

                            asset_name = blob_info.get("name", "unknown")
                            asset_folder = ""
                            if "/" in blob_path:
                                parts = blob_path.split("/")
                                asset_folder = "/".join(parts[:-1])
                                asset_name = parts[-1]
                            

                            thread_db = SessionLocal()
                            try:


                                existing_asset = None
                                
                                # Initialize azure_properties (always needed, regardless of deduplication)
                                azure_properties = {
                                    "etag": blob_info.get("etag", ""),
                                    "size": blob_info.get("size", 0),
                                    "content_type": blob_info.get("content_type", "application/octet-stream"),
                                    "created_at": blob_info.get("created_at"),
                                    "last_modified": blob_info.get("last_modified"),
                                    "access_tier": blob_info.get("access_tier"),
                                    "lease_status": blob_info.get("lease_status"),
                                    "content_encoding": blob_info.get("content_encoding"),
                                    "content_language": blob_info.get("content_language"),
                                    "cache_control": blob_info.get("cache_control"),
                                    "metadata": blob_info.get("metadata", {})
                                }
                                
                                # OPTIMIZED: Use pre-loaded existing_assets_map for fast in-memory lookup instead of DB query per file
                                # Skip deduplication for test discoveries (from ConnectorsPage)
                                # Only do deduplication for refresh operations (from AssetsPage)
                                if AZURE_AVAILABLE and not skip_deduplication:
                                    try:
                                        # Fast in-memory lookup using pre-loaded map
                                        from utils.asset_deduplication import normalize_path
                                        normalized_blob_path = normalize_path(blob_path)
                                        if normalized_blob_path and normalized_blob_path in existing_assets_map:
                                            existing_asset = existing_assets_map[normalized_blob_path]
                                            # Refresh the asset object from DB to ensure we have latest data
                                            existing_asset = thread_db.query(Asset).filter(Asset.id == existing_asset.id).first()
                                            if existing_asset:
                                                logger.debug('FN:discover_assets blob_path:{} existing_asset_id:{} message:Found existing asset via fast lookup (refresh)'.format(blob_path, existing_asset.id))
                                        else:
                                            # Not in pre-loaded map, so it's definitely new
                                            existing_asset = None
                                    except Exception as e:
                                        logger.error('FN:discover_assets blob_path:{} error:Fast lookup failed, falling back to DB query error:{}'.format(blob_path, str(e)))
                                        # Fallback to original DB query if in-memory lookup fails
                                        try:
                                            existing_asset = check_asset_exists(thread_db, connector_id, blob_path)
                                        except Exception:
                                            existing_asset = None
                                elif skip_deduplication:
                                    logger.debug('FN:discover_assets blob_path:{} message:Skipping deduplication (test discovery)'.format(blob_path))
                                



                                    azure_properties = {
                                        "etag": blob_info.get("etag", ""),
                                        "size": blob_info.get("size", 0),
                                        "content_type": blob_info.get("content_type", "application/octet-stream"),
                                        "created_at": blob_info.get("created_at"),
                                        "last_modified": blob_info.get("last_modified"),
                                        "access_tier": blob_info.get("access_tier"),
                                        "lease_status": blob_info.get("lease_status"),
                                        "content_encoding": blob_info.get("content_encoding"),
                                        "content_language": blob_info.get("content_language"),
                                        "cache_control": blob_info.get("cache_control"),
                                        "metadata": blob_info.get("metadata", {})
                                    }
                                


                                if not azure_properties.get("size") or not azure_properties.get("last_modified"):
                                    try:
                                        additional_props = blob_client.get_blob_properties(container_name, blob_path)
                                        if additional_props:
                                            azure_properties.update(additional_props)
                                            logger.debug('FN:discover_assets container_name:{} blob_path:{} message:Fetched additional properties'.format(container_name, blob_path))
                                    except Exception as e:
                                        logger.debug('FN:discover_assets container_name:{} blob_path:{} message:Using list_blobs properties only error:{}'.format(container_name, blob_path, str(e)))
                                



                                file_sample = None
                                try:
                                    if file_extension == "parquet":

                                        file_sample = blob_client.get_blob_tail(container_name, blob_path, max_bytes=8192)
                                    elif file_extension in ["csv", "json"]:

                                        file_sample = blob_client.get_blob_sample(container_name, blob_path, max_bytes=8192)
                                    else:
                                        file_sample = blob_client.get_blob_sample(container_name, blob_path, max_bytes=1024)
                                except Exception as e:
                                    logger.warning('FN:discover_assets container_name:{} blob_path:{} message:Could not get sample error:{}'.format(container_name, blob_path, str(e)))
                                

                                enhanced_blob_info = {**blob_info, **azure_properties}
                                if file_sample:
                                    metadata = extract_file_metadata(enhanced_blob_info, file_sample)
                                else:
                                    metadata = extract_file_metadata(enhanced_blob_info, None)
                                

                                file_hash = metadata.get("file_hash", generate_file_hash(b""))
                                schema_hash = metadata.get("schema_hash", generate_schema_hash({}))
                                

                                should_update, schema_changed = should_update_or_insert(
                                    existing_asset,
                                    file_hash,
                                    schema_hash
                                )
                                

                                if existing_asset:
                                    if not should_update:
                                        logger.info('FN:discover_assets blob_path:{} existing_asset_id:{} message:Skipping unchanged asset (deduplication)'.format(blob_path, existing_asset.id))
                                        thread_db.close()
                                        return None
                                    else:
                                        logger.info('FN:discover_assets blob_path:{} existing_asset_id:{} schema_changed:{} message:Updating existing asset'.format(blob_path, existing_asset.id, schema_changed))
                                
                                current_date = datetime.utcnow().isoformat()
                                
                                if existing_asset and schema_changed:

                                    existing_asset.name = blob_info["name"]
                                    existing_asset.type = file_extension or "blob"
                                    

                                    technical_meta = build_technical_metadata(
                                        asset_id=existing_asset.id,
                                        blob_info=enhanced_blob_info,
                                        file_extension=file_extension,
                                        blob_path=blob_path,
                                        container_name=container_name,
                                        storage_account=config_data.get("account_name", "unknown"),
                                        file_hash=file_hash,
                                        schema_hash=schema_hash,
                                        metadata=metadata,
                                        current_date=current_date
                                    )
                                    

                                    operational_meta = build_operational_metadata(
                                        azure_properties=azure_properties,
                                        current_date=current_date
                                    )
                                    

                                    business_meta = build_business_metadata(
                                        blob_info=enhanced_blob_info,
                                        azure_properties=azure_properties,
                                        file_extension=file_extension,
                                        container_name=container_name
                                    )
                                    
                                    existing_asset.technical_metadata = technical_meta
                                    existing_asset.operational_metadata = operational_meta
                                    existing_asset.business_metadata = business_meta
                                    existing_asset.columns = clean_for_json(metadata.get("schema_json", {}).get("columns", []))
                                    existing_asset.operational_metadata = {
                                        **(existing_asset.operational_metadata or {}),
                                        "last_updated_by": "azure_blob_discovery",
                                        "last_updated_at": current_date,
                                    }
                                    
                                    return {
                                        "action": "updated",
                                        "asset": existing_asset,
                                        "name": asset_name,
                                        "folder": asset_folder,
                                        "container": container_name,
                                        "thread_db": thread_db
                                    }
                                else:


                                    normalized_path = blob_path.strip('/').replace('/', '_').replace(' ', '_')
                                    asset_id = f"azure_blob_{connection.name}_{normalized_path}"
                                    

                                    technical_meta = build_technical_metadata(
                                        asset_id=asset_id,
                                        blob_info=enhanced_blob_info,
                                        file_extension=file_extension,
                                        blob_path=blob_path,
                                        container_name=container_name,
                                        storage_account=config_data.get("account_name", "unknown"),
                                        file_hash=file_hash,
                                        schema_hash=schema_hash,
                                        metadata=metadata,
                                        current_date=current_date
                                    )
                                    
                                    operational_meta = build_operational_metadata(
                                        azure_properties=azure_properties,
                                        current_date=current_date
                                    )
                                    
                                    business_meta = build_business_metadata(
                                        blob_info=enhanced_blob_info,
                                        azure_properties=azure_properties,
                                        file_extension=file_extension,
                                        container_name=container_name
                                    )
                                    
                                    columns_clean = clean_for_json(metadata.get("schema_json", {}).get("columns", []))

                                    schema_json_full = clean_for_json(metadata.get("schema_json", {}))
                                    
                                    # IMPORTANT: close the thread DB session for created assets to avoid connection leaks
                                    # (main thread will create/save the Asset using its own session)
                                    thread_db.close()
                                    return {
                                        "action": "created",
                                        "asset_data": {
                                            "id": asset_id,
                                            "name": blob_info["name"],
                                            "type": file_extension or "blob",
                                            "catalog": connection.name,
                                            "connector_id": connector_id,
                                            "discovered_at": current_date,
                                            "technical_metadata": technical_meta,
                                            "operational_metadata": operational_meta,
                                            "business_metadata": business_meta,
                                            "columns": columns_clean,
                                            "schema_json": schema_json_full
                                        },
                                        "name": asset_name,
                                        "folder": asset_folder,
                                        "container": container_name,
                                        "blob_path": blob_path,
                                        "config_data": config_data,
                                        "connection_id": connection_id,
                                        "connection_name": connection.name,
                                    }
                            except Exception as e:
                                thread_db.close()
                                raise e
                        except Exception as e:
                            logger.error('FN:discover_assets container_name:{} blob_name:{} error:{}'.format(container_name, blob_info.get('name', 'unknown'), str(e)), exc_info=True)
                            if 'thread_db' in locals():
                                thread_db.close()
                            return None
                    

                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(process_blob, blob_info): blob_info for blob_info in blobs}
                        
                        for future in as_completed(futures):
                            try:
                                result = future.result()
                                if result:
                                    container_discovered_assets.append(result)
                                elif result is None:

                                    container_skipped_count += 1
                            except Exception as e:
                                blob_info = futures[future]
                                logger.error('FN:discover_assets container_name:{} blob_name:{} error:{}'.format(container_name, blob_info.get('name', 'unknown'), str(e)), exc_info=True)
                    
                    return {
                        "discovered_assets": container_discovered_assets,
                        "folders_found": container_folders_found,
                        "assets_by_folder": container_assets_by_folder,
                        "skipped_count": container_skipped_count
                    }
                except Exception as e:
                    logger.error('FN:discover_assets container_name:{} message:Error listing blobs error:{}'.format(container_name, str(e)), exc_info=True)
                    return {
                        "discovered_assets": [],
                        "folders_found": [],
                        "assets_by_folder": {},
                        "skipped_count": 0
                    }
            

            total_skipped_from_containers = 0
            


            logger.info('FN:discover_assets total_containers:{} message:Processing containers with 10 concurrent workers'.format(len(containers)))
            with ThreadPoolExecutor(max_workers=min(10, len(containers))) as container_executor:
                container_futures = {container_executor.submit(process_container, container_name): container_name for container_name in containers}
                
                for future in as_completed(container_futures):
                    try:
                        result = future.result()
                        if result:
                            with discovered_assets_lock:
                                discovered_assets.extend(result["discovered_assets"])

                                total_skipped_from_containers += result.get("skipped_count", 0)
                            with folders_lock:
                                container_name = container_futures[future]
                                folders_found[container_name] = result["folders_found"]
                                assets_by_folder[container_name] = result["assets_by_folder"]
                    except Exception as e:
                        container_name = container_futures[future]
                        logger.error('FN:discover_assets container_name:{} message:Error processing container error:{}'.format(container_name, str(e)), exc_info=True)
            
            logger.info('FN:discover_assets total_assets_to_process:{}'.format(len(discovered_assets)))
            




            batch_size = int(os.getenv("DISCOVERY_BATCH_SIZE", "1500"))
            total_assets = len(discovered_assets)
            
            if total_assets > 1000:
                logger.info('FN:discover_assets total_assets:{} batch_size:{} message:Large discovery detected'.format(total_assets, batch_size))
            

            created_count = 0
            updated_count = 0
            skipped_count = total_skipped_from_containers
            

            for batch_start in range(0, total_assets, batch_size):
                batch_end = min(batch_start + batch_size, total_assets)
                batch = discovered_assets[batch_start:batch_end]
                
                batch_num = batch_start//batch_size + 1
                total_batches = (total_assets + batch_size - 1)//batch_size
                logger.info('FN:discover_assets batch_number:{} total_batches:{} batch_start:{} batch_end:{} total_assets:{}'.format(batch_num, total_batches, batch_start+1, batch_end, total_assets))
                
                # Collect discoveries for bulk insert
                discoveries_to_add = []

                # Pre-check for duplicate Asset IDs in this batch to avoid IntegrityError at flush/commit time.
                created_ids_in_batch = []
                for it in batch:
                    try:
                        if it and it.get("action") == "created":
                            asset_id = (it.get("asset_data") or {}).get("id")
                            if asset_id:
                                created_ids_in_batch.append(asset_id)
                    except Exception:
                        continue

                existing_ids_in_batch = set()
                if created_ids_in_batch:
                    try:
                        existing_ids_in_batch = set(
                            x[0] for x in db.query(Asset.id).filter(Asset.id.in_(created_ids_in_batch)).all()
                        )
                    except Exception:
                        existing_ids_in_batch = set()

                seen_created_ids_in_batch = set()
                
                for item in batch:
                    try:
                        if item is None:

                            skipped_count += 1
                            continue
                        elif item.get("action") == "updated":

                            # OPTIMIZED: Merge updated assets into main session instead of individual commits
                            thread_db = item.get("thread_db")
                            if thread_db:
                                try:
                                    updated_asset = item.get("asset")
                                    if updated_asset:
                                        # Merge the object from thread_db into main db session
                                        # This avoids individual commits and allows batch processing
                                        db.merge(updated_asset)
                                    updated_count += 1
                                except Exception as e:
                                    logger.error('FN:discover_assets message:Error merging updated asset error:{}'.format(str(e)), exc_info=True)
                                    skipped_count += 1
                                finally:
                                    thread_db.close()
                            else:

                                updated_count += 1
                        elif item.get("action") == "created":

                            asset_data = item["asset_data"]
                            try:
                                asset_id = asset_data.get("id")
                                if not asset_id:
                                    skipped_count += 1
                                    continue

                                # Skip duplicates (already in DB or duplicated within this batch)
                                if asset_id in existing_ids_in_batch or asset_id in seen_created_ids_in_batch:
                                    skipped_count += 1
                                    continue
                                seen_created_ids_in_batch.add(asset_id)

                                asset = Asset(
                                    id=asset_data['id'],
                                    name=asset_data['name'],
                                    type=asset_data['type'],
                                    catalog=asset_data['catalog'],
                                    connector_id=asset_data['connector_id'],
                                    technical_metadata=asset_data['technical_metadata'],
                                    operational_metadata=asset_data['operational_metadata'],
                                    business_metadata=asset_data['business_metadata'],
                                    columns=asset_data['columns']
                                )
                                db.add(asset)
                                # OPTIMIZED: Removed individual db.flush() - let SQLAlchemy batch inserts
                            except Exception as flush_error:

                                error_str = str(flush_error)
                                if "Duplicate entry" in error_str or "1062" in error_str or "UNIQUE constraint" in error_str or "IntegrityError" in error_str:

                                    db.rollback()

                                    existing_asset = db.query(Asset).filter(Asset.id == asset_data['id']).first()
                                    if existing_asset:
                                        logger.debug('FN:discover_assets asset_id:{} message:Asset already exists (race condition), skipping duplicate creation'.format(asset_data['id']))
                                        skipped_count += 1
                                        continue
                                    else:

                                        try:
                                            asset = Asset(
                                                id=asset_data['id'],
                                                name=asset_data['name'],
                                                type=asset_data['type'],
                                                catalog=asset_data['catalog'],
                                                connector_id=asset_data['connector_id'],
                                                technical_metadata=asset_data['technical_metadata'],
                                                operational_metadata=asset_data['operational_metadata'],
                                                business_metadata=asset_data['business_metadata'],
                                                columns=asset_data['columns']
                                            )
                                            db.add(asset)
                                            # OPTIMIZED: Removed individual db.flush() here too
                                        except Exception as retry_error:
                                            logger.warning('FN:discover_assets asset_id:{} message:Retry failed, skipping error:{}'.format(asset_data['id'], str(retry_error)))
                                            skipped_count += 1
                                            continue
                                else:

                                    raise
                            


                            tech_meta = asset_data.get('technical_metadata', {})
                            item_config = item.get("config_data", {})
                            item_connection_id = item.get("connection_id")
                            item_connection_name = item.get("connection_name", connection.name)
                            
                            storage_location = {
                                "type": "azure_blob",
                                "path": item.get("blob_path", tech_meta.get("location", "")),
                                "connection": {
                                    "method": "connection_string" if item_config.get("connection_string") else "service_principal",
                                    "account_name": item_config.get("account_name", config_data.get("account_name", "unknown"))
                                },
                                "container": {
                                    "name": item.get("container", ""),
                                    "type": "blob_container"
                                }
                            }
                            

                            file_metadata = {
                                "basic": {
                                    "name": asset_data['name'],
                                    "size_bytes": tech_meta.get("size_bytes", tech_meta.get("size", 0)),
                                    "format": tech_meta.get("format", asset_data['type'])
                                },
                                "hash": {
                                    "value": tech_meta.get("file_hash", ""),
                                    "algorithm": "md5"
                                },
                                "timestamps": {
                                    "last_modified": tech_meta.get("last_modified", datetime.utcnow().isoformat()),
                                    "created": tech_meta.get("created_at", datetime.utcnow().isoformat())
                                }
                            }
                            

                            schema_hash = tech_meta.get("schema_hash", "")
                            


                            schema_json_full = asset_data.get('schema_json', {})
                            if not schema_json_full or not isinstance(schema_json_full, dict):

                                columns = asset_data.get('columns', [])
                                schema_json_full = {
                                    "columns": columns,
                                    "num_columns": len(columns),
                                    "delimiter": None,
                                    "has_header": None,
                                    "num_rows": None,
                                    "sample_rows_count": None
                                }
                            
                            # OPTIMIZED: Collect discovery data for bulk insert instead of individual adds
                            discovery_data = {
                                "storage_location": storage_location,
                                "file_metadata": file_metadata,
                                "schema_json": schema_json_full,
                                "schema_hash": schema_hash,
                                "status": "pending",
                                "approval_status": None,
                                "discovered_at": datetime.utcnow(),
                                "folder_path": item.get("folder", ""),
                                "data_source_type": "azure_blob_storage",
                                "environment": item_config.get("environment", config_data.get("environment", "production")),
                                "discovery_info": {
                                    "connection_id": item_connection_id if item_connection_id else connection_id,
                                    "connection_name": item_connection_name,
                                    "container": item.get("container", ""),
                                    "discovered_by": "api_discovery"
                                },
                                "asset_id": None  # Will be set after flush
                                }
                            discoveries_to_add.append((asset, discovery_data))
                            created_count += 1
                    except Exception as e:
                        logger.error('FN:discover_assets message:Error processing asset error:{}'.format(str(e)), exc_info=True)
                        skipped_count += 1
                        continue
                
                # OPTIMIZED: Flush to get asset IDs, then bulk insert discoveries
                if discoveries_to_add:
                    try:
                        db.flush()  # Get IDs for assets
                        # Now update discovery_data with asset_id and bulk insert
                        discovery_mappings = []
                        for asset, discovery_data in discoveries_to_add:
                            discovery_data["asset_id"] = asset.id
                            discovery_mappings.append(discovery_data)
                        
                        if discovery_mappings:
                            db.bulk_insert_mappings(DataDiscovery, discovery_mappings)
                            logger.debug('FN:discover_assets batch_number:{} message:Bulk inserted {} discovery records'.format(batch_num, len(discovery_mappings)))
                    except Exception as e:
                        logger.error('FN:discover_assets message:Error flushing assets or bulk inserting discoveries error:{}'.format(str(e)), exc_info=True)
                        db.rollback()
                        # Fallback to individual inserts if bulk insert fails
                        for asset, discovery_data in discoveries_to_add:
                            try:
                                discovery = DataDiscovery(
                                    asset_id=asset.id,
                                    storage_location=discovery_data["storage_location"],
                                    file_metadata=discovery_data["file_metadata"],
                                    schema_json=discovery_data["schema_json"],
                                    schema_hash=discovery_data["schema_hash"],
                                    status=discovery_data["status"],
                                    approval_status=discovery_data["approval_status"],
                                    discovered_at=discovery_data["discovered_at"],
                                    folder_path=discovery_data["folder_path"],
                                    data_source_type=discovery_data["data_source_type"],
                                    environment=discovery_data["environment"],
                                    discovery_info=discovery_data["discovery_info"]
                                )
                                db.add(discovery)
                            except Exception as fallback_error:
                                logger.warning('FN:discover_assets message:Fallback discovery insert failed error:{}'.format(str(fallback_error)))
                

                # OPTIMIZED: Always commit in batches (not just for >5000 assets)
                if len(batch) > 0:
                    try:
                        db.commit()
                        logger.debug('FN:discover_assets batch_number:{} message:Committed batch'.format(batch_num))
                    except Exception as e:
                        logger.error('FN:discover_assets message:Error committing batch error:{}'.format(str(e)), exc_info=True)
                        try:
                            db.rollback()
                        except Exception as rollback_error:
                            logger.error('FN:discover_assets message:Error during batch rollback error:{}'.format(str(rollback_error)))
                            db.close()
                            db = SessionLocal()
                        

                        error_str = str(e).lower()
                        if 'timeout' in error_str or 'lost connection' in error_str or 'operationalerror' in error_str:
                            logger.warning('FN:discover_assets message:Database connection timeout during batch commit, continuing with next batch')

                            continue
                        raise
            
            logger.info('FN:discover_assets created_count:{} updated_count:{} message:Committing to database'.format(created_count, updated_count))
            try:
                db.commit()
                logger.info('FN:discover_assets total_committed:{} message:Successfully committed assets to database'.format(created_count + updated_count))
            except Exception as e:
                logger.error('FN:discover_assets message:Error committing assets to database error:{}'.format(str(e)), exc_info=True)
                try:
                    db.rollback()
                except Exception as rollback_error:
                    logger.error('FN:discover_assets message:Error during rollback error:{}'.format(str(rollback_error)))
                    db.close()
                    db = SessionLocal()
                

                error_str = str(e).lower()
                if 'timeout' in error_str or 'lost connection' in error_str or 'operationalerror' in error_str:

                    logger.warning('FN:discover_assets message:Database connection timeout, returning partial results')
                    return jsonify({
                        "message": "Discovery completed but database commit failed due to connection timeout. Some assets may not be saved.",
                        "created_count": created_count,
                        "updated_count": updated_count,
                        "skipped_count": skipped_count,
                        "error": "Database connection timeout during commit",
                        "partial_success": True
                    }), 207
                raise
            
            total_processed = created_count + updated_count
            
            logger.info('FN:discover_assets total_processed:{} created_count:{} updated_count:{} skipped_count:{} message:Discovery summary'.format(total_processed, created_count, updated_count, skipped_count))
            



            folder_structure = {}
            for container_name in containers:
                folder_structure[container_name] = {}
                for asset_info in discovered_assets:
                    if asset_info.get("container") == container_name:
                        folder = asset_info.get("folder", "")
                        if folder not in folder_structure[container_name]:
                            folder_structure[container_name][folder] = []
                        folder_structure[container_name][folder].append({
                            "name": asset_info.get("name", "unknown"),
                            "action": asset_info.get("action", "created")
                        })
            

            has_folders = {}
            for container_name in containers:
                folders = folders_found.get(container_name, [])

                has_folders[container_name] = any(f for f in folders if f != "")
            

            file_shares_discovered = 0
            try:
                file_shares = blob_client.list_file_shares()
                logger.info('FN:discover_assets file_shares_count:{}'.format(len(file_shares)))
                
                for share in file_shares:
                    share_name = share["name"]
                    try:

                        share_files = blob_client.list_file_share_files(share_name=share_name, directory_path=folder_path)
                        
                        for file_info in share_files:
                            try:
                                file_path = file_info.get("full_path", file_info.get("name", ""))
                                file_extension = file_info.get("name", "").split(".")[-1].lower() if "." in file_info.get("name", "") else ""
                                connector_id = f"azure_blob_{connection.name}"
                                

                                existing_asset = check_asset_exists(db, connector_id, f"file-share://{share_name}/{file_path}") if AZURE_AVAILABLE else None
                                

                                storage_path_for_check = f"file-share://{share_name}/{file_path}"

                                normalized_path = file_path.strip('/').replace('/', '_').replace(' ', '_')
                                asset_id = f"azure_file_{connection.name}_{share_name}_{normalized_path}"
                                
                                asset_data = {
                                    "id": asset_id,
                                    "name": file_info.get("name", "unknown"),
                                    "type": "file",
                                    "catalog": "azure_file_share",
                                    "connector_id": connector_id,
                                    "columns": [],
                                    "business_metadata": build_business_metadata(file_info, {}, file_extension, share_name),
                                    "technical_metadata": {
                                        "location": storage_path_for_check,
                                        "file_size": file_info.get("size", 0),
                                        "content_type": file_info.get("content_type", "application/octet-stream"),
                                        "last_modified": file_info.get("last_modified"),
                                        "file_attributes": file_info.get("file_attributes"),
                                        "service_type": "azure_file_share",
                                        "share_name": share_name,
                                        "file_path": file_path
                                    }
                                }
                                
                                if existing_asset:

                                    existing_asset.business_metadata = asset_data["business_metadata"]
                                    existing_asset.technical_metadata = asset_data["technical_metadata"]
                                    db.commit()
                                    updated_count += 1
                                else:

                                    asset = Asset(**asset_data)
                                    db.add(asset)
                                    db.flush()
                                    
                                    discovery = DataDiscovery(
                                        asset_id=asset.id,
                                        storage_location={"type": "azure_file_share", "path": storage_path_for_check},
                                        file_metadata={},
                                        schema_json=[],
                                        schema_hash="",
                                        status="pending",
                                        approval_status=None,
                                        discovered_at=datetime.utcnow(),
                                        folder_path=folder_path,
                                        data_source_type="azure_file_share",
                                        environment=config_data.get("environment", "production"),
                                        discovery_info={
                                            "connection_id": connection_id,
                                            "connection_name": connection.name,
                                            "share": share_name,
                                            "discovered_by": "api_discovery"
                                        }
                                    )
                                    db.add(discovery)
                                    created_count += 1
                                    file_shares_discovered += 1
                            except Exception as e:
                                logger.error('FN:discover_assets share_name:{} file_name:{} error:{}'.format(share_name, file_info.get("name", "unknown"), str(e)))
                                skipped_count += 1
                                continue
                    except Exception as e:
                        logger.error('FN:discover_assets share_name:{} error:{}'.format(share_name, str(e)))
                        continue
                
                if file_shares_discovered > 0:
                    db.commit()
                    logger.info('FN:discover_assets file_shares_discovered:{}'.format(file_shares_discovered))
            except Exception as e:
                logger.warning('FN:discover_assets message:File shares discovery failed error:{}'.format(str(e)))
            

            queues_discovered = 0
            try:
                queues = blob_client.list_queues()
                logger.info('FN:discover_assets queues_count:{}'.format(len(queues)))
                
                for queue in queues:
                    try:
                        queue_name = queue["name"]
                        connector_id = f"azure_blob_{connection.name}"
                        

                        existing_asset = check_asset_exists(db, connector_id, f"queue://{queue_name}") if AZURE_AVAILABLE else None
                        

                        storage_location_str = f"queue://{queue_name}"

                        asset_id = f"azure_queue_{connection.name}_{queue_name}"
                        
                        asset_data = {
                            "id": asset_id,
                            "name": queue_name,
                            "type": "queue",
                            "catalog": "azure_queue",
                            "connector_id": connector_id,
                            "columns": [],
                            "business_metadata": {
                                "description": f"Azure Queue: {queue_name}",
                                "data_type": "queue",
                                "tags": [queue_name, "azure_queue"]
                            },
                            "technical_metadata": {
                                "location": storage_location_str,
                                "service_type": "azure_queue",
                                "queue_name": queue_name,
                                "metadata": queue.get("metadata", {}),
                                "storage_location": storage_location_str
                            }
                        }
                        
                        if existing_asset:

                            existing_asset.business_metadata = asset_data["business_metadata"]
                            existing_asset.technical_metadata = asset_data["technical_metadata"]
                            db.commit()
                            updated_count += 1
                        else:

                            asset = Asset(
                                id=asset_data["id"],
                                name=asset_data["name"],
                                type=asset_data["type"],
                                catalog=asset_data["catalog"],
                                connector_id=asset_data["connector_id"],
                                columns=asset_data["columns"],
                                business_metadata=asset_data["business_metadata"],
                                technical_metadata=asset_data["technical_metadata"]
                            )
                            db.add(asset)
                            db.flush()
                            
                            storage_location = {
                                "type": "azure_queue",
                                "account_name": config_data.get("account_name", ""),
                                "queue_name": queue_name
                            }
                            
                            discovery = DataDiscovery(
                                asset_id=asset.id,
                                storage_location=storage_location,
                                file_metadata={},
                                schema_json=[],
                                schema_hash="",
                                status="pending",
                                approval_status=None,
                                discovered_at=datetime.utcnow(),
                                folder_path="",
                                data_source_type="azure_queue",
                                environment=config_data.get("environment", "production"),
                                discovery_info={
                                    "connection_id": connection_id,
                                    "connection_name": connection.name,
                                    "queue": queue_name,
                                    "discovered_by": "api_discovery"
                                }
                            )
                            db.add(discovery)
                            created_count += 1
                            queues_discovered += 1
                    except Exception as e:
                        logger.error('FN:discover_assets queue_name:{} error:{}'.format(queue.get("name", "unknown"), str(e)))
                        skipped_count += 1
                        continue
                
                if queues_discovered > 0:
                    db.commit()
                    logger.info('FN:discover_assets queues_discovered:{}'.format(queues_discovered))
            except Exception as e:
                logger.warning('FN:discover_assets message:Queues discovery failed error:{}'.format(str(e)))
            

            tables_discovered = 0
            try:
                tables = blob_client.list_tables()
                logger.info('FN:discover_assets tables_count:{}'.format(len(tables)))
                
                for table in tables:
                    try:
                        table_name = table["name"]
                        connector_id = f"azure_blob_{connection.name}"
                        

                        existing_asset = check_asset_exists(db, connector_id, f"table://{table_name}") if AZURE_AVAILABLE else None
                        

                        storage_location_str = f"table://{table_name}"

                        asset_id = f"azure_table_{connection.name}_{table_name}"
                        
                        asset_data = {
                            "id": asset_id,
                            "name": table_name,
                            "type": "table",
                            "catalog": "azure_table",
                            "connector_id": connector_id,
                            "columns": [],
                            "business_metadata": {
                                "description": f"Azure Table: {table_name}",
                                "data_type": "table",
                                "tags": [table_name, "azure_table"]
                            },
                            "technical_metadata": {
                                "service_type": "azure_table",
                                "table_name": table_name,
                                "storage_location": storage_location_str
                            }
                        }
                        
                        if existing_asset:

                            existing_asset.business_metadata = asset_data["business_metadata"]
                            existing_asset.technical_metadata = asset_data["technical_metadata"]
                            db.commit()
                            updated_count += 1
                        else:

                            asset = Asset(
                                id=asset_data["id"],
                                name=asset_data["name"],
                                type=asset_data["type"],
                                catalog=asset_data["catalog"],
                                connector_id=asset_data["connector_id"],
                                columns=asset_data["columns"],
                                business_metadata=asset_data["business_metadata"],
                                technical_metadata=asset_data["technical_metadata"]
                            )
                            db.add(asset)
                            db.flush()
                            
                            storage_location = {
                                "type": "azure_table",
                                "account_name": config_data.get("account_name", ""),
                                "table_name": table_name
                            }
                            
                            discovery = DataDiscovery(
                                asset_id=asset.id,
                                storage_location=storage_location,
                                file_metadata={},
                                schema_json=[],
                                schema_hash="",
                                status="pending",
                                approval_status=None,
                                discovered_at=datetime.utcnow(),
                                folder_path="",
                                data_source_type="azure_table",
                                environment=config_data.get("environment", "production"),
                                discovery_info={
                                    "connection_id": connection_id,
                                    "connection_name": connection.name,
                                    "table": table_name,
                                    "discovered_by": "api_discovery"
                                }
                            )
                            db.add(discovery)
                            created_count += 1
                            tables_discovered += 1
                    except Exception as e:
                        logger.error('FN:discover_assets table_name:{} error:{}'.format(table.get("name", "unknown"), str(e)))
                        skipped_count += 1
                        continue
                
                if tables_discovered > 0:
                    db.commit()
                    logger.info('FN:discover_assets tables_discovered:{}'.format(tables_discovered))
            except Exception as e:
                logger.warning('FN:discover_assets message:Tables discovery failed error:{}'.format(str(e)))
            

            total_processed = created_count + updated_count
            
            return jsonify({
                "success": True,
                "message": f"Discovery complete: {created_count} new, {updated_count} updated, {skipped_count} skipped",
                "discovered_count": total_processed,
                "created_count": created_count,
                "updated_count": updated_count,
                "skipped_count": skipped_count,
                "folders": folders_found,
                "assets_by_folder": folder_structure,
                "has_folders": has_folders,
                "services_discovered": {
                    "containers": len(containers),
                    "file_shares": file_shares_discovered,
                    "queues": queues_discovered,
                    "tables": tables_discovered
                }
            }), 201
        except Exception as e:
            db.rollback()
            logger.error('FN:discover_assets error:{}'.format(str(e)), exc_info=True)
            return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/assets/<asset_id>/approve', methods=['POST'])
@handle_error
def approve_asset(asset_id):
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        
        if not asset.operational_metadata:
            asset.operational_metadata = {}
        
        approval_time = datetime.utcnow()
        asset.operational_metadata["approval_status"] = "approved"
        asset.operational_metadata["approved_at"] = approval_time.isoformat()
        asset.operational_metadata["approved_by"] = "user"
        

        flag_modified(asset, "operational_metadata")
        

        discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
        if discovery:
            discovery.approval_status = "approved"
            discovery.status = "approved"
            if not discovery.approval_workflow:
                discovery.approval_workflow = {}
            discovery.approval_workflow["approved_at"] = approval_time.isoformat()
            discovery.approval_workflow["approved_by"] = "user"
            flag_modified(discovery, "approval_workflow")
        else:

            discovery = DataDiscovery(
                asset_id=asset_id,
                storage_location=asset.technical_metadata.get("storage_location", {}) if asset.technical_metadata else {},
                file_metadata=asset.technical_metadata if asset.technical_metadata else {},
                schema_json=asset.columns if asset.columns else {},
                schema_hash=asset.technical_metadata.get("schema_hash", "") if asset.technical_metadata else "",
                status="approved",
                approval_status="approved",
                approval_workflow={
                    "approved_at": approval_time.isoformat(),
                    "approved_by": "user"
                },
                discovered_at=asset.discovered_at if asset.discovered_at else approval_time
            )
            db.add(discovery)
        
        db.commit()
        db.refresh(asset)
        db.refresh(discovery)
        

        logger.info('FN:approve_asset asset_id:{} approval_status:{} saved_to_db:True'.format(
            asset_id, asset.operational_metadata.get("approval_status")
        ))
        

        return jsonify({
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "technical_metadata": asset.technical_metadata,
            "operational_metadata": asset.operational_metadata,
            "business_metadata": asset.business_metadata,
            "columns": asset.columns,
            "approval_status": "approved",
            "discovery_id": discovery.id,
            "updated_at": approval_time.isoformat()
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:approve_asset asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/assets/<asset_id>/reject', methods=['POST'])
@handle_error
def reject_asset(asset_id):
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        
        if not asset.operational_metadata:
            asset.operational_metadata = {}
        
        data = request.json or {}
        reason = data.get('reason', 'No reason provided')
        
        rejection_time = datetime.utcnow()
        asset.operational_metadata["approval_status"] = "rejected"
        asset.operational_metadata["rejected_at"] = rejection_time.isoformat()
        asset.operational_metadata["rejected_by"] = "user"
        asset.operational_metadata["rejection_reason"] = reason
        

        flag_modified(asset, "operational_metadata")
        

        discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
        if discovery:
            discovery.approval_status = "rejected"
            discovery.status = "rejected"
            if not discovery.approval_workflow:
                discovery.approval_workflow = {}
            discovery.approval_workflow["rejected_at"] = rejection_time.isoformat()
            discovery.approval_workflow["rejected_by"] = "user"
            discovery.approval_workflow["rejection_reason"] = reason
            flag_modified(discovery, "approval_workflow")
        else:

            discovery = DataDiscovery(
                asset_id=asset_id,
                storage_location=asset.technical_metadata.get("storage_location", {}) if asset.technical_metadata else {},
                file_metadata=asset.technical_metadata if asset.technical_metadata else {},
                schema_json=asset.columns if asset.columns else {},
                schema_hash=asset.technical_metadata.get("schema_hash", "") if asset.technical_metadata else "",
                status="rejected",
                approval_status="rejected",
                approval_workflow={
                    "rejected_at": rejection_time.isoformat(),
                    "rejected_by": "user",
                    "rejection_reason": reason
                },
                discovered_at=asset.discovered_at if asset.discovered_at else rejection_time
            )
            db.add(discovery)
        
        db.commit()
        db.refresh(asset)
        db.refresh(discovery)
        

        logger.info('FN:reject_asset asset_id:{} approval_status:{} saved_to_db:True'.format(
            asset_id, asset.operational_metadata.get("approval_status")
        ))
        
        return jsonify({
            "id": asset.id,
            "name": asset.name,
            "approval_status": "rejected",
            "discovery_id": discovery.id,
            "rejection_reason": reason,
            "updated_at": rejection_time.isoformat()
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:reject_asset asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/assets/<asset_id>/publish', methods=['POST'])
@handle_error
def publish_asset(asset_id):
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        

        approval_status = asset.operational_metadata.get("approval_status") if asset.operational_metadata else None
        if approval_status != "approved":
            return jsonify({"error": f"Asset must be approved before publishing. Current status: {approval_status}"}), 400
        
        if not asset.operational_metadata:
            asset.operational_metadata = {}
        
        publish_time = datetime.utcnow()
        data = request.json or {}
        published_to = data.get('published_to', 'catalog')
        
        asset.operational_metadata["publish_status"] = "published"
        asset.operational_metadata["published_at"] = publish_time.isoformat()
        asset.operational_metadata["published_by"] = "user"
        asset.operational_metadata["published_to"] = published_to
        

        flag_modified(asset, "operational_metadata")
        

        discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
        if discovery:
            discovery.status = "published"
            discovery.published_at = publish_time
            discovery.published_to = published_to
        else:

            discovery = DataDiscovery(
                asset_id=asset_id,
                storage_location=asset.technical_metadata.get("storage_location", {}) if asset.technical_metadata else {},
                file_metadata=asset.technical_metadata if asset.technical_metadata else {},
                schema_json=asset.columns if asset.columns else {},
                schema_hash=asset.technical_metadata.get("schema_hash", "") if asset.technical_metadata else "",
                status="published",
                approval_status="approved",
                published_at=publish_time,
                published_to=published_to,
                discovered_at=asset.discovered_at if asset.discovered_at else publish_time
            )
            db.add(discovery)
        
        db.commit()
        db.refresh(asset)
        db.refresh(discovery)
        
        return jsonify({
            "id": asset.id,
            "name": asset.name,
            "status": "published",
            "discovery_id": discovery.id,
            "published_to": published_to,
            "published_at": publish_time.isoformat()
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:publish_asset asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()





@app.route('/api/lineage/relationships', methods=['GET'])
@handle_error
def get_lineage_relationships():
    db = SessionLocal()
    try:


        include_inferred = request.args.get('include_inferred', 'false').lower() == 'true'
        
        if include_inferred:

            relationships = db.query(LineageRelationship).all()
        else:

            exclude_methods = ['column_matching', 'ml_inference', 'inferred']
            relationships = db.query(LineageRelationship).filter(
                ~LineageRelationship.extraction_method.in_(exclude_methods)
            ).all()
            

            relationships = [rel for rel in relationships 
                           if rel.extraction_method and rel.extraction_method not in exclude_methods]
        
        result = []
        
        for rel in relationships:

            source_asset = db.query(Asset).filter(Asset.id == rel.source_asset_id).first()
            target_asset = db.query(Asset).filter(Asset.id == rel.target_asset_id).first()
            

            source_quality = None
            target_quality = None
            if source_asset:
                source_asset_dict = {
                    'id': source_asset.id,
                    'columns': source_asset.columns,
                    'last_modified': source_asset.discovered_at.isoformat() if source_asset.discovered_at else None
                }
                source_quality = calculate_asset_quality_score(source_asset_dict)
            
            if target_asset:
                target_asset_dict = {
                    'id': target_asset.id,
                    'columns': target_asset.columns,
                    'last_modified': target_asset.discovered_at.isoformat() if target_asset.discovered_at else None
                }
                target_quality = calculate_asset_quality_score(target_asset_dict)
            

            column_lineage = rel.column_lineage or []
            end_to_end_lineage = []
            

            for col_rel in column_lineage:
                enhanced_col = {
                    **col_rel,
                    'source_quality': source_quality.get('quality_score') if source_quality else None,
                    'target_quality': target_quality.get('quality_score') if target_quality else None
                }
                end_to_end_lineage.append(enhanced_col)
            
            rel_data = {
                "id": rel.id,
                "source_asset_id": rel.source_asset_id,
                "target_asset_id": rel.target_asset_id,
                "relationship_type": rel.relationship_type,
                "source_type": rel.source_type,
                "target_type": rel.target_type,
                "column_lineage": end_to_end_lineage,
                "transformation_type": rel.transformation_type,
                "transformation_description": rel.transformation_description,
                "source_system": rel.source_system,
                "source_job_id": rel.source_job_id,
                "source_job_name": rel.source_job_name,
                "confidence_score": float(rel.confidence_score) if rel.confidence_score else None,
                "extraction_method": rel.extraction_method,
                "created_at": rel.created_at.isoformat() if rel.created_at else None,
                "discovered_at": rel.discovered_at.isoformat() if rel.discovered_at else None,
                "source_quality": source_quality,
                "target_quality": target_quality
            }
            

            if source_quality and target_quality:
                relationship_dict = {
                    'transformation_type': rel.transformation_type or 'pass_through'
                }
                quality_propagation = propagate_quality_through_lineage(
                    source_quality,
                    target_quality,
                    relationship_dict
                )
                rel_data['quality_propagation'] = quality_propagation
            
            result.append(rel_data)
        
        return jsonify(result), 200
    finally:
        db.close()

@app.route('/api/lineage/relationships', methods=['POST'])
@handle_error
def create_lineage_relationship():
    db = SessionLocal()
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        

        if not data.get('source_asset_id') or not data.get('target_asset_id'):
            return jsonify({"error": "source_asset_id and target_asset_id are required"}), 400
        

        source_asset = db.query(Asset).filter(Asset.id == data['source_asset_id']).first()
        target_asset = db.query(Asset).filter(Asset.id == data['target_asset_id']).first()
        
        if not source_asset:
            return jsonify({"error": f"Source asset {data['source_asset_id']} not found"}), 404
        if not target_asset:
            return jsonify({"error": f"Target asset {data['target_asset_id']} not found"}), 404
        

        relationship = LineageRelationship(
            source_asset_id=data['source_asset_id'],
            target_asset_id=data['target_asset_id'],
            relationship_type=data.get('relationship_type', 'transformation'),
            source_type=data.get('source_type', source_asset.type),
            target_type=data.get('target_type', target_asset.type),
            column_lineage=data.get('column_lineage', []),
            transformation_type=data.get('transformation_type'),
            transformation_description=data.get('transformation_description'),
            source_system=data.get('source_system', 'manual'),
            source_job_id=data.get('source_job_id'),
            source_job_name=data.get('source_job_name'),
            confidence_score=data.get('confidence_score', 1.0),
            extraction_method=data.get('extraction_method', 'manual'),
            discovered_at=datetime.utcnow()
        )
        
        db.add(relationship)
        db.commit()
        db.refresh(relationship)
        
        logger.info('FN:create_lineage_relationship relationship_id:{} source:{} target:{}'.format(
            relationship.id, data['source_asset_id'], data['target_asset_id']
        ))
        
        return jsonify({
            "id": relationship.id,
            "source_asset_id": relationship.source_asset_id,
            "target_asset_id": relationship.target_asset_id,
            "relationship_type": relationship.relationship_type,
            "column_lineage": relationship.column_lineage,
            "confidence_score": float(relationship.confidence_score) if relationship.confidence_score else None
        }), 201
    except Exception as e:
        db.rollback()
        logger.error('FN:create_lineage_relationship error:{}'.format(str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create lineage relationship"}), 400
    finally:
        db.close()

@app.route('/api/lineage/sql/parse', methods=['POST'])
@handle_error
def parse_sql_lineage():
    try:
        data = request.json
        if not data or not data.get('sql_query'):
            return jsonify({"error": "sql_query is required"}), 400
        
        sql_query = data['sql_query']
        dialect = data.get('dialect', 'mysql')
        source_system = data.get('source_system', 'unknown')
        job_id = data.get('job_id')
        job_name = data.get('job_name')
        

        lineage_result = extract_lineage_from_sql(sql_query, dialect)
        

        db = SessionLocal()
        sql_record = None
        try:
            sql_record = SQLQuery(
                query_text=sql_query,
                query_type=lineage_result.get('query_type'),
                source_system=source_system,
                job_id=job_id,
                job_name=job_name,
                parsed_lineage=lineage_result,
                parse_status='parsed'
            )
            db.add(sql_record)
            db.commit()
            db.refresh(sql_record)
            
            logger.info('FN:parse_sql_lineage sql_query_id:{} query_type:{} source_tables_count:{}'.format(
                sql_record.id, lineage_result.get('query_type'), len(lineage_result.get('source_tables', []))
            ))
        except Exception as e:
            logger.error('FN:parse_sql_lineage db_error:{}'.format(str(e)))
        finally:
            db.close()
        
        return jsonify({
            "lineage": lineage_result,
            "sql_query_id": sql_record.id if sql_record else None
        }), 200
    except Exception as e:
        logger.error('FN:parse_sql_lineage error:{}'.format(str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to parse SQL lineage"}), 400

@app.route('/api/lineage/infer', methods=['POST'])
@handle_error
def infer_lineage_relationships():
    db = SessionLocal()
    try:
        data = request.json or {}
        min_confidence = float(data.get('min_confidence', 0.5))
        min_matching_columns = int(data.get('min_matching_columns', 2))
        

        assets = db.query(Asset).filter(Asset.columns.isnot(None)).all()
        
        created_relationships = []
        skipped_relationships = []
        

        for i, source_asset in enumerate(assets):
            source_columns = source_asset.columns or []
            if not source_columns or len(source_columns) == 0:
                continue
                
            source_col_names = {col.get('name', '').lower() for col in source_columns if col.get('name')}
            
            for target_asset in assets[i+1:]:
                target_columns = target_asset.columns or []
                if not target_columns or len(target_columns) == 0:
                    continue
                    
                target_col_names = {col.get('name', '').lower() for col in target_columns if col.get('name')}
                

                column_lineage, confidence = infer_relationships_ml(
                    source_columns,
                    target_columns,
                    min_matching_ratio=min_confidence
                )
                
                if len(column_lineage) < min_matching_columns:
                    continue
                

                existing = db.query(LineageRelationship).filter(
                    LineageRelationship.source_asset_id == source_asset.id,
                    LineageRelationship.target_asset_id == target_asset.id
                ).first()
                
                if existing:
                    skipped_relationships.append({
                        'source': source_asset.id,
                        'target': target_asset.id,
                        'reason': 'already_exists'
                    })
                    continue
                
                if confidence < min_confidence:
                    continue
                

                relationship = LineageRelationship(
                    source_asset_id=source_asset.id,
                    target_asset_id=target_asset.id,
                    relationship_type='inferred',
                    source_type=source_asset.type,
                    target_type=target_asset.type,
                    column_lineage=column_lineage,
                    transformation_type='pass_through',
                    transformation_description=f'Inferred from {len(column_lineage)} matching columns using ML',
                    source_system=source_asset.connector_id.split('_')[0] if source_asset.connector_id else 'unknown',
                    confidence_score=confidence,
                    extraction_method='ml_inference',
                    discovered_at=datetime.utcnow()
                )
                
                db.add(relationship)
                created_relationships.append({
                    'source_asset_id': source_asset.id,
                    'target_asset_id': target_asset.id,
                    'matching_columns': len(column_lineage),
                    'confidence': confidence
                })
        
        db.commit()
        
        logger.info('FN:infer_lineage_relationships created:{} skipped:{}'.format(
            len(created_relationships), len(skipped_relationships)
        ))
        
        return jsonify({
            'created': len(created_relationships),
            'skipped': len(skipped_relationships),
            'relationships': created_relationships
        }), 200
        
    except Exception as e:
        db.rollback()
        logger.error('FN:infer_lineage_relationships error:{}'.format(str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to infer lineage relationships"}), 400
    finally:
        db.close()

@app.route('/api/lineage/sql/parse-and-create', methods=['POST'])
@handle_error
def parse_sql_and_create_lineage():
    db = SessionLocal()
    try:
        data = request.json
        if not data or not data.get('sql_query'):
            return jsonify({"error": "sql_query is required"}), 400
        
        sql_query = data['sql_query']
        dialect = data.get('dialect', 'mysql')
        source_system = data.get('source_system', 'airflow')
        job_id = data.get('job_id')
        job_name = data.get('job_name')
        

        lineage_result = extract_lineage_from_sql(sql_query, dialect)
        
        if not lineage_result.get('target_table') or not lineage_result.get('source_tables'):
            return jsonify({
                "lineage": lineage_result,
                "relationships_created": 0,
                "message": "No target or source tables found in query"
            }), 200
        

        sql_record = SQLQuery(
            query_text=sql_query,
            query_type=lineage_result.get('query_type'),
            source_system=source_system,
            job_id=job_id,
            job_name=job_name,
            parsed_lineage=lineage_result,
            parse_status='parsed'
        )
        db.add(sql_record)
        db.commit()
        db.refresh(sql_record)
        

        target_table = lineage_result['target_table']
        target_asset = db.query(Asset).filter(
            Asset.name.ilike(f'%{target_table}%')
        ).first()
        
        if not target_asset:
            return jsonify({
                "lineage": lineage_result,
                "sql_query_id": sql_record.id,
                "relationships_created": 0,
                "message": f"Target asset '{target_table}' not found"
            }), 200
        

        created_count = 0
        for source_table in lineage_result.get('source_tables', []):
            source_asset = db.query(Asset).filter(
                Asset.name.ilike(f'%{source_table}%')
            ).first()
            
            if not source_asset:
                continue
            

            existing = db.query(LineageRelationship).filter(
                LineageRelationship.source_asset_id == source_asset.id,
                LineageRelationship.target_asset_id == target_asset.id
            ).first()
            
            if existing:
                continue
            

            relationship = LineageRelationship(
                source_asset_id=source_asset.id,
                target_asset_id=target_asset.id,
                relationship_type='transformation',
                source_type=source_asset.type,
                target_type=target_asset.type,
                column_lineage=lineage_result.get('column_lineage', []),
                transformation_type=lineage_result.get('query_type', 'SELECT'),
                transformation_description=f'Extracted from SQL query in {job_name or source_system}',
                source_system=source_system,
                source_job_id=job_id,
                source_job_name=job_name,
                confidence_score=lineage_result.get('confidence_score', 0.8),
                extraction_method='sql_parsing',
                discovered_at=datetime.utcnow()
            )
            
            db.add(relationship)
            created_count += 1
        
        db.commit()
        
        logger.info('FN:parse_sql_and_create_lineage sql_query_id:{} relationships_created:{}'.format(
            sql_record.id, created_count
        ))
        
        return jsonify({
            "lineage": lineage_result,
            "sql_query_id": sql_record.id,
            "relationships_created": created_count
        }), 200
        
    except Exception as e:
        db.rollback()
        logger.error('FN:parse_sql_and_create_lineage error:{}'.format(str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to parse SQL and create lineage"}), 400
    finally:
        db.close()

@app.route('/api/lineage/impact/<asset_id>', methods=['GET'])
@handle_error
def get_impact_analysis(asset_id):
    db = SessionLocal()
    try:

        downstream = db.query(LineageRelationship).filter(
            LineageRelationship.source_asset_id == asset_id
        ).all()
        

        upstream = db.query(LineageRelationship).filter(
            LineageRelationship.target_asset_id == asset_id
        ).all()
        

        impacted_assets = set()
        impact_paths = []
        

        for rel in downstream:
            impacted_assets.add(rel.target_asset_id)
            impact_paths.append({
                "path": [asset_id, rel.target_asset_id],
                "relationship": {
                    "id": rel.id,
                    "type": rel.relationship_type,
                    "columns": rel.column_lineage or []
                },
                "depth": 1
            })
        

        for rel in downstream:
            second_level = db.query(LineageRelationship).filter(
                LineageRelationship.source_asset_id == rel.target_asset_id
            ).all()
            for rel2 in second_level:
                impacted_assets.add(rel2.target_asset_id)
                impact_paths.append({
                    "path": [asset_id, rel.target_asset_id, rel2.target_asset_id],
                    "relationship": {
                        "id": rel2.id,
                        "type": rel2.relationship_type,
                        "columns": rel2.column_lineage or []
                    },
                    "depth": 2
                })
        

        impacted_asset_details = []
        for asset_id_str in impacted_assets:
            asset = db.query(Asset).filter(Asset.id == asset_id_str).first()
            if asset:
                impacted_asset_details.append({
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog
                })
        

        dependency_assets = []
        for rel in upstream:
            asset = db.query(Asset).filter(Asset.id == rel.source_asset_id).first()
            if asset:
                dependency_assets.append({
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog,
                    "relationship": {
                        "id": rel.id,
                        "type": rel.relationship_type,
                        "columns": rel.column_lineage or []
                    }
                })
        
        result = {
            "asset_id": asset_id,
            "impact_summary": {
                "direct_impact": len(downstream),
                "total_impacted_assets": len(impacted_assets),
                "total_dependencies": len(upstream),
                "impact_depth": 2
            },
            "impacted_assets": impacted_asset_details,
            "impact_paths": impact_paths,
            "dependencies": dependency_assets
        }
        
        logger.info('FN:get_impact_analysis asset_id:{} impacted_count:{} dependencies_count:{}'.format(
            asset_id, len(impacted_assets), len(upstream)
        ))
        
        return jsonify(result), 200
    except Exception as e:
        logger.error('FN:get_impact_analysis asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to get impact analysis"}), 400
    finally:
        db.close()

@app.route('/api/lineage/asset/<asset_id>', methods=['GET'])
@handle_error
def get_asset_lineage(asset_id):
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        

        upstream_rels = db.query(LineageRelationship).filter(
            LineageRelationship.target_asset_id == asset_id
        ).all()
        

        downstream_rels = db.query(LineageRelationship).filter(
            LineageRelationship.source_asset_id == asset_id
        ).all()
        

        nodes = [{
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "is_selected": True
        }]
        
        edges = []
        node_ids = {asset.id}
        

        for rel in upstream_rels:
            source_asset = db.query(Asset).filter(Asset.id == rel.source_asset_id).first()
            if source_asset and source_asset.id not in node_ids:
                nodes.append({
                    "id": source_asset.id,
                    "name": source_asset.name,
                    "type": source_asset.type,
                    "catalog": source_asset.catalog,
                    "is_selected": False
                })
                node_ids.add(source_asset.id)
            
            edges.append({
                "id": f"{rel.source_asset_id}-{rel.target_asset_id}",
                "source": rel.source_asset_id,
                "target": rel.target_asset_id,
                "type": rel.relationship_type,
                "column_lineage": rel.column_lineage or [],
                "confidence_score": float(rel.confidence_score) if rel.confidence_score else None
            })
        

        for rel in downstream_rels:
            target_asset = db.query(Asset).filter(Asset.id == rel.target_asset_id).first()
            if target_asset and target_asset.id not in node_ids:
                nodes.append({
                    "id": target_asset.id,
                    "name": target_asset.name,
                    "type": target_asset.type,
                    "catalog": target_asset.catalog,
                    "is_selected": False
                })
                node_ids.add(target_asset.id)
            
            edges.append({
                "id": f"{rel.source_asset_id}-{rel.target_asset_id}",
                "source": rel.source_asset_id,
                "target": rel.target_asset_id,
                "type": rel.relationship_type,
                "column_lineage": rel.column_lineage or [],
                "confidence_score": float(rel.confidence_score) if rel.confidence_score else None
            })
        
        return jsonify({
            "asset": {
                "id": asset.id,
                "name": asset.name,
                "type": asset.type,
                "catalog": asset.catalog
            },
            "lineage": {
                "nodes": nodes,
                "edges": edges,
                "upstream_count": len(upstream_rels),
                "downstream_count": len(downstream_rels)
            }
        }), 200
    except Exception as e:
        logger.error('FN:get_asset_lineage asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to get asset lineage"}), 400
    finally:
        db.close()

@app.errorhandler(500)
def internal_error(error):
    
    logger.error('FN:internal_error error:{}'.format(str(error)), exc_info=True)
    if app.config.get("DEBUG"):
        return jsonify({"error": str(error)}), 500
    else:
        return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == '__main__':
    host = app.config.get("FLASK_HOST", "0.0.0.0")
    port = app.config.get("FLASK_PORT", 8099)
    debug = app.config.get("DEBUG", False)
    logger.info('FN:__main__ port:{} environment:{} debug:{} message:Starting Flask app'.format(port, env, debug))
    app.run(host=host, port=port, debug=debug)
