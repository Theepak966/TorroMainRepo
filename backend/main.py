from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy.orm.attributes import flag_modified
import sys
import os

# Fix imports for running directly
# Add current directory to path first
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Use absolute imports (works both as module and directly)
from database import engine, Base, SessionLocal
from models import Asset, Connection, LineageRelationship, LineageHistory, SQLQuery, DataDiscovery
from config import config
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps
from datetime import datetime, timedelta
from sqlalchemy import func
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Set up logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import Azure utilities
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
    # Try to import anyway for discovery runner
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
    Base.metadata.create_all(bind=engine)
    logger.info('FN:__init__ message:Database tables initialized successfully')
except Exception as e:
    logger.error('FN:__init__ message:Error initializing database tables error:{}'.format(str(e)))
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
    """Simple health check endpoint"""
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

        # Check if connection with the same name already exists
        existing_connection = db.query(Connection).filter(Connection.name == data['name']).first()
        if existing_connection:
            db.close()
            return jsonify({
                "error": f"A connection with the name '{data['name']}' already exists. Please use a different name or update the existing connection."
            }), 409  # 409 Conflict

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
        # Check if it's an IntegrityError (duplicate entry)
        error_str = str(e)
        if "Duplicate entry" in error_str or "1062" in error_str or "UNIQUE constraint" in error_str:
            db.close()
            return jsonify({
                "error": f"A connection with the name '{data.get('name', '')}' already exists. Please use a different name or update the existing connection."
            }), 409  # 409 Conflict
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create connection"}), 400
    finally:
        db.close()

@app.route('/api/connections/<int:connection_id>', methods=['PUT'])
@handle_error
def update_connection(connection_id):
    """Update an existing connection"""
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        # Update fields
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
    """Delete a connection and all associated assets"""
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404

        # Build connector_id pattern based on connector type
        # For azure_blob: connector_id = "azure_blob_{connection_name}"
        if connection.connector_type == 'azure_blob':
            connector_id_pattern = f"azure_blob_{connection.name}"
        else:
            connector_id_pattern = f"{connection.connector_type}_{connection.name}"
        
        # Find all assets associated with this connection
        associated_assets = db.query(Asset).filter(Asset.connector_id == connector_id_pattern).all()
        asset_ids = [asset.id for asset in associated_assets]

        # Delete lineage relationships that reference these assets (before deleting assets)
        # This prevents foreign key constraint violations
        if asset_ids:
            # LineageRelationship is already imported at the top of the file
            lineage_relationships = db.query(LineageRelationship).filter(
                (LineageRelationship.source_asset_id.in_(asset_ids)) |
                (LineageRelationship.target_asset_id.in_(asset_ids))
            ).all()
            for rel in lineage_relationships:
                db.delete(rel)
            logger.debug('FN:delete_connection connection_id:{} deleted_lineage_relationships:{}'.format(connection_id, len(lineage_relationships)))

        # Delete all associated assets
        deleted_count = len(associated_assets)
        for asset in associated_assets:
            db.delete(asset)
            logger.debug('FN:delete_connection connection_id:{} asset_id:{} asset_name:{} message:Deleting asset'.format(connection_id, asset.id, asset.name))

        # Delete the connection
        connection_name = connection.name
        db.delete(connection)
        db.commit()

        logger.info('FN:delete_connection connection_name:{} connection_id:{} deleted_assets_count:{}'.format(connection_name, connection_id, deleted_count))

        return jsonify({
            "message": "Connection and associated assets deleted successfully",
            "deleted_assets": deleted_count,
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
    """Get data quality metrics for an asset"""
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
    """Get all assets, optionally filtered by discovery_id"""
    db = SessionLocal()
    try:
        discovery_id = request.args.get('discovery_id', type=int)
        
        if discovery_id:
            # Get asset by discovery_id
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
        
        # Get all assets with their discovery_id, sorted by discovery_id
        # Join with data_discovery to sort by discovery_id
        from sqlalchemy import case
        
        # Query assets with discovery records, sorted by discovery_id
        # Use CASE to put NULL discovery_ids at the end
        assets_with_discovery = db.query(Asset, DataDiscovery).outerjoin(
            DataDiscovery, Asset.id == DataDiscovery.asset_id
        ).order_by(
            case((DataDiscovery.id.is_(None), 1), else_=0),  # Put NULLs last
            DataDiscovery.id.asc(),  # Sort by discovery_id ascending
            Asset.discovered_at.desc()  # Fallback sort by discovered_at for assets without discovery_id
        ).all()
        
        result = []
        for asset, discovery in assets_with_discovery:
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
        
        return jsonify(result)
    finally:
        db.close()

@app.route('/api/discovery/<int:discovery_id>', methods=['GET'])
@handle_error
def get_discovery_by_id(discovery_id):
    """Get discovery record and associated asset by discovery_id"""
    db = SessionLocal()
    try:
        discovery = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
        if not discovery:
            return jsonify({"error": "Discovery record not found"}), 404
        
        result = {
            "discovery_id": discovery.id,
            "asset_id": discovery.asset_id,
            "status": discovery.status,
            "approval_status": discovery.approval_status,
            "storage_location": discovery.storage_location,
            "file_metadata": discovery.file_metadata,
            "schema_json": discovery.schema_json,
            "discovered_at": discovery.discovered_at.isoformat() if discovery.discovered_at else None,
            "last_checked_at": discovery.last_checked_at.isoformat() if discovery.last_checked_at else None,
            "approval_workflow": discovery.approval_workflow,
            "published_at": discovery.published_at.isoformat() if discovery.published_at else None,
            "published_to": discovery.published_to,
            "environment": discovery.environment,
            "data_source_type": discovery.data_source_type,
            "folder_path": discovery.folder_path,
            "tags": discovery.tags,
            "discovery_info": discovery.discovery_info,
            "storage_metadata": discovery.storage_metadata,
            "storage_data_metadata": discovery.storage_data_metadata,
            "additional_metadata": discovery.additional_metadata,
            "data_quality_score": float(discovery.data_quality_score) if discovery.data_quality_score else None,
            "validation_status": discovery.validation_status,
            "validated_at": discovery.validated_at.isoformat() if discovery.validated_at else None,
            "created_at": discovery.created_at.isoformat() if discovery.created_at else None,
            "updated_at": discovery.updated_at.isoformat() if discovery.updated_at else None
        }
        
        # Get associated asset if available
        if discovery.asset_id:
            asset = db.query(Asset).filter(Asset.id == discovery.asset_id).first()
            if asset:
                result["asset"] = {
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
        
        return jsonify(result), 200
    except Exception as e:
        logger.error('FN:get_discovery_by_id discovery_id:{} error:{}'.format(discovery_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

@app.route('/api/discovery', methods=['GET'])
@handle_error
def list_discoveries():
    """List all discovery records with optional filtering"""
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
            
            # Get associated asset if available
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
    """Approve a discovery record by discovery_id"""
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
        
        # Also update associated asset if available
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
    """Reject a discovery record by discovery_id"""
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
        
        # Also update associated asset if available
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
    """Get discovery statistics"""
    db = SessionLocal()
    try:
        total = db.query(DataDiscovery).count()
        by_status = {}
        by_approval_status = {}
        
        # Count by status
        statuses = db.query(DataDiscovery.status, func.count(DataDiscovery.id)).group_by(DataDiscovery.status).all()
        for status, count in statuses:
            by_status[status or "unknown"] = count
        
        # Count by approval_status
        approval_statuses = db.query(DataDiscovery.approval_status, func.count(DataDiscovery.id)).group_by(DataDiscovery.approval_status).all()
        for approval_status, count in approval_statuses:
            by_approval_status[approval_status or "unknown"] = count
        
        # Recent discoveries (last 7 days)
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
    """Trigger Airflow DAG for discovery - can be called without connection_id to trigger all"""
    try:
        data = request.json or {}
        connection_id = data.get('connection_id')  # Optional
        
        # Trigger Airflow DAG
        airflow_triggered = False
        try:
            airflow_base_url = os.getenv("AIRFLOW_BASE_URL")
            if not airflow_base_url:
                return jsonify({
                    "error": "AIRFLOW_BASE_URL not configured",
                    "message": "Cannot trigger Airflow DAG without AIRFLOW_BASE_URL"
                }), 400
            
            dag_id = "azure_blob_discovery"
            import requests
            from requests.auth import HTTPBasicAuth
            
            # Use Airflow CLI instead of REST API (more reliable with session auth)
            import subprocess
            import os as os_module
            
            note = f"Triggered from refresh button"
            if connection_id:
                note += f" for connection_id: {connection_id}"
            
            # Set AIRFLOW_HOME and use CLI to trigger DAG
            airflow_home = os.getenv("AIRFLOW_HOME", "/mnt/torro/torrofinalv2release/airflow")
            airflow_bin = os.path.join(airflow_home, "venv", "bin", "airflow")
            env = os_module.environ.copy()
            env["AIRFLOW_HOME"] = airflow_home
            
            # Use airflow dags trigger command (note: --note is not supported in CLI, use --conf instead)
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
                # Don't fail the request - just log the warning
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

            # Check if asset already exists (deduplication)
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
    """List all files/blobs from an Azure Storage connection"""
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        config_data = connection.config or {}
        
        # Get query parameters
        container_name = request.args.get('container')
        share_name = request.args.get('share')  # For file shares
        folder_path = request.args.get('folder_path', '')
        file_extensions = request.args.get('file_extensions')
        file_extensions_list = [ext.strip() for ext in file_extensions.split(',')] if file_extensions else None
        
        # Create Azure Blob Client (supports both connection string and service principal)
        if AZURE_AVAILABLE:
            try:
                from utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(config_data)
                
                # If neither container nor share specified, list all services
                if not container_name and not share_name:
                    containers = blob_client.list_containers()
                    file_shares = blob_client.list_file_shares()
                    return jsonify({
                        "success": True,
                        "containers": [c["name"] for c in containers],
                        "file_shares": [s["name"] for s in file_shares],
                        "message": "Specify 'container' or 'share' parameter to list files. Available services listed above."
                    }), 200
                
                # Handle file share listing
                if share_name:
                    files = blob_client.list_file_share_files(
                        share_name=share_name,
                        directory_path=folder_path,
                        file_extensions=file_extensions_list
                    )
                else:
                    # Check if this is a Data Lake Gen2 connection
                    is_datalake = config_data.get('storage_type') == 'datalake' or config_data.get('use_dfs_endpoint', False)
                    
                    # Use Data Lake API for Data Lake Gen2, blob API for regular blob storage
                    if is_datalake and hasattr(blob_client, 'list_datalake_files'):
                        # Use Data Lake Gen2 API (matches 'az storage fs file list')
                        files = blob_client.list_datalake_files(
                            file_system_name=container_name,
                            path=folder_path,
                            file_extensions=file_extensions_list
                        )
                    else:
                        # Use blob API for regular blob storage
                        files = blob_client.list_blobs(
                            container_name=container_name,
                            folder_path=folder_path,
                            file_extensions=file_extensions_list
                        )
                
                # Format response
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

@app.route('/api/connections/test-config', methods=['POST'])
@handle_error
def test_connection_config():
    """Test a connection configuration WITHOUT saving it to database"""
    if not AZURE_AVAILABLE:
        return jsonify({"error": "Azure utilities not available"}), 503
    
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        config_data = data.get('config', {})
        if not config_data:
            return jsonify({"error": "Config is required"}), 400
        
        # Test the connection using the provided config
        try:
            from utils.azure_blob_client import create_azure_blob_client
            blob_client = create_azure_blob_client(config_data)
            test_result = blob_client.test_connection()
            
            # Return test result (don't save anything)
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
    """Test an Azure Blob Storage connection and trigger discovery"""
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob connections"}), 400
        
        config_data = connection.config or {}
        
        # Create Azure Blob Client (supports both connection string and service principal)
        if AZURE_AVAILABLE:
            try:
                from utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(config_data)
                test_result = blob_client.test_connection()
                
                if not test_result.get("success"):
                    return jsonify(test_result), 200
                
                # Connection successful - trigger Airflow DAG
                airflow_triggered = False
                try:
                    # Try to trigger Airflow DAG via REST API
                    airflow_base_url = os.getenv("AIRFLOW_BASE_URL")
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
                    
                    # Use Airflow CLI instead of REST API (more reliable with session auth)
                    import subprocess
                    import os as os_module
                    
                    # Set AIRFLOW_HOME and use CLI to trigger DAG
                    airflow_home = os.getenv("AIRFLOW_HOME", "/mnt/torro/torrofinalv2release/airflow")
                    airflow_bin = os.path.join(airflow_home, "venv", "bin", "airflow")
                    env = os_module.environ.copy()
                    env["AIRFLOW_HOME"] = airflow_home
                    
                    # Use airflow dags trigger command (note: --note is not supported in CLI, use --conf instead)
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
                    # Fallback: trigger discovery directly
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
            # Azure utilities not available - try to trigger discovery anyway via Airflow DAG logic
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
    """List all Azure Storage services (containers, file shares, queues, tables) for a connection"""
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
            
            # Discover all Azure Storage services
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
    """Recursively clean object to ensure JSON serializability"""
    import base64
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, (bytes, bytearray)):
        # Convert bytearray/bytes to base64 string
        return base64.b64encode(obj).decode('utf-8')
    elif isinstance(obj, (datetime,)):
        return obj.isoformat()
    elif hasattr(obj, 'isoformat'):  # datetime-like objects
        return obj.isoformat()
    else:
        # Try to convert to string if it's not a basic type
        try:
            json.dumps(obj)  # Test if it's JSON serializable
            return obj
        except (TypeError, ValueError):
            return str(obj)

def build_technical_metadata(asset_id, blob_info, file_extension, blob_path, container_name, storage_account, file_hash, schema_hash, metadata, current_date):
    """Build technical metadata from Azure Blob properties"""
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
    
    # Get Azure metadata (key-value pairs) and clean it
    azure_metadata_dict = blob_info.get("metadata", {})
    if not isinstance(azure_metadata_dict, dict):
        azure_metadata_dict = {}
    # Clean metadata to ensure JSON serializability
    azure_metadata_dict = clean_for_json(azure_metadata_dict)
    
    # Ensure file_hash and schema_hash are strings
    file_hash_str = str(file_hash) if file_hash else ""
    schema_hash_str = str(schema_hash) if schema_hash else ""
    
    # Get size from Azure - blob_info should already have size from azure_properties merge
    # When enhanced_blob_info = {**blob_info, **azure_properties}, size from azure_properties overrides
    size_bytes = blob_info.get("size") or blob_info.get("size_bytes") or 0
    # Ensure size_bytes is an integer, not None or empty string
    if size_bytes is None or size_bytes == "":
        size_bytes = 0
    try:
        size_bytes = int(size_bytes)
        # Log if size is 0 to help debug
        if size_bytes == 0:
            logger.warning('FN:build_technical_metadata blob_path:{} message:Size is 0, may indicate missing size from Azure'.format(blob_path))
    except (ValueError, TypeError) as e:
        logger.warning('FN:build_technical_metadata blob_path:{} size_value:{} message:Could not convert size to int error:{}'.format(blob_path, size_bytes, str(e)))
        size_bytes = 0
    
    # Get format - try to determine from file extension or content type
    format_value = file_extension or "unknown"
    if format_value == "unknown" or not format_value:
        # Try to extract from content type
        content_type = blob_info.get("content_type", "")
        if content_type and "/" in content_type:
            format_value = content_type.split("/")[-1]
        elif content_type:
            format_value = content_type
    
    # Build technical metadata dict
    tech_meta = {
        "asset_id": asset_id,
        "asset_type": file_extension or "blob",
        "format": format_value,
        "content_type": blob_info.get("content_type", "application/octet-stream"),
        "size_bytes": size_bytes,
        "size": size_bytes,  # Also include as 'size' for compatibility
        "location": blob_path,
        "container": container_name,
        "storage_account": storage_account,
        "created_at": created_at or current_date,
        "last_modified": last_modified or current_date,
        "file_extension": f".{file_extension}" if file_extension else "",
        "file_hash": file_hash_str,
        "schema_hash": schema_hash_str,
        # Azure-specific properties (required fields)
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
        # Azure metadata (key-value pairs)
        "azure_metadata": azure_metadata_dict,
        # Format-specific metadata
        **metadata.get("file_metadata", {}).get("format_specific", {}),
        # Additional storage metadata
        "azure_storage_metadata": metadata.get("storage_metadata", {}).get("azure", {})
    }
    
    # Clean entire technical metadata to ensure JSON serializability
    return clean_for_json(tech_meta)

def build_operational_metadata(azure_properties, current_date):
    """Build operational metadata from Azure Blob properties"""
    # Extract owner from Azure metadata if available
    owner = azure_properties.get("metadata", {}).get("owner") if azure_properties else None
    if not owner:
        owner = "system"
    
    # Extract access level from Azure properties
    access_level = "internal"
    if azure_properties:
        lease_status = azure_properties.get("lease_status", "").lower()
        if lease_status == "locked":
            access_level = "restricted"
        elif azure_properties.get("access_tier") == "Archive":
            access_level = "archived"
    
    # Clean and ensure all values are JSON serializable
    return clean_for_json({
        "owner": str(owner),
        "created_by": str(azure_properties.get("metadata", {}).get("created_by", "azure_blob_discovery") if azure_properties else "azure_blob_discovery"),
        "last_updated_by": str(azure_properties.get("metadata", {}).get("last_updated_by", "azure_blob_discovery") if azure_properties else "azure_blob_discovery"),
        "last_updated_at": current_date,
        "access_level": access_level,
        "approval_status": "pending_review",
        # Azure operational properties
        "lease_status": azure_properties.get("lease_status") if azure_properties else None,
        "access_tier": azure_properties.get("access_tier") if azure_properties else None,
        "etag": azure_properties.get("etag", "").strip('"') if azure_properties and azure_properties.get("etag") else None
    })

def build_business_metadata(blob_info, azure_properties, file_extension, container_name):
    """Build business metadata from Azure Blob properties and metadata tags"""
    azure_metadata = azure_properties.get("metadata", {}) if azure_properties else {}
    
    # Clean azure_metadata to ensure JSON serializability
    azure_metadata = clean_for_json(azure_metadata)
    
    # Extract business metadata from Azure blob metadata tags
    description = azure_metadata.get("description") or f"Azure Blob Storage file: {blob_info.get('name', 'unknown')}"
    business_owner = azure_metadata.get("business_owner") or azure_metadata.get("owner") or "system"
    department = azure_metadata.get("department") or "Data Engineering"
    classification = azure_metadata.get("classification") or "internal"
    sensitivity_level = azure_metadata.get("sensitivity_level") or azure_metadata.get("sensitivity") or "medium"
    
    # Extract tags from Azure metadata
    tags = []
    if azure_metadata.get("tags"):
        tags_value = azure_metadata["tags"]
        if isinstance(tags_value, str):
            tags = [t.strip() for t in tags_value.split(",")]
        elif isinstance(tags_value, list):
            tags = [str(t) for t in tags_value]  # Ensure all tags are strings
    
    # Add container name as a tag
    if container_name and container_name not in tags:
        tags.append(container_name)
    
    # Clean all values to ensure JSON serializability
    return clean_for_json({
        "description": str(description),
        "data_type": file_extension or "unknown",
        "business_owner": str(business_owner),
        "department": str(department),
        "classification": str(classification),
        "sensitivity_level": str(sensitivity_level),
        "tags": tags,
        # Additional Azure metadata
        "container": container_name,
        "content_language": azure_properties.get("content_language") if azure_properties else None,
        "azure_metadata_tags": azure_metadata
    })

@app.route('/api/connections/<int:connection_id>/discover', methods=['POST'])
@handle_error
def discover_assets(connection_id):
    """Discover assets from Azure Blob Storage
    
    For large-scale discovery (>1000 assets), this runs synchronously but processes in batches.
    Consider using background workers for very large discoveries (>10k assets).
    """
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
        
        # Create Azure Blob Client (supports both connection string and service principal)
        try:
            from utils.azure_blob_client import create_azure_blob_client
            blob_client = create_azure_blob_client(config_data)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        
        # Auto-discover all containers if none specified
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
            folders_found = {}  # Track folders per container
            assets_by_folder = {}  # Track assets per folder
            discovered_assets_lock = Lock()
            folders_lock = Lock()
            
            def process_container(container_name):
                """Process a single container with all its blobs"""
                container_discovered_assets = []
                container_folders_found = set()
                container_assets_by_folder = {}
                container_skipped_count = 0
                
                try:
                    logger.info('FN:discover_assets container_name:{} folder_path:{} message:Listing files'.format(container_name, folder_path))
                    
                    # Use Data Lake API for Data Lake Gen2, blob API for regular blob storage
                    is_datalake = config_data.get('storage_type') == 'datalake' or config_data.get('use_dfs_endpoint', False)
                    if is_datalake and hasattr(blob_client, 'list_datalake_files'):
                        # Use Data Lake Gen2 API for better metadata and hierarchical support
                        blobs = blob_client.list_datalake_files(
                            file_system_name=container_name,
                            path=folder_path,
                            file_extensions=None
                        )
                        logger.info('FN:discover_assets container_name:{} message:Using Data Lake Gen2 API'.format(container_name))
                    else:
                        # Use regular blob API
                        blobs = blob_client.list_blobs(
                            container_name=container_name,
                            folder_path=folder_path,
                            file_extensions=None
                        )
                        logger.info('FN:discover_assets container_name:{} message:Using Blob Storage API'.format(container_name))
                    
                    logger.info('FN:discover_assets container_name:{} blob_count:{}'.format(container_name, len(blobs)))
                    
                    # Group blobs by folder
                    folders_in_container = set()
                    assets_in_folders = {}
                    
                    for blob_info in blobs:
                        blob_path = blob_info["full_path"]
                        # Extract folder path (everything before the last /)
                        if "/" in blob_path:
                            folder = "/".join(blob_path.split("/")[:-1])
                        else:
                            folder = ""  # Root of container
                        
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
                    
                    # Process blobs with 50 concurrent workers (optimized for large-scale discovery)
                    max_workers = 50 if len(blobs) > 500 else 20
                    logger.info('FN:discover_assets container_name:{} total_blobs:{} message:Processing with {} concurrent workers'.format(container_name, len(blobs), max_workers))
                    
                    def process_blob(blob_info):
                        """Process a single blob and return asset data"""
                        try:
                            blob_path = blob_info["full_path"]
                            file_extension = blob_info["name"].split(".")[-1].lower() if "." in blob_info["name"] else ""
                            connector_id = f"azure_blob_{connection.name}"
                            
                            # Track asset for response
                            asset_name = blob_info.get("name", "unknown")
                            asset_folder = ""
                            if "/" in blob_path:
                                parts = blob_path.split("/")
                                asset_folder = "/".join(parts[:-1])
                                asset_name = parts[-1]
                            
                            # Create a new database session for this thread
                            thread_db = SessionLocal()
                            try:
                                # Check if asset already exists (deduplication)
                                existing_asset = None
                                if AZURE_AVAILABLE:
                                    try:
                                        existing_asset = check_asset_exists(thread_db, connector_id, blob_path)
                                        if existing_asset:
                                            logger.debug('FN:discover_assets blob_path:{} existing_asset_id:{} message:Found existing asset for deduplication'.format(blob_path, existing_asset.id))
                                    except Exception as e:
                                        logger.error('FN:discover_assets blob_path:{} error:check_asset_exists failed error:{}'.format(blob_path, str(e)))
                                        # Continue without deduplication if check fails (shouldn't happen, but be safe)
                                        existing_asset = None
                                
                                # Get full blob properties from Azure for complete metadata
                                # This ensures we have all metadata for every file in the datalake
                                azure_properties = None
                                try:
                                    azure_properties = blob_client.get_blob_properties(container_name, blob_path)
                                    # Ensure size is properly extracted - get_blob_properties returns size in "size" key
                                    if azure_properties and "size" in azure_properties:
                                        # Size is already in azure_properties from get_blob_properties
                                        logger.debug('FN:discover_assets container_name:{} blob_path:{} size:{} message:Got size from Azure properties'.format(container_name, blob_path, azure_properties.get("size", 0)))
                                except Exception as e:
                                    logger.warning('FN:discover_assets container_name:{} blob_path:{} message:Could not get blob properties error:{}'.format(container_name, blob_path, str(e)))
                                    # Fallback to blob_info if properties fetch fails
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
                                
                                # Get file sample for metadata extraction
                                # Fetch metadata for every file in the datalake to ensure complete information
                                file_sample = None
                                try:
                                    if file_extension == "parquet":
                                        file_sample = blob_client.get_blob_tail(container_name, blob_path, max_bytes=8192)
                                    else:
                                        file_sample = blob_client.get_blob_sample(container_name, blob_path, max_bytes=1024)
                                except Exception as e:
                                    logger.warning('FN:discover_assets container_name:{} blob_path:{} message:Could not get sample error:{}'.format(container_name, blob_path, str(e)))
                                
                                # Extract metadata (merge Azure properties into blob_info for extract_file_metadata)
                                enhanced_blob_info = {**blob_info, **azure_properties}
                                if file_sample:
                                    metadata = extract_file_metadata(enhanced_blob_info, file_sample)
                                else:
                                    metadata = extract_file_metadata(enhanced_blob_info, None)
                                
                                # Get hashes for deduplication
                                file_hash = metadata.get("file_hash", generate_file_hash(b""))
                                schema_hash = metadata.get("schema_hash", generate_schema_hash({}))
                                
                                # Check if we should update or insert
                                should_update, schema_changed = should_update_or_insert(
                                    existing_asset,
                                    file_hash,
                                    schema_hash
                                )
                                
                                # Skip if nothing changed OR if asset already exists and we shouldn't update
                                if existing_asset:
                                    if not should_update:
                                        logger.info('FN:discover_assets blob_path:{} existing_asset_id:{} message:Skipping unchanged asset (deduplication)'.format(blob_path, existing_asset.id))
                                        # Return None to indicate this asset was skipped (deduplication)
                                        return None
                                    else:
                                        logger.info('FN:discover_assets blob_path:{} existing_asset_id:{} schema_changed:{} message:Updating existing asset'.format(blob_path, existing_asset.id, schema_changed))
                                
                                current_date = datetime.utcnow().isoformat()
                                
                                if existing_asset and schema_changed:
                                    # Update existing asset with new schema
                                    existing_asset.name = blob_info["name"]
                                    existing_asset.type = file_extension or "blob"
                                    
                                    # Build technical metadata from Azure
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
                                    
                                    # Build operational metadata from Azure
                                    operational_meta = build_operational_metadata(
                                        azure_properties=azure_properties,
                                        current_date=current_date
                                    )
                                    
                                    # Build business metadata from Azure
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
                                    # Create new asset - use consistent ID without timestamp for deduplication
                                    # Normalize blob_path for ID generation (remove leading/trailing slashes)
                                    normalized_path = blob_path.strip('/').replace('/', '_').replace(' ', '_')
                                    asset_id = f"azure_blob_{connection.name}_{normalized_path}"
                                    
                                    # Build all metadata from Azure properties
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
                                            "columns": columns_clean
                                        },
                                        "name": asset_name,
                                        "folder": asset_folder,
                                        "container": container_name,
                                        "blob_path": blob_path,
                                        "config_data": config_data,
                                        "connection_id": connection_id,
                                        "connection_name": connection.name,
                                        "thread_db": thread_db
                                    }
                            except Exception as e:
                                thread_db.close()
                                raise e
                        except Exception as e:
                            logger.error('FN:discover_assets container_name:{} blob_name:{} error:{}'.format(container_name, blob_info.get('name', 'unknown'), str(e)), exc_info=True)
                            return None
                    
                    # Process blobs concurrently with 20 workers
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(process_blob, blob_info): blob_info for blob_info in blobs}
                        
                        for future in as_completed(futures):
                            try:
                                result = future.result()
                                if result:
                                    container_discovered_assets.append(result)
                                elif result is None:
                                    # Asset was skipped due to deduplication (already exists and unchanged)
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
            
            # Track total skipped count across all containers
            total_skipped_from_containers = 0
            
            # Process containers concurrently (up to 10 containers in parallel, each with 50 blob workers)
            # This allows processing up to 500 blobs simultaneously (10 containers * 50 blobs)
            logger.info('FN:discover_assets total_containers:{} message:Processing containers with 10 concurrent workers'.format(len(containers)))
            with ThreadPoolExecutor(max_workers=min(10, len(containers))) as container_executor:
                container_futures = {container_executor.submit(process_container, container_name): container_name for container_name in containers}
                
                for future in as_completed(container_futures):
                    try:
                        result = future.result()
                        if result:
                            with discovered_assets_lock:
                                discovered_assets.extend(result["discovered_assets"])
                                # Aggregate skipped count from containers
                                total_skipped_from_containers += result.get("skipped_count", 0)
                            with folders_lock:
                                container_name = container_futures[future]
                                folders_found[container_name] = result["folders_found"]
                                assets_by_folder[container_name] = result["assets_by_folder"]
                    except Exception as e:
                        container_name = container_futures[future]
                        logger.error('FN:discover_assets container_name:{} message:Error processing container error:{}'.format(container_name, str(e)), exc_info=True)
            
            logger.info('FN:discover_assets total_assets_to_process:{}'.format(len(discovered_assets)))
            
            # For very large discoveries, process in batches to avoid memory issues
            # Increased batch size for faster processing (3500 assets)
            batch_size = 2000
            total_assets = len(discovered_assets)
            
            if total_assets > 5000:
                logger.info('FN:discover_assets total_assets:{} batch_size:{} message:Large discovery detected'.format(total_assets, batch_size))
            
            # Save discovered assets
            created_count = 0
            updated_count = 0
            skipped_count = total_skipped_from_containers  # Start with skipped items from deduplication during discovery
            
            # Process in batches for large discoveries
            for batch_start in range(0, total_assets, batch_size):
                batch_end = min(batch_start + batch_size, total_assets)
                batch = discovered_assets[batch_start:batch_end]
                
                if total_assets > 5000:
                    batch_num = batch_start//batch_size + 1
                    total_batches = (total_assets + batch_size - 1)//batch_size
                    logger.info('FN:discover_assets batch_number:{} total_batches:{} batch_start:{} batch_end:{} total_assets:{}'.format(batch_num, total_batches, batch_start+1, batch_end, total_assets))
                
                for item in batch:
                    try:
                        if item is None:
                            # Item was skipped due to deduplication (shouldn't happen here, but handle it)
                            skipped_count += 1
                            continue
                        elif item.get("action") == "updated":
                            # Asset already exists and was updated - commit from thread's db session
                            thread_db = item.get("thread_db")
                            if thread_db:
                                try:
                                    thread_db.commit()
                                    updated_count += 1
                                except Exception as e:
                                    thread_db.rollback()
                                    logger.error('FN:discover_assets message:Error committing updated asset error:{}'.format(str(e)), exc_info=True)
                                    skipped_count += 1
                                finally:
                                    thread_db.close()
                            else:
                                # Fallback: update in main session
                                updated_count += 1
                        elif item.get("action") == "created":
                            # New asset to create
                            asset_data = item["asset_data"]
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
                                db.flush()  # Flush to get asset ID before creating discovery record
                            except Exception as flush_error:
                                # Handle race condition: another thread may have created this asset
                                error_str = str(flush_error)
                                if "Duplicate entry" in error_str or "1062" in error_str or "UNIQUE constraint" in error_str or "IntegrityError" in error_str:
                                    # Asset already exists (race condition from concurrent processing)
                                    db.rollback()
                                    # Query for existing asset
                                    existing_asset = db.query(Asset).filter(Asset.id == asset_data['id']).first()
                                    if existing_asset:
                                        logger.debug('FN:discover_assets asset_id:{} message:Asset already exists (race condition), skipping duplicate creation'.format(asset_data['id']))
                                        skipped_count += 1
                                        continue
                                    else:
                                        # Rollback cleared it, retry once
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
                                            db.flush()
                                        except Exception as retry_error:
                                            logger.warning('FN:discover_assets asset_id:{} message:Retry failed, skipping error:{}'.format(asset_data['id'], str(retry_error)))
                                            skipped_count += 1
                                            continue
                                else:
                                    # Different error, re-raise
                                    raise
                            
                            # Create discovery record with sequential ID (auto-increment)
                            # Extract storage location from technical metadata
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
                            
                            # Build file_metadata from technical metadata
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
                            
                            # Get schema_hash from technical metadata
                            schema_hash = tech_meta.get("schema_hash", "")
                            
                            discovery = DataDiscovery(
                                asset_id=asset.id,
                                storage_location=storage_location,
                                file_metadata=file_metadata,
                                schema_json=asset_data.get('columns', []),
                                schema_hash=schema_hash,
                                status="pending",
                                approval_status=None,
                                discovered_at=datetime.utcnow(),
                                folder_path=item.get("folder", ""),
                                data_source_type="azure_blob_storage",
                                environment=item_config.get("environment", config_data.get("environment", "production")),
                                discovery_info={
                                    "connection_id": item_connection_id if item_connection_id else connection_id,
                                    "connection_name": item_connection_name,
                                    "container": item.get("container", ""),
                                    "discovered_by": "api_discovery"
                                }
                            )
                            db.add(discovery)
                            created_count += 1
                            logger.debug('FN:discover_assets asset_name:{} discovery_id:{} message:Added asset and discovery record'.format(
                                asset_data.get('name', 'unknown'), discovery.id if discovery.id else 'pending'
                            ))
                    except Exception as e:
                        logger.error('FN:discover_assets message:Error processing asset error:{}'.format(str(e)), exc_info=True)
                        skipped_count += 1
                        continue
                
                # Commit batch to avoid long transactions
                if total_assets > 5000:
                    try:
                        db.commit()
                        logger.debug('FN:discover_assets batch_number:{} message:Committed batch'.format(batch_start//batch_size + 1))
                    except Exception as e:
                        logger.error('FN:discover_assets message:Error committing batch error:{}'.format(str(e)), exc_info=True)
                        db.rollback()
                        raise
            
            logger.info('FN:discover_assets created_count:{} updated_count:{} message:Committing to database'.format(created_count, updated_count))
            try:
                db.commit()
                logger.info('FN:discover_assets total_committed:{} message:Successfully committed assets to database'.format(created_count + updated_count))
            except Exception as e:
                logger.error('FN:discover_assets message:Error committing assets to database error:{}'.format(str(e)), exc_info=True)
                db.rollback()
                raise
            
            total_processed = created_count + updated_count
            
            logger.info('FN:discover_assets total_processed:{} created_count:{} updated_count:{} skipped_count:{} message:Discovery summary'.format(total_processed, created_count, updated_count, skipped_count))
            
            # Build folder and asset structure for response
            # Structure: {container: {folder: [assets]}}
            # If no folders (only root files), folder will be empty string ""
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
            
            # Determine if containers have folders or just root files
            has_folders = {}
            for container_name in containers:
                folders = folders_found.get(container_name, [])
                # Check if there are any non-empty folders
                has_folders[container_name] = any(f for f in folders if f != "")
            
            # Discover File Shares
            file_shares_discovered = 0
            try:
                file_shares = blob_client.list_file_shares()
                logger.info('FN:discover_assets file_shares_count:{}'.format(len(file_shares)))
                
                for share in file_shares:
                    share_name = share["name"]
                    try:
                        # List files in the share
                        share_files = blob_client.list_file_share_files(share_name=share_name, directory_path=folder_path)
                        
                        for file_info in share_files:
                            try:
                                file_path = file_info.get("full_path", file_info.get("name", ""))
                                file_extension = file_info.get("name", "").split(".")[-1].lower() if "." in file_info.get("name", "") else ""
                                connector_id = f"azure_blob_{connection.name}"
                                
                                # Check if asset already exists
                                existing_asset = check_asset_exists(db, connector_id, f"file-share://{share_name}/{file_path}") if AZURE_AVAILABLE else None
                                
                                # Build asset data
                                storage_path_for_check = f"file-share://{share_name}/{file_path}"
                                # Generate consistent asset ID (without timestamp for deduplication)
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
                                        "location": storage_path_for_check,  # Add location field for deduplication check
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
                                    # Update existing asset
                                    existing_asset.business_metadata = asset_data["business_metadata"]
                                    existing_asset.technical_metadata = asset_data["technical_metadata"]
                                    db.commit()
                                    updated_count += 1
                                else:
                                    # Create new asset (storage_location is NOT part of Asset model)
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
            
            # Discover Queues (queues are assets themselves)
            queues_discovered = 0
            try:
                queues = blob_client.list_queues()
                logger.info('FN:discover_assets queues_count:{}'.format(len(queues)))
                
                for queue in queues:
                    try:
                        queue_name = queue["name"]
                        connector_id = f"azure_blob_{connection.name}"
                        
                        # Check if asset already exists
                        existing_asset = check_asset_exists(db, connector_id, f"queue://{queue_name}") if AZURE_AVAILABLE else None
                        
                        # Build asset data
                        storage_location_str = f"queue://{queue_name}"
                        # Use consistent ID format (without timestamp to avoid duplicates on refresh)
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
                                "location": storage_location_str,  # Add location field for deduplication check
                                "service_type": "azure_queue",
                                "queue_name": queue_name,
                                "metadata": queue.get("metadata", {}),
                                "storage_location": storage_location_str
                            }
                        }
                        
                        if existing_asset:
                            # Update existing asset
                            existing_asset.business_metadata = asset_data["business_metadata"]
                            existing_asset.technical_metadata = asset_data["technical_metadata"]
                            db.commit()
                            updated_count += 1
                        else:
                            # Create new asset
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
            
            # Discover Tables (tables are assets themselves)
            tables_discovered = 0
            try:
                tables = blob_client.list_tables()
                logger.info('FN:discover_assets tables_count:{}'.format(len(tables)))
                
                for table in tables:
                    try:
                        table_name = table["name"]
                        connector_id = f"azure_blob_{connection.name}"
                        
                        # Check if asset already exists
                        existing_asset = check_asset_exists(db, connector_id, f"table://{table_name}") if AZURE_AVAILABLE else None
                        
                        # Build asset data
                        storage_location_str = f"table://{table_name}"
                        # Use consistent ID format (without timestamp to avoid duplicates on refresh)
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
                            # Update existing asset
                            existing_asset.business_metadata = asset_data["business_metadata"]
                            existing_asset.technical_metadata = asset_data["technical_metadata"]
                            db.commit()
                            updated_count += 1
                        else:
                            # Create new asset
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
            
            # Update total processed count
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
                "has_folders": has_folders,  # Indicates if container has subfolders
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
    """Approve a discovered asset - updates both assets and data_discovery tables"""
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
        asset.operational_metadata["approved_by"] = "user"  # TODO: Get from auth
        
        # CRITICAL: Flag the JSON field as modified so SQLAlchemy saves it
        flag_modified(asset, "operational_metadata")
        
        # Also update/create data_discovery record
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
            # Create new data_discovery record
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
        
        # Verify the save worked
        logger.info('FN:approve_asset asset_id:{} approval_status:{} saved_to_db:True'.format(
            asset_id, asset.operational_metadata.get("approval_status")
        ))
        
        # Return full asset data so frontend can update without refetching
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
    """Reject a discovered asset - updates both assets and data_discovery tables"""
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
        asset.operational_metadata["rejected_by"] = "user"  # TODO: Get from auth
        asset.operational_metadata["rejection_reason"] = reason
        
        # CRITICAL: Flag the JSON field as modified so SQLAlchemy saves it
        flag_modified(asset, "operational_metadata")
        
        # Also update/create data_discovery record
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
            # Create new data_discovery record
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
        
        # Verify the save worked
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
    """Publish an approved asset - updates both assets and data_discovery tables"""
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        
        # Check if asset is approved
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
        asset.operational_metadata["published_by"] = "user"  # TODO: Get from auth
        asset.operational_metadata["published_to"] = published_to
        
        # CRITICAL: Flag the JSON field as modified so SQLAlchemy saves it
        flag_modified(asset, "operational_metadata")
        
        # Also update/create data_discovery record
        discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
        if discovery:
            discovery.status = "published"
            discovery.published_at = publish_time
            discovery.published_to = published_to
        else:
            # Create new data_discovery record
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

# ============================================================================
# REAL DATA LINEAGE API ENDPOINTS
# ============================================================================

@app.route('/api/lineage/relationships', methods=['GET'])
@handle_error
def get_lineage_relationships():
    """Get all lineage relationships with end-to-end column-level lineage and quality metrics"""
    db = SessionLocal()
    try:
        # Filter to only show REAL lineage (exclude inferred/fake)
        # Only include: sql_parsing, manual, api, etl, dbt, databricks
        include_inferred = request.args.get('include_inferred', 'false').lower() == 'true'
        
        if include_inferred:
            # Show all relationships including inferred
            relationships = db.query(LineageRelationship).all()
        else:
            # Only show real lineage - exclude inferred methods
            exclude_methods = ['column_matching', 'ml_inference', 'inferred']
            relationships = db.query(LineageRelationship).filter(
                ~LineageRelationship.extraction_method.in_(exclude_methods)
            ).all()
            
            # Also filter out relationships with extraction_method = None or empty (likely inferred)
            relationships = [rel for rel in relationships 
                           if rel.extraction_method and rel.extraction_method not in exclude_methods]
        
        result = []
        
        for rel in relationships:
            # Get source and target assets for quality metrics
            source_asset = db.query(Asset).filter(Asset.id == rel.source_asset_id).first()
            target_asset = db.query(Asset).filter(Asset.id == rel.target_asset_id).first()
            
            # Calculate quality scores
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
            
            # Build end-to-end column lineage
            column_lineage = rel.column_lineage or []
            end_to_end_lineage = []
            
            # If we have column lineage, enhance it with quality info
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
            
            # Add quality propagation if both qualities exist
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
    """Create a new lineage relationship"""
    db = SessionLocal()
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        # Validate required fields
        if not data.get('source_asset_id') or not data.get('target_asset_id'):
            return jsonify({"error": "source_asset_id and target_asset_id are required"}), 400
        
        # Check if assets exist
        source_asset = db.query(Asset).filter(Asset.id == data['source_asset_id']).first()
        target_asset = db.query(Asset).filter(Asset.id == data['target_asset_id']).first()
        
        if not source_asset:
            return jsonify({"error": f"Source asset {data['source_asset_id']} not found"}), 404
        if not target_asset:
            return jsonify({"error": f"Target asset {data['target_asset_id']} not found"}), 404
        
        # Create relationship
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
    """Parse SQL query and extract lineage"""
    try:
        data = request.json
        if not data or not data.get('sql_query'):
            return jsonify({"error": "sql_query is required"}), 400
        
        sql_query = data['sql_query']
        dialect = data.get('dialect', 'mysql')
        source_system = data.get('source_system', 'unknown')
        job_id = data.get('job_id')
        job_name = data.get('job_name')
        
        # Extract lineage
        lineage_result = extract_lineage_from_sql(sql_query, dialect)
        
        # Store SQL query for future reference
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
    """Infer lineage relationships based on column name matching"""
    db = SessionLocal()
    try:
        data = request.json or {}
        min_confidence = float(data.get('min_confidence', 0.5))
        min_matching_columns = int(data.get('min_matching_columns', 2))
        
        # Get all assets with columns
        assets = db.query(Asset).filter(Asset.columns.isnot(None)).all()
        
        created_relationships = []
        skipped_relationships = []
        
        # Compare each pair of assets
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
                
                # Use ML-based inference for better matching
                column_lineage, confidence = infer_relationships_ml(
                    source_columns,
                    target_columns,
                    min_matching_ratio=min_confidence
                )
                
                if len(column_lineage) < min_matching_columns:
                    continue
                
                # Check if relationship already exists
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
                
                # Create relationship
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
                    extraction_method='ml_inference',  # Mark as inferred for filtering
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
    """Parse SQL query and automatically create lineage relationships"""
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
        
        # Extract lineage
        lineage_result = extract_lineage_from_sql(sql_query, dialect)
        
        if not lineage_result.get('target_table') or not lineage_result.get('source_tables'):
            return jsonify({
                "lineage": lineage_result,
                "relationships_created": 0,
                "message": "No target or source tables found in query"
            }), 200
        
        # Store SQL query
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
        
        # Find target asset by name
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
        
        # Create relationships for each source table
        created_count = 0
        for source_table in lineage_result.get('source_tables', []):
            source_asset = db.query(Asset).filter(
                Asset.name.ilike(f'%{source_table}%')
            ).first()
            
            if not source_asset:
                continue
            
            # Check if relationship already exists
            existing = db.query(LineageRelationship).filter(
                LineageRelationship.source_asset_id == source_asset.id,
                LineageRelationship.target_asset_id == target_asset.id
            ).first()
            
            if existing:
                continue
            
            # Create relationship
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
    """Get impact analysis for an asset - what breaks if this asset changes"""
    db = SessionLocal()
    try:
        # Find all relationships where this asset is a source (downstream impact)
        downstream = db.query(LineageRelationship).filter(
            LineageRelationship.source_asset_id == asset_id
        ).all()
        
        # Find all relationships where this asset is a target (upstream dependencies)
        upstream = db.query(LineageRelationship).filter(
            LineageRelationship.target_asset_id == asset_id
        ).all()
        
        # Build impact tree
        impacted_assets = set()
        impact_paths = []
        
        # Direct downstream impact
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
        
        # Recursive downstream impact (2 levels deep)
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
        
        # Get asset details for impacted assets
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
        
        # Get upstream dependencies
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
    """Get complete lineage for a specific asset (upstream and downstream)"""
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        
        # Get upstream (sources)
        upstream_rels = db.query(LineageRelationship).filter(
            LineageRelationship.target_asset_id == asset_id
        ).all()
        
        # Get downstream (targets)
        downstream_rels = db.query(LineageRelationship).filter(
            LineageRelationship.source_asset_id == asset_id
        ).all()
        
        # Build lineage graph
        nodes = [{
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "is_selected": True
        }]
        
        edges = []
        node_ids = {asset.id}
        
        # Add upstream nodes and edges
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
        
        # Add downstream nodes and edges
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
    port = int(os.getenv("FLASK_PORT", "8099"))
    debug = app.config.get("DEBUG", False)
    logger.info('FN:__main__ port:{} environment:{} debug:{} message:Starting Flask app'.format(port, env, debug))
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", port))
    app.run(host=host, port=port, debug=debug)
