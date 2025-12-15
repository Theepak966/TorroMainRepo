from flask import Flask, jsonify, request
from flask_cors import CORS
from .database import engine, Base, SessionLocal
from .models import Asset, Connection
from .config import config
import os
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps

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
    logger.info("Database tables initialized successfully")
except Exception as e:
    logger.error(f"Error initializing database tables: {str(e)}")
    raise

def handle_error(f):
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {f.__name__}: {str(e)}", exc_info=True)
            if app.config.get("DEBUG"):
                return jsonify({"error": str(e)}), 500
            else:
                return jsonify({"error": "An internal error occurred"}), 500
    return decorated_function

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

        existing = db.query(Connection).filter(Connection.name == data['name']).first()
        if existing:
            return jsonify({"error": "Connection with this name already exists"}), 409

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

        logger.info(f"Created connection: {connection.name} (ID: {connection.id})")

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
        logger.error(f"Error creating connection: {str(e)}", exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create connection"}), 400
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

        connector_id_pattern = f"parquet_test_{connection.name}"
        associated_assets = db.query(Asset).filter(Asset.connector_id == connector_id_pattern).all()

        for asset in associated_assets:
            db.delete(asset)

        connection_name = connection.name
        db.delete(connection)
        db.commit()

        deleted_count = len(associated_assets)
        logger.info(f"Deleted connection: {connection_name} (ID: {connection_id}) and {deleted_count} associated assets")

        return jsonify({
            "message": "Connection deleted successfully",
            "deleted_assets": deleted_count
        }), 200
    except Exception as e:
        db.rollback()
        logger.error(f"Error deleting connection: {str(e)}", exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to delete connection"}), 400
    finally:
        db.close()

@app.route('/api/assets', methods=['GET'])
@handle_error
def get_assets():
    
    db = SessionLocal()
    try:
        assets = db.query(Asset).all()
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
            "columns": asset.columns
        } for asset in assets])
    finally:
        db.close()

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

        for asset_data in assets_data:
            if not asset_data.get('id'):
                return jsonify({"error": "Asset ID is required"}), 400
            if not asset_data.get('name'):
                return jsonify({"error": "Asset name is required"}), 400
            if not asset_data.get('type'):
                return jsonify({"error": "Asset type is required"}), 400

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

        logger.info(f"Created {len(created_assets)} asset(s)")

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
            "columns": asset.columns
        } for asset in created_assets]), 201
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating assets: {str(e)}", exc_info=True)
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

        logger.info(f"Updated asset: {asset_id}")

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
        logger.error(f"Error updating asset: {str(e)}", exc_info=True)
        if app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to update asset"}), 400
    finally:
        db.close()

@app.errorhandler(404)
def not_found(error):
    
    return jsonify({"error": "Resource not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    
    logger.error(f"Internal server error: {str(error)}", exc_info=True)
    if app.config.get("DEBUG"):
        return jsonify({"error": str(error)}), 500
    else:
        return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == '__main__':
    port = int(os.getenv("FLASK_PORT", "8099"))
    debug = app.config.get("DEBUG", False)
    logger.info(f"Starting Flask app on port {port} in {env} mode (debug={debug})")
    app.run(host='0.0.0.0', port=port, debug=debug)
