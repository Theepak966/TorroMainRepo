"""
Connection management routes.
Production-level route handlers for connection CRUD and operations.
"""

import os
import sys
import json
import threading
import subprocess
import logging
from flask import Blueprint, request, jsonify, Response
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal, get_db_session
from models import Asset, Connection, DataDiscovery
from utils.helpers import handle_error, sanitize_connection_config
from utils.shared_state import DISCOVERY_PROGRESS, DISCOVERY_PROGRESS_LOCK, try_start_lineage_job, finish_lineage_job
from utils.azure_utils import AZURE_AVAILABLE
from services.discovery_service import discover_oracle_assets, discover_assets, discover_s3_assets
from flask import current_app

logger = logging.getLogger(__name__)

connections_bp = Blueprint('connections', __name__)


@connections_bp.route('/api/connections', methods=['GET'])
@handle_error
def get_connections():
    """Get all connections with optional pagination"""
    db = SessionLocal()
    try:
        page = request.args.get('page', type=int)
        per_page = request.args.get('per_page', type=int)
        
        use_pagination = page is not None and per_page is not None
        
        if use_pagination:
            if page < 1:
                return jsonify({"error": "Page must be >= 1"}), 400
            if per_page < 1:
                return jsonify({"error": "Per page must be >= 1"}), 400
            if per_page > 1000:
                return jsonify({"error": "Per page cannot exceed 1000"}), 400
            
            per_page = min(per_page, 1000)
            offset = (page - 1) * per_page
            
            total_count = db.query(Connection).count()
            connections = db.query(Connection).order_by(Connection.id.desc()).limit(per_page).offset(offset).all()
            
            total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
            result = [{
                "id": conn.id,
                "name": conn.name,
                "connector_type": conn.connector_type,
                "connection_type": conn.connection_type,
                "config": sanitize_connection_config(conn.config),
                "status": conn.status,
                "created_at": conn.created_at.isoformat() if conn.created_at else None
            } for conn in connections]
            
            return jsonify({
                "connections": result,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total_count,
                    "total_pages": total_pages
                }
            })
        else:
            connections = db.query(Connection).all()
            return jsonify([{
                "id": conn.id,
                "name": conn.name,
                "connector_type": conn.connector_type,
                "connection_type": conn.connection_type,
                "config": sanitize_connection_config(conn.config),
                "status": conn.status,
                "created_at": conn.created_at.isoformat() if conn.created_at else None
            } for conn in connections])
    finally:
        db.close()


@connections_bp.route('/api/connections', methods=['POST'])
@handle_error
def create_connection():
    """Create a new connection"""
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

        logger.info('FN:create_connection connection_name:{} connection_id:{}'.format(connection.name, connection.id))

        return jsonify({
            "id": connection.id,
            "name": connection.name,
            "connector_type": connection.connector_type,
            "connection_type": connection.connection_type,
            "config": sanitize_connection_config(connection.config),
            "status": connection.status,
            "created_at": connection.created_at.isoformat() if connection.created_at else None
        }), 201
    except Exception as e:
        db.rollback()
        logger.error('FN:create_connection error:{}'.format(str(e)), exc_info=True)

        error_str = str(e)
        if "Duplicate entry" in error_str or "1062" in error_str or "UNIQUE constraint" in error_str:
            return jsonify({
                "error": f"A connection with the name '{data.get('name', '')}' already exists. Please use a different name or update the existing connection."
            }), 409
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create connection"}), 400
    finally:
        db.close()


@connections_bp.route('/api/connections/<int:connection_id>', methods=['PUT'])
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
        
        if 'connection_type' in data:
            connection.connection_type = data['connection_type']
        if 'config' in data:
            connection.config = data['config']
        if 'status' in data:
            connection.status = data['status']
        
        db.commit()
        
        logger.info('FN:update_connection connection_id:{} connection_name:{}'.format(connection_id, connection.name))
        
        return jsonify({
            "id": connection.id,
            "name": connection.name,
            "connector_type": connection.connector_type,
            "connection_type": connection.connection_type,
            "config": sanitize_connection_config(connection.config),
            "status": connection.status,
            "created_at": connection.created_at.isoformat() if connection.created_at else None
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:update_connection connection_id:{} error:{}'.format(connection_id, str(e)), exc_info=True)
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to update connection"}), 400
    finally:
        db.close()


@connections_bp.route('/api/connections/<int:connection_id>', methods=['DELETE'])
@handle_error
def delete_connection(connection_id):
    """Delete a connection and its associated assets"""
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404

        # Build connector_id pattern (azure_blob, aws_s3, oracle_db, etc.)
        connector_id_pattern = f"{connection.connector_type}_{connection.name}"
        
        asset_count = db.query(Asset).filter(Asset.connector_id == connector_id_pattern).count()
        
        deleted_assets_count = db.query(Asset).filter(
            Asset.connector_id == connector_id_pattern
        ).delete(synchronize_session=False)
        
        connection_name = connection.name
        db.delete(connection)
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
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to delete connection"}), 400
    finally:
        db.close()


@connections_bp.route('/api/connections/<int:connection_id>/list-files', methods=['GET'])
@handle_error
def list_connection_files(connection_id):
    """List files in a connection's containers"""
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
                
                files_list = [{
                    "name": file_info.get("name"),
                    "full_path": file_info.get("full_path"),
                    "size": file_info.get("size", 0),
                    "content_type": file_info.get("content_type"),
                    "last_modified": file_info.get("last_modified").isoformat() if file_info.get("last_modified") else None,
                    "created_at": file_info.get("created_at").isoformat() if file_info.get("created_at") else None,
                    "etag": file_info.get("etag"),
                    "blob_type": file_info.get("blob_type")
                } for file_info in files]
                
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


@connections_bp.route('/api/connections/test-config', methods=['GET', 'POST'])
@handle_error
def test_connection_config():
    """Test connection configuration"""
    try:
        config_data = {}
        
        if request.method == 'POST':
            data = request.json
            if not data:
                return jsonify({"error": "Request body is required"}), 400
            config_data = data.get('config', {})
            connector_type_hint = data.get('connector_type')
        else:
            connector_type_hint = None
            config_json_str = request.args.get('config')
            if config_json_str:
                try:
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
                    'host': request.args.get('host'),
                    'port': request.args.get('port'),
                    'service_name': request.args.get('service_name'),
                    'username': request.args.get('username'),
                    'password': request.args.get('password'),
                }
                config_data = {k: v for k, v in config_data.items() if v is not None}
            connector_type_hint = request.args.get('connector_type')
        
        if not config_data:
            return jsonify({"error": "Config is required. For GET, provide 'config' query param as JSON string or individual params"}), 400
        
        # AWS S3: when connector_type is aws_s3, always route to S3 (avoids misrouting to Azure)
        if connector_type_hint == 'aws_s3':
            ak = config_data.get('aws_access_key_id') or config_data.get('aws_access_key')
            sk = config_data.get('aws_secret_access_key') or config_data.get('aws_secret_key')
            if not ak or not sk:
                return jsonify({
                    "success": False,
                    "message": "AWS Access Key ID and Secret Access Key are required for S3. Enter them in the Configuration step."
                }), 200
            config_data = {**config_data, 'aws_access_key_id': ak, 'aws_secret_access_key': sk}
            try:
                from utils.s3_client import create_s3_client, BOTO3_AVAILABLE
                if not BOTO3_AVAILABLE:
                    return jsonify({"success": False, "message": "boto3 not installed. pip install boto3"}), 400
                s3_client = create_s3_client(config_data)
                test_result = s3_client.test_connection()
                return jsonify(test_result), 200
            except ValueError as e:
                return jsonify({"success": False, "message": str(e)}), 200
            except Exception as e:
                logger.error('FN:test_connection_config s3 error:{}'.format(str(e)), exc_info=True)
                return jsonify({"success": False, "message": "Connection test failed: {}".format(str(e))}), 200
        
        # Check if this is an Oracle connection
        if ('host' in config_data and 'service_name' in config_data) or 'jdbc_url' in config_data:
            try:
                from utils.oracle_db_client import OracleDBClient
                client = OracleDBClient(config_data)
                test_result = client.test_connection()
                client.close()
                return jsonify(test_result), 200 if test_result.get('success') else 400
            except ImportError as e:
                return jsonify({"success": False, "message": f"Oracle driver not installed: {str(e)}"}), 400
            except ValueError as e:
                return jsonify({"success": False, "message": str(e)}), 200
            except Exception as e:
                logger.error('FN:test_connection_config oracle error:{}'.format(str(e)), exc_info=True)
                return jsonify({"success": False, "message": f"Connection test failed: {str(e)}"}), 200
        
        # AWS S3 connection (access key auth)
        if config_data.get('aws_access_key_id') and config_data.get('aws_secret_access_key'):
            try:
                from utils.s3_client import create_s3_client, BOTO3_AVAILABLE
                if not BOTO3_AVAILABLE:
                    return jsonify({"success": False, "message": "boto3 not installed. pip install boto3"}), 400
                s3_client = create_s3_client(config_data)
                test_result = s3_client.test_connection()
                return jsonify(test_result), 200
            except ValueError as e:
                return jsonify({"success": False, "message": str(e)}), 200
            except Exception as e:
                logger.error('FN:test_connection_config s3 error:{}'.format(str(e)), exc_info=True)
                return jsonify({"success": False, "message": "Connection test failed: {}".format(str(e))}), 200
        
        # Azure Blob connection
        if not AZURE_AVAILABLE:
            return jsonify({"error": "Azure utilities not available"}), 503

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


@connections_bp.route('/api/connections/<int:connection_id>/test', methods=['POST'])
@handle_error
def test_connection(connection_id):
    """Test an existing connection"""
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
                    airflow_base_url = current_app.config.get("AIRFLOW_BASE_URL")
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
                    
                    default_airflow_home = os.path.join(os.path.dirname(os.path.dirname(__file__)), "airflow")
                    airflow_home = current_app.config.get("AIRFLOW_HOME", default_airflow_home)
                    airflow_bin = os.path.join(airflow_home, "venv", "bin", "airflow")
                    env = os.environ.copy()
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
                        import importlib.util
                        discovery_runner_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts', 'discovery_runner.py')
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
            import importlib.util
            discovery_runner_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'discovery_runner.py')
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


@connections_bp.route('/api/connections/<int:connection_id>/containers', methods=['GET'])
@handle_error
def list_containers(connection_id):
    """List containers (Azure) or buckets (S3) for a connection"""
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        config_data = connection.config or {}
        
        if connection.connector_type == 'aws_s3':
            try:
                from utils.s3_client import create_s3_client, BOTO3_AVAILABLE
                if not BOTO3_AVAILABLE:
                    return jsonify({"error": "boto3 not installed. pip install boto3"}), 503
                s3_client = create_s3_client(config_data)
                buckets = s3_client.list_buckets()
                return jsonify({
                    "containers": buckets,
                    "file_shares": [],
                    "queues": [],
                    "tables": []
                }), 200
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
            except Exception as e:
                return jsonify({"error": str(e)}), 400
        
        if connection.connector_type != 'azure_blob':
            return jsonify({"error": "This endpoint is only for Azure Blob or AWS S3 connections"}), 400
        
        if not AZURE_AVAILABLE:
            return jsonify({"error": "Azure utilities not available"}), 503
        
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


@connections_bp.route('/api/connections/<int:connection_id>/discover-stream', methods=['POST'])
@handle_error
def discover_assets_stream(connection_id):
    """Stream discovery progress using Server-Sent Events (SSE)"""
    request_data = request.json or {}
    db = SessionLocal()
    try:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        connector_type = connection.connector_type
        config_data = connection.config or {}
        connection_name = connection.name
    finally:
        db.close()
    
    def generate():
        import json
        try:
            # ORACLE STREAMING
            if connector_type == 'oracle_db':
                import threading
                if "include_lineage" not in request_data:
                    request_data["include_lineage"] = False

                result_holder = {"result": None, "error": None}

                def _run():
                    try:
                        with current_app.app_context():
                            result_holder["result"] = discover_oracle_assets(
                                connection_id=connection_id,
                                connection_name=connection_name,
                                config_data=config_data,
                                request_data=request_data,
                            )
                    except Exception as e:
                        result_holder["error"] = str(e)

                t = threading.Thread(target=_run, daemon=True)
                t.start()

                last_msg = None
                last_status = None

                while t.is_alive():
                    try:
                        with DISCOVERY_PROGRESS_LOCK:
                            p = DISCOVERY_PROGRESS.get(connection_id, {}) if isinstance(DISCOVERY_PROGRESS.get(connection_id), dict) else {}
                        msg = p.get("message")
                        status = p.get("status")
                        if msg and msg != last_msg:
                            last_msg = msg
                            yield f"data: {json.dumps({'type': 'progress', 'message': msg, 'phase': p.get('phase'), 'percent': p.get('percent')})}\n\n"
                        if status and status != last_status:
                            last_status = status
                        import time
                        time.sleep(0.05)
                    except Exception as e:
                        yield f"data: {json.dumps({'type': 'warning', 'message': f'Progress stream warning: {str(e)}'})}\n\n"
                        import time
                        time.sleep(0.25)

                if result_holder["error"]:
                    yield f"data: {json.dumps({'type': 'error', 'message': result_holder['error']})}\n\n"
                    return

                try:
                    with DISCOVERY_PROGRESS_LOCK:
                        p = DISCOVERY_PROGRESS.get(connection_id, {}) if isinstance(DISCOVERY_PROGRESS.get(connection_id), dict) else {}
                    final_msg = p.get("message") or "Discovery complete"
                    yield f"data: {json.dumps({'type': 'complete', 'message': final_msg, 'created': p.get('created_count', 0), 'updated': p.get('updated_count', 0), 'skipped': p.get('skipped_count', 0)})}\n\n"
                except Exception:
                    yield f"data: {json.dumps({'type': 'complete', 'message': 'Discovery complete'})}\n\n"
                return

            # AWS S3 STREAMING
            if connector_type == 'aws_s3':
                try:
                    from utils.s3_client import create_s3_client, BOTO3_AVAILABLE
                    if not BOTO3_AVAILABLE:
                        yield f"data: {json.dumps({'type': 'error', 'message': 'boto3 not installed. pip install boto3'})}\n\n"
                        return
                    containers = request_data.get('containers', config_data.get('containers', []))
                    folder_path = (request_data.get('folder_path') or config_data.get('folder_path') or '').strip().rstrip('/')
                    if folder_path and not folder_path.endswith('/'):
                        folder_path = folder_path + '/'
                    yield f"data: {json.dumps({'type': 'progress', 'message': 'Authenticating with AWS S3...', 'step': 'auth'})}\n\n"
                    s3_client = create_s3_client(config_data)
                    yield f"data: {json.dumps({'type': 'progress', 'message': 'Authentication successful', 'step': 'auth_complete'})}\n\n"
                    if not containers:
                        yield f"data: {json.dumps({'type': 'progress', 'message': 'Discovering buckets...', 'step': 'containers'})}\n\n"
                        buckets = s3_client.list_buckets()
                        containers = [b['name'] for b in buckets]
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'Found {len(containers)} bucket(s)', 'containers': containers})}\n\n"
                    if not containers:
                        yield f"data: {json.dumps({'type': 'error', 'message': 'No buckets found'})}\n\n"
                        return
                    total_discovered = 0
                    total_updated = 0
                    total_skipped = 0
                    for container_idx, bucket_name in enumerate(containers):
                        yield f"data: {json.dumps({'type': 'container', 'container': bucket_name, 'message': f'Processing bucket {container_idx + 1}/{len(containers)}: {bucket_name}', 'container_index': container_idx + 1, 'total_containers': len(containers)})}\n\n"
                        try:
                            path_display = folder_path if folder_path else "root"
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'Listing objects in {bucket_name} (prefix: {path_display})...', 'container': bucket_name})}\n\n"
                            objs = s3_client.list_objects(bucket_name=bucket_name, prefix=folder_path)
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'Found {len(objs)} objects in {bucket_name}', 'file_count': len(objs), 'container': bucket_name})}\n\n"
                            if len(objs) > 0:
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'Processing {len(objs)} objects from {bucket_name}...', 'step': 'processing', 'container': bucket_name})}\n\n"
                                for i, obj in enumerate(objs):
                                    name = obj.get('name', 'unknown')
                                    full_path = obj.get('full_path', name)
                                    if len(objs) < 100 or i < 20 or (i + 1) % 50 == 0:
                                        yield f"data: {json.dumps({'type': 'file', 'file': name, 'full_path': full_path, 'container': bucket_name, 'index': i + 1, 'total': len(objs)})}\n\n"
                                    total_discovered += 1
                                    if (i + 1) % 100 == 0 or (i + 1) == len(objs):
                                        pct = round((i + 1) / len(objs) * 100) if len(objs) > 0 else 0
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'Processed {i + 1}/{len(objs)} objects in {bucket_name} ({pct}%)', 'processed': i + 1, 'total': len(objs), 'percentage': pct, 'container': bucket_name})}\n\n"
                            else:
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'No objects found in {bucket_name}', 'container': bucket_name})}\n\n"
                        except Exception as e:
                            logger.error('FN:discover_assets_stream aws_s3 bucket:{} error:{}'.format(bucket_name, str(e)), exc_info=True)
                            yield f"data: {json.dumps({'type': 'error', 'message': f'Error processing bucket {bucket_name}: {str(e)}', 'container': bucket_name})}\n\n"
                    yield f"data: {json.dumps({'type': 'complete', 'message': 'Discovery complete', 'discovered': total_discovered, 'updated': total_updated, 'skipped': total_skipped})}\n\n"
                except Exception as e:
                    logger.error('FN:discover_assets_stream aws_s3 error:{}'.format(str(e)), exc_info=True)
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                return

            # AZURE STREAMING
            if connector_type != 'azure_blob':
                yield f"data: {json.dumps({'type': 'error', 'message': f'Connector type {connector_type} does not support streaming'})}\n\n"
                return

            if not AZURE_AVAILABLE:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Azure utilities not available'})}\n\n"
                return

            containers = request_data.get('containers', config_data.get('containers', []))
            folder_path = request_data.get('folder_path', config_data.get('folder_path', ''))
            
            parsed_container = None
            parsed_path = folder_path
            parsed_account_name = None
            working_config = config_data.copy()
            
            if folder_path and (folder_path.startswith('abfs://') or folder_path.startswith('abfss://') or folder_path.startswith('https://') or folder_path.startswith('http://')):
                try:
                    from utils.storage_path_parser import parse_storage_path
                    parsed = parse_storage_path(folder_path)
                    parsed_container = parsed.get('container')
                    parsed_path = parsed.get('path', '')
                    parsed_account_name = parsed.get('account_name')
                    parsed_storage_type = parsed.get('type')
                    
                    if parsed_account_name and parsed_account_name != working_config.get('account_name'):
                        working_config['account_name'] = parsed_account_name
                    
                    if parsed_storage_type == 'azure_datalake' or folder_path.startswith(('abfs://', 'abfss://')):
                        working_config['use_dfs_endpoint'] = True
                        working_config['storage_type'] = 'datalake'
                    
                    if parsed_container:
                        containers = [parsed_container]
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'Using container from URL: {parsed_container}', 'container': parsed_container})}\n\n"
                    folder_path = parsed_path
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'warning', 'message': f'Failed to parse storage URL: {str(e)}'})}\n\n"
            
            yield f"data: {json.dumps({'type': 'progress', 'message': 'Authenticating with Azure...', 'step': 'auth'})}\n\n"
            
            try:
                from utils.azure_blob_client import create_azure_blob_client
                blob_client = create_azure_blob_client(working_config)
                yield f"data: {json.dumps({'type': 'progress', 'message': 'Authentication successful', 'step': 'auth_complete'})}\n\n"
            except ValueError as e:
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                return
            
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
            
            total_discovered = 0
            total_updated = 0
            total_skipped = 0
            
            for container_idx, container_name in enumerate(containers):
                yield f"data: {json.dumps({'type': 'container', 'container': container_name, 'message': f'Processing container {container_idx + 1}/{len(containers)}: {container_name}', 'container_index': container_idx + 1, 'total_containers': len(containers)})}\n\n"
                
                try:
                    is_datalake = working_config.get('storage_type') == 'datalake' or working_config.get('use_dfs_endpoint', False)
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
                        
                        for i, blob_info in enumerate(blobs):
                            blob_name = blob_info.get("name", "unknown")
                            full_path = blob_info.get("full_path", blob_name)
                            
                            if len(blobs) < 100 or i < 20 or (i + 1) % 50 == 0:
                                yield f"data: {json.dumps({'type': 'file', 'file': blob_name, 'full_path': full_path, 'container': container_name, 'index': i+1, 'total': len(blobs)})}\n\n"
                            
                            total_discovered += 1
                            
                            if (i + 1) % 100 == 0 or (i + 1) == len(blobs):
                                percentage = round((i+1)/len(blobs)*100) if len(blobs) > 0 else 0
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'Processed {i+1}/{len(blobs)} files in {container_name} ({percentage}%)', 'processed': i+1, 'total': len(blobs), 'percentage': percentage, 'container': container_name})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'No files found in {container_name}', 'container': container_name})}\n\n"
                    
                except Exception as e:
                    import traceback
                    error_details = str(e)
                    error_trace = traceback.format_exc()
                    logger.error('FN:discover_assets_stream container:{} path:{} error:{} traceback:{}'.format(
                        container_name, folder_path, error_details, error_trace
                    ))
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Error processing container {container_name}: {error_details}', 'container': container_name})}\n\n"
                    continue
            
            yield f"data: {json.dumps({'type': 'complete', 'message': 'Discovery complete', 'discovered': total_discovered, 'updated': total_updated, 'skipped': total_skipped})}\n\n"
            
        except Exception as e:
            error_msg = str(e)
            logger.error('FN:discover_assets_stream error:{}'.format(error_msg))
            yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
    })


@connections_bp.route('/api/connections/<int:connection_id>/discover-progress', methods=['GET'])
def get_discover_progress(connection_id):
    """Return best-effort progress for the current /discover run (polled by frontend)."""
    with DISCOVERY_PROGRESS_LOCK:
        progress = DISCOVERY_PROGRESS.get(connection_id)
        if not progress:
            return jsonify({"status": "idle"}), 200
        return jsonify(progress), 200


@connections_bp.route('/api/connections/<int:connection_id>/discover', methods=['POST'])
@handle_error
def discover_assets_route(connection_id):
    """Discover assets for a connection"""
    # Get connection info quickly, then close connection immediately
    with get_db_session() as db:
        connection = db.query(Connection).filter(Connection.id == connection_id).first()
        if not connection:
            return jsonify({"error": "Connection not found"}), 404
        
        config_data = connection.config or {}
        connection_name = connection.name
        connector_type = connection.connector_type
    
    # Handle Oracle DB discovery
    if connector_type == 'oracle_db':
        return discover_oracle_assets(connection_id, connection_name, config_data, request.json or {})
    
    # AWS S3 discovery
    if connector_type == 'aws_s3':
        return discover_s3_assets(connection_id, connection_name, config_data, request.json or {})
    
    # Azure Blob discovery
    if connector_type != 'azure_blob':
        return jsonify({"error": f"Connector type {connector_type} not supported"}), 400
    
    if not AZURE_AVAILABLE:
        return jsonify({"error": "Azure utilities not available"}), 503
    
    return discover_assets(connection_id)


@connections_bp.route('/api/connections/<int:connection_id>/extract-lineage', methods=['POST'])
@handle_error
def extract_oracle_lineage(connection_id):
    """Extract comprehensive Oracle lineage using advanced methods. Runs in background."""
    try:
        db = SessionLocal()
        try:
            connection = db.query(Connection).filter(Connection.id == connection_id).first()
            if not connection:
                return jsonify({"error": "Connection not found"}), 404
            
            if connection.connector_type != 'oracle_db':
                return jsonify({"error": "This endpoint is only for Oracle DB connections"}), 400
            
            config_data = connection.config.copy() if connection.config else {}
            connection_name = connection.name
            connector_id = f"oracle_db_{connection_name}"

            job_key = f"oracle:{connection_id}"
            if not try_start_lineage_job(job_key):
                return jsonify({
                    "success": True,
                    "message": "Oracle lineage extraction already running",
                    "status": "already_running"
                }), 202
            
            def _extract_in_background():
                try:
                    from utils.oracle_db_client import OracleDBClient
                    from utils.oracle_lineage_extractor import OracleLineageExtractor
                    from models import Asset, LineageRelationship
                    from sqlalchemy.dialects.mysql import insert as mysql_insert
                    
                    db_bg = SessionLocal()
                    try:
                        oracle_assets = db_bg.query(Asset).filter(
                            Asset.connector_id == connector_id
                        ).all()
                        
                        if not oracle_assets:
                            logger.info(f'FN:extract_oracle_lineage connection_id:{connection_id} no_assets_found')
                            return
                        
                        asset_map = {asset.id: asset for asset in oracle_assets}
                        schemas = set(asset.catalog for asset in oracle_assets if asset.catalog)
                        
                        if not schemas:
                            logger.info(f'FN:extract_oracle_lineage connection_id:{connection_id} no_schemas_found')
                            return
                        
                        client = OracleDBClient(config_data)
                        lineage_extractor = OracleLineageExtractor(client)
                        
                        all_lineage = []
                        for schema in schemas:
                            logger.info(f'FN:extract_oracle_lineage extracting schema:{schema}')
                            
                            try:
                                sql_lineage = lineage_extractor._extract_sql_column_lineage(schema, connector_id, asset_map)
                                all_lineage.extend(sql_lineage)
                                logger.info(f'FN:extract_oracle_lineage sql_lineage schema:{schema} found:{len(sql_lineage)} relationships')
                            except Exception as e:
                                logger.warning(f'FN:extract_oracle_lineage sql_lineage_error schema:{schema} error:{str(e)}')
                            
                            try:
                                folder_lineage = lineage_extractor._extract_folder_hierarchy_lineage(schema, connector_id, asset_map)
                                all_lineage.extend(folder_lineage)
                                logger.info(f'FN:extract_oracle_lineage folder_hierarchy schema:{schema} found:{len(folder_lineage)} relationships')
                            except Exception as e:
                                logger.warning(f'FN:extract_oracle_lineage folder_hierarchy_error schema:{schema} error:{str(e)}')
                        
                        deduplicated = lineage_extractor._deduplicate_lineage(all_lineage)
                        
                        if deduplicated:
                            LINEAGE_BATCH_SIZE = 500
                            created_count = 0
                            
                            for i in range(0, len(deduplicated), LINEAGE_BATCH_SIZE):
                                chunk = deduplicated[i:i + LINEAGE_BATCH_SIZE]
                                try:
                                    stmt = mysql_insert(LineageRelationship).values(chunk).prefix_with("IGNORE")
                                    db_bg.execute(stmt)
                                    db_bg.commit()
                                    created_count += len(chunk)
                                except Exception as e:
                                    db_bg.rollback()
                                    logger.warning(f'FN:extract_oracle_lineage batch_failed batch:{i//LINEAGE_BATCH_SIZE+1} error:{str(e)}')
                            
                            logger.info(f'FN:extract_oracle_lineage connection_id:{connection_id} created:{created_count} relationships')
                        
                        client.close()
                    finally:
                        db_bg.close()
                except Exception as e:
                    logger.error(f'FN:extract_oracle_lineage background_error:{str(e)}', exc_info=True)
                finally:
                    finish_lineage_job(job_key)
            
            thread = threading.Thread(target=_extract_in_background, daemon=True)
            thread.start()
            
            return jsonify({
                "success": True,
                "message": "Oracle lineage extraction started in background",
                "status": "started"
            }), 202
            
        finally:
            db.close()
    except Exception as e:
        logger.error(f'FN:extract_oracle_lineage error:{str(e)}', exc_info=True)
        return jsonify({"error": str(e)}), 500


@connections_bp.route('/api/connections/<int:connection_id>/extract-azure-lineage', methods=['POST'])
@handle_error
def extract_azure_blob_lineage(connection_id):
    """Extract comprehensive Azure Blob Storage lineage using advanced methods. Runs in background."""
    try:
        db = SessionLocal()
        try:
            connection = db.query(Connection).filter(Connection.id == connection_id).first()
            if not connection:
                return jsonify({"error": "Connection not found"}), 404
            
            if connection.connector_type != 'azure_blob':
                return jsonify({"error": "This endpoint is only for Azure Blob Storage connections"}), 400
            
            connection_name = connection.name
            connector_id = f"azure_blob_{connection_name}"

            job_key = f"azure:{connection_id}"
            if not try_start_lineage_job(job_key):
                return jsonify({
                    "success": True,
                    "message": "Azure Blob lineage extraction already running",
                    "status": "already_running"
                }), 202
            
            def _extract_in_background():
                try:
                    from utils.azure_blob_lineage_extractor import AzureBlobLineageExtractor
                    from models import Asset, LineageRelationship
                    from sqlalchemy.dialects.mysql import insert as mysql_insert
                    
                    db_bg = SessionLocal()
                    try:
                        azure_assets = db_bg.query(Asset).filter(
                            Asset.connector_id == connector_id
                        ).all()
                        
                        if not azure_assets:
                            logger.info(f'FN:extract_azure_blob_lineage connection_id:{connection_id} no_assets_found')
                            return
                        
                        asset_map = {asset.id: asset for asset in azure_assets}
                        lineage_extractor = AzureBlobLineageExtractor()
                        
                        all_lineage = []
                        try:
                            folder_lineage = lineage_extractor._extract_folder_hierarchy_lineage(connector_id, asset_map)
                            all_lineage.extend(folder_lineage)
                            logger.info(f'FN:extract_azure_blob_lineage folder_hierarchy found:{len(folder_lineage)} relationships')
                        except Exception as e:
                            logger.warning(f'FN:extract_azure_blob_lineage folder_hierarchy_error error:{str(e)}')
                        
                        try:
                            ml_lineage = lineage_extractor._extract_ml_inferred_lineage(connector_id, asset_map)
                            all_lineage.extend(ml_lineage)
                            logger.info(f'FN:extract_azure_blob_lineage ml_inference found:{len(ml_lineage)} relationships')
                        except Exception as e:
                            logger.warning(f'FN:extract_azure_blob_lineage ml_inference_error error:{str(e)}')
                        
                        seen = set()
                        deduplicated = []
                        for rel in all_lineage:
                            key = (rel.get('source_asset_id'), rel.get('target_asset_id'), rel.get('source_job_id'))
                            if key not in seen:
                                seen.add(key)
                                deduplicated.append(rel)
                        
                        if deduplicated:
                            LINEAGE_BATCH_SIZE = 500
                            for i in range(0, len(deduplicated), LINEAGE_BATCH_SIZE):
                                chunk = deduplicated[i:i + LINEAGE_BATCH_SIZE]
                                try:
                                    stmt = mysql_insert(LineageRelationship).values(chunk).prefix_with("IGNORE")
                                    db_bg.execute(stmt)
                                    db_bg.commit()
                                except Exception as e:
                                    db_bg.rollback()
                                    logger.warning(f'FN:extract_azure_blob_lineage batch_failed batch:{i//LINEAGE_BATCH_SIZE+1} error:{str(e)}')
                            
                            logger.info(f'FN:extract_azure_blob_lineage connection_id:{connection_id} created:{len(deduplicated)} relationships')
                    finally:
                        db_bg.close()
                except Exception as e:
                    logger.error(f'FN:extract_azure_blob_lineage background_error:{str(e)}', exc_info=True)
                finally:
                    finish_lineage_job(job_key)
            
            thread = threading.Thread(target=_extract_in_background, daemon=True)
            thread.start()
            
            return jsonify({
                "success": True,
                "message": "Azure Blob lineage extraction started in background",
                "status": "started"
            }), 202
            
        finally:
            db.close()
    except Exception as e:
        logger.error(f'FN:extract_azure_blob_lineage error:{str(e)}', exc_info=True)
        return jsonify({"error": str(e)}), 500


@connections_bp.route('/api/connections/<int:connection_id>/extract-s3-lineage', methods=['POST'])
@handle_error
def extract_s3_lineage(connection_id):
    """Extract comprehensive AWS S3 lineage (folder hierarchy + ML inference). Runs in background."""
    try:
        db = SessionLocal()
        try:
            connection = db.query(Connection).filter(Connection.id == connection_id).first()
            if not connection:
                return jsonify({"error": "Connection not found"}), 404

            if connection.connector_type != 'aws_s3':
                return jsonify({"error": "This endpoint is only for AWS S3 connections"}), 400

            connection_name = connection.name
            connector_id = "aws_s3_{}".format(connection_name)

            job_key = "s3:{}".format(connection_id)
            if not try_start_lineage_job(job_key):
                return jsonify({
                    "success": True,
                    "message": "S3 lineage extraction already running",
                    "status": "already_running"
                }), 202

            def _extract_in_background():
                try:
                    from utils.s3_lineage_extractor import S3LineageExtractor
                    from models import Asset, LineageRelationship
                    from sqlalchemy.dialects.mysql import insert as mysql_insert

                    db_bg = SessionLocal()
                    try:
                        s3_assets = db_bg.query(Asset).filter(
                            Asset.connector_id == connector_id
                        ).all()

                        if not s3_assets:
                            logger.info(
                                "FN:extract_s3_lineage connection_id:{} no_assets_found".format(
                                    connection_id
                                )
                            )
                            return

                        asset_map = {a.id: a for a in s3_assets}
                        extractor = S3LineageExtractor()

                        all_lineage = []
                        try:
                            folder_lineage = extractor._extract_folder_hierarchy_lineage(
                                connector_id, asset_map
                            )
                            all_lineage.extend(folder_lineage)
                            logger.info(
                                "FN:extract_s3_lineage folder_hierarchy found:{} relationships".format(
                                    len(folder_lineage)
                                )
                            )
                        except Exception as e:
                            logger.warning(
                                "FN:extract_s3_lineage folder_hierarchy_error error:{}".format(
                                    str(e)
                                )
                            )

                        try:
                            ml_lineage = extractor._extract_ml_inferred_lineage(
                                connector_id, asset_map
                            )
                            all_lineage.extend(ml_lineage)
                            logger.info(
                                "FN:extract_s3_lineage ml_inference found:{} relationships".format(
                                    len(ml_lineage)
                                )
                            )
                        except Exception as e:
                            logger.warning(
                                "FN:extract_s3_lineage ml_inference_error error:{}".format(
                                    str(e)
                                )
                            )

                        seen = set()
                        deduplicated = []
                        for rel in all_lineage:
                            key = (
                                rel.get("source_asset_id"),
                                rel.get("target_asset_id"),
                                rel.get("source_job_id"),
                            )
                            if key not in seen:
                                seen.add(key)
                                deduplicated.append(rel)

                        if deduplicated:
                            LINEAGE_BATCH_SIZE = 500
                            for i in range(0, len(deduplicated), LINEAGE_BATCH_SIZE):
                                chunk = deduplicated[i : i + LINEAGE_BATCH_SIZE]
                                try:
                                    stmt = mysql_insert(LineageRelationship).values(
                                        chunk
                                    ).prefix_with("IGNORE")
                                    db_bg.execute(stmt)
                                    db_bg.commit()
                                except Exception as e:
                                    db_bg.rollback()
                                    logger.warning(
                                        "FN:extract_s3_lineage batch_failed batch:{} error:{}".format(
                                            i // LINEAGE_BATCH_SIZE + 1, str(e)
                                        )
                                    )

                            logger.info(
                                "FN:extract_s3_lineage connection_id:{} created:{} relationships".format(
                                    connection_id, len(deduplicated)
                                )
                            )
                    finally:
                        db_bg.close()
                except Exception as e:
                    logger.error(
                        "FN:extract_s3_lineage background_error:{}".format(str(e)),
                        exc_info=True,
                    )
                finally:
                    finish_lineage_job(job_key)

            thread = threading.Thread(target=_extract_in_background, daemon=True)
            thread.start()

            return jsonify({
                "success": True,
                "message": "S3 lineage extraction started in background",
                "status": "started"
            }), 202

        finally:
            db.close()
    except Exception as e:
        logger.error("FN:extract_s3_lineage error:{}".format(str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 500

