"""
Discovery management routes.
Production-level route handlers for data discovery operations.
"""

import os
import sys
import logging
from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import func
from sqlalchemy.orm import joinedload

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import Asset, DataDiscovery
from utils.helpers import handle_error, normalize_columns, generate_view_sql_commands
from flask import current_app

logger = logging.getLogger(__name__)

discovery_bp = Blueprint('discovery', __name__)

@discovery_bp.route('/api/discovery/<int:discovery_id>', methods=['GET'])
@handle_error
def get_discovery_by_id(discovery_id):
    db = SessionLocal()
    try:
        # OPTIMIZATION: Eager load asset with joinedload to avoid N+1 query
        from sqlalchemy.orm import joinedload
        discovery = db.query(DataDiscovery).options(joinedload(DataDiscovery.asset)).filter(DataDiscovery.id == discovery_id).first()
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
                from datetime import timezone
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')
            return None
        
        result = {
            "id": discovery.id,
            "asset_id": discovery.asset_id,
            "additional_metadata": discovery.additional_metadata,
            "approval_status": discovery.approval_status or "pending_review",
            "approval_workflow": discovery.approval_workflow,
            "created_at": format_rfc2822(discovery.created_at),
            "created_by": discovery.created_by or "api_trigger",
            "data_publishing_id": discovery.data_publishing_id,
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
        
        if discovery.asset_id and discovery.asset:
            try:
                asset = discovery.asset
                columns = normalize_columns(asset.columns or [])
                view_sql = generate_view_sql_commands(asset, columns)
                result["view_sql_commands"] = {
                    "analytical_sql": view_sql['analytical_sql'],
                    "operational_sql": view_sql['operational_sql'],
                    "has_masked_columns": view_sql['has_masked_columns']
                }
            except Exception as e:
                logger.warning('FN:get_discovery_by_id discovery_id:{} asset_id:{} message:Failed to fetch view_sql_commands error:{}'.format(
                    discovery_id, discovery.asset_id, str(e)
                ))
        
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

@discovery_bp.route('/api/discovery', methods=['GET'])
@handle_error
def list_discoveries():
    db = SessionLocal()
    try:
        status_filter = request.args.get('status')
        approval_status_filter = request.args.get('approval_status')
        limit = request.args.get('limit', type=int, default=100)
        offset = request.args.get('offset', type=int, default=0)
        
        # Validate pagination parameters
        if limit is not None and (limit < 1 or limit > 1000):
            return jsonify({"error": "Limit must be between 1 and 1000"}), 400
        if offset is not None and offset < 0:
            return jsonify({"error": "Offset must be >= 0"}), 400
        
        # OPTIMIZATION: Use joinedload to eager load assets, avoiding N+1 queries
        from sqlalchemy.orm import joinedload
        query = db.query(DataDiscovery).options(joinedload(DataDiscovery.asset))
        
        if status_filter:
            query = query.filter(DataDiscovery.status == status_filter)
        if approval_status_filter:
            query = query.filter(DataDiscovery.approval_status == approval_status_filter)
        
        total = query.count()
        # OPTIMIZATION: Fetch discoveries with assets eagerly loaded in single query
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
            
            # OPTIMIZATION: Asset already loaded via joinedload, no additional query needed
            if discovery.asset:
                discovery_data["asset"] = {
                    "id": discovery.asset.id,
                    "name": discovery.asset.name,
                    "type": discovery.asset.type
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



@discovery_bp.route('/api/discovery/<int:discovery_id>/approve', methods=['PUT'])
@handle_error
def approve_discovery(discovery_id):
    db = SessionLocal()
    try:
        # OPTIMIZATION: Eager load asset with joinedload to avoid N+1 query
        from sqlalchemy.orm import joinedload
        discovery = db.query(DataDiscovery).options(joinedload(DataDiscovery.asset)).filter(DataDiscovery.id == discovery_id).first()
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
        
        # OPTIMIZATION: Asset already loaded via joinedload, no additional query needed
        if discovery.asset_id and discovery.asset:
            asset = discovery.asset
            if not asset.operational_metadata:
                asset.operational_metadata = {}
            asset.operational_metadata["approval_status"] = "approved"
            asset.operational_metadata["approved_at"] = approval_time.isoformat()
            asset.operational_metadata["approved_by"] = "user"
            flag_modified(asset, "operational_metadata")
        
        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh - data already in session
        
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



@discovery_bp.route('/api/discovery/<int:discovery_id>/reject', methods=['PUT'])
@handle_error
def reject_discovery(discovery_id):
    db = SessionLocal()
    try:
        # OPTIMIZATION: Eager load asset with joinedload to avoid N+1 query
        from sqlalchemy.orm import joinedload
        discovery = db.query(DataDiscovery).options(joinedload(DataDiscovery.asset)).filter(DataDiscovery.id == discovery_id).first()
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
        
        # OPTIMIZATION: Asset already loaded via joinedload, no additional query needed
        if discovery.asset_id and discovery.asset:
            asset = discovery.asset
            if not asset.operational_metadata:
                asset.operational_metadata = {}
            asset.operational_metadata["approval_status"] = "rejected"
            asset.operational_metadata["rejected_at"] = rejection_time.isoformat()
            asset.operational_metadata["rejected_by"] = "user"
            asset.operational_metadata["rejection_reason"] = reason
            flag_modified(asset, "operational_metadata")
        
        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh - data already in session
        
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



@discovery_bp.route('/api/discovery/stats', methods=['GET'])
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



@discovery_bp.route('/api/discovery/trigger', methods=['POST'])
@handle_error
def trigger_discovery():
    try:
        data = request.json or {}
        connection_id = data.get('connection_id')
        

        airflow_triggered = False
        try:
            airflow_base_url = current_app.config.get("AIRFLOW_BASE_URL")
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
            airflow_home = current_app.config.get("AIRFLOW_HOME", default_airflow_home)
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


