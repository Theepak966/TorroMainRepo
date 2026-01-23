"""
Discovery management routes.
Production-level route handlers for data discovery operations.
"""

import os
import sys
import logging
import threading
import uuid
from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import func
from sqlalchemy.orm import joinedload, load_only

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import Asset, DataDiscovery, DeduplicationJob
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
                
                # Merge asset columns (with updated description, PII, masking logic) into schema_json
                if not result["schema_json"] or not isinstance(result["schema_json"], dict):
                    result["schema_json"] = {"columns": [], "num_columns": 0}
                
                schema_json = result["schema_json"]
                schema_columns = schema_json.get("columns", [])
                
                # Create a map of asset columns by name for quick lookup
                asset_columns_map = {col.get('name'): col for col in columns if col.get('name')}
                
                # Merge asset column data (description, PII, masking logic) into schema_json columns
                merged_columns = []
                for schema_col in schema_columns:
                    col_name = schema_col.get('name')
                    if col_name and col_name in asset_columns_map:
                        # Merge asset column data into schema column
                        asset_col = asset_columns_map[col_name]
                        merged_col = dict(schema_col)  # Start with schema column
                        # Update with asset column data (description, PII, masking logic)
                        if 'description' in asset_col:
                            merged_col['description'] = asset_col['description']
                        if 'pii_detected' in asset_col:
                            merged_col['pii_detected'] = asset_col['pii_detected']
                        if 'pii_types' in asset_col:
                            merged_col['pii_types'] = asset_col['pii_types']
                        if 'masking_logic_analytical' in asset_col:
                            merged_col['masking_logic_analytical'] = asset_col['masking_logic_analytical']
                        if 'masking_logic_operational' in asset_col:
                            merged_col['masking_logic_operational'] = asset_col['masking_logic_operational']
                        merged_columns.append(merged_col)
                    else:
                        # Keep schema column as-is if not found in asset columns
                        merged_columns.append(schema_col)
                
                # Add any asset columns that aren't in schema_json
                schema_col_names = {col.get('name') for col in schema_columns if col.get('name')}
                for asset_col in columns:
                    if asset_col.get('name') and asset_col.get('name') not in schema_col_names:
                        merged_columns.append(asset_col)
                
                schema_json["columns"] = merged_columns
                schema_json["num_columns"] = len(merged_columns)
                result["schema_json"] = schema_json
                
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


def _run_deduplication_worker(job_id: int):
    """
    Background worker for deduplication. Handles streaming reads and chunked updates.
    Scales to 100k+ files without loading everything into memory.
    """
    import time
    db = SessionLocal()
    try:
        # Get job
        job = db.query(DeduplicationJob).filter(DeduplicationJob.id == job_id).first()
        if not job:
            logger.error('FN:_run_deduplication_worker job_id:{} not_found'.format(job_id))
            return
        
        job.status = 'running'
        job.started_at = datetime.utcnow()
        db.commit()
        
        start_time = time.time()
        
        # Helper functions
        def parse_last_modified(d: DataDiscovery):
            file_metadata = d.file_metadata or {}
            timestamps = (file_metadata.get("timestamps") or {}) if isinstance(file_metadata, dict) else {}
            storage_metadata = d.storage_metadata or {}
            azure = (storage_metadata.get("azure") or {}) if isinstance(storage_metadata, dict) else {}
            
            raw = timestamps.get("last_modified") or azure.get("last_modified")
            dt = None
            
            if raw:
                try:
                    if isinstance(raw, str):
                        if raw.endswith('Z'):
                            raw = raw.replace('Z', '+00:00')
                        if 'T' in raw:
                            dt = datetime.fromisoformat(raw)
                        else:
                            dt = datetime.strptime(raw, '%Y-%m-%d')
                    elif hasattr(raw, 'tzinfo') or hasattr(raw, 'strftime'):
                        dt = raw
                except Exception:
                    dt = None
            
            if isinstance(dt, datetime) and dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            
            return dt or d.updated_at or d.discovered_at or datetime.min
        
        def schema_key(asset: Asset):
            cols = normalize_columns(asset.columns or [])
            names = []
            for c in cols:
                if not isinstance(c, dict):
                    continue
                n = c.get("name")
                if not n:
                    continue
                names.append(str(n).strip().lower())
            return tuple(sorted(set(names)))
        
        # STREAMING READ: Process in batches to avoid loading 100k rows at once
        BATCH_SIZE = 5000
        groups = {}  # schema_key -> [(discovery_id, last_modified), ...]
        total_discoveries = 0
        offset = 0
        
        logger.info('FN:_run_deduplication_worker job_id:{} starting_streaming_read batch_size:{}'.format(
            job_id, BATCH_SIZE
        ))
        
        while True:
            # Fetch batch
            batch = (
                db.query(DataDiscovery)
                .options(joinedload(DataDiscovery.asset).load_only(Asset.columns))
                .filter(
                    DataDiscovery.is_visible.is_(True),
                    DataDiscovery.asset_id.isnot(None),
                )
                .order_by(DataDiscovery.id)
                .limit(BATCH_SIZE)
                .offset(offset)
                .all()
            )
            
            if not batch:
                break
            
            # Process batch: build schema groups (only store id + timestamp, not full objects)
            for d in batch:
                if not d.asset:
                    continue
                key = schema_key(d.asset)
                if not key:
                    continue
                
                last_mod = parse_last_modified(d)
                if key not in groups:
                    groups[key] = []
                groups[key].append((d.id, last_mod))
                total_discoveries += 1
            
            offset += BATCH_SIZE
            
            # Update progress
            job.total_discoveries = total_discoveries
            # Estimate progress (we don't know total until done, so use a rough estimate)
            db.commit()
            
            logger.debug('FN:_run_deduplication_worker job_id:{} processed_batch offset:{} discoveries:{}'.format(
                job_id, offset, total_discoveries
            ))
        
        job.total_discoveries = total_discoveries
        db.commit()
        
        # Process groups: find duplicates and prepare updates
        updates_list = []
        hidden = 0
        groups_deduped = 0
        
        for key, items in groups.items():
            if len(items) <= 1:
                continue
            
            groups_deduped += 1
            # Sort by last_modified descending (latest first)
            items_sorted = sorted(items, key=lambda x: x[1], reverse=True)
            
            # Keep first (latest), hide rest
            for discovery_id, _ in items_sorted[1:]:
                updates_list.append({
                    'id': discovery_id,
                    'is_visible': False
                })
                hidden += 1
        
        # CHUNKED BULK UPDATES: Update in chunks to avoid huge transactions
        UPDATE_CHUNK_SIZE = 2000
        total_updates = len(updates_list)
        
        logger.info('FN:_run_deduplication_worker job_id:{} applying_updates total:{} chunk_size:{}'.format(
            job_id, total_updates, UPDATE_CHUNK_SIZE
        ))
        
        for i in range(0, total_updates, UPDATE_CHUNK_SIZE):
            chunk = updates_list[i:i + UPDATE_CHUNK_SIZE]
            db.bulk_update_mappings(DataDiscovery, chunk)
            db.commit()
            
            # Update progress
            job.hidden_count = min(i + len(chunk), total_updates)
            job.progress_percent = min(100.0, (job.hidden_count / total_updates * 100) if total_updates > 0 else 100.0)
            db.commit()
            
            logger.debug('FN:_run_deduplication_worker job_id:{} update_chunk progress:{}/{}'.format(
                job_id, job.hidden_count, total_updates
            ))
        
        # Finalize job
        total_time = time.time() - start_time
        job.status = 'completed'
        job.groups_deduped = groups_deduped
        job.hidden_count = hidden
        job.progress_percent = 100.0
        job.completed_at = datetime.utcnow()
        db.commit()
        
        logger.info('FN:_run_deduplication_worker job_id:{} completed total_time:{:.2f}s total_discoveries:{} groups_deduped:{} hidden:{}'.format(
            job_id, total_time, total_discoveries, groups_deduped, hidden
        ))
        
    except Exception as e:
        db.rollback()
        try:
            job = db.query(DeduplicationJob).filter(DeduplicationJob.id == job_id).first()
            if job:
                job.status = 'failed'
                job.error_message = str(e)[:1000]  # Truncate long errors
                job.completed_at = datetime.utcnow()
                db.commit()
        except:
            pass
        logger.error('FN:_run_deduplication_worker job_id:{} error:{}'.format(job_id, str(e)), exc_info=True)
    finally:
        db.close()


@discovery_bp.route('/api/discovery/deduplicate', methods=['POST'])
@handle_error
def deduplicate_discoveries_by_schema():
    """
    Hide duplicate discovered assets by schema (column names).
    
    For small datasets (<5000): Runs synchronously (fast response).
    For large datasets (>=5000): Runs asynchronously (returns job_id, poll for status).
    
    SCALABLE to 100k+ files:
    - Streaming/batched reads (processes in 5k chunks)
    - Chunked bulk updates (2k per transaction)
    - Background processing for large datasets
    """
    db = SessionLocal()
    try:
        # Check total visible discoveries to decide sync vs async
        total_count = (
            db.query(DataDiscovery)
            .filter(
                DataDiscovery.is_visible.is_(True),
                DataDiscovery.asset_id.isnot(None),
            )
            .count()
        )
        
        # For small datasets (<5000), run synchronously (fast path)
        if total_count < 5000:
            # Use original fast path logic (simplified version)
            import time
            start_time = time.time()
            
            discoveries = (
                db.query(DataDiscovery)
                .options(joinedload(DataDiscovery.asset).load_only(Asset.columns))
                .filter(
                    DataDiscovery.is_visible.is_(True),
                    DataDiscovery.asset_id.isnot(None),
                )
                .all()
            )
            
            def parse_last_modified(d: DataDiscovery):
                file_metadata = d.file_metadata or {}
                timestamps = (file_metadata.get("timestamps") or {}) if isinstance(file_metadata, dict) else {}
                storage_metadata = d.storage_metadata or {}
                azure = (storage_metadata.get("azure") or {}) if isinstance(storage_metadata, dict) else {}
                raw = timestamps.get("last_modified") or azure.get("last_modified")
                dt = None
                if raw:
                    try:
                        if isinstance(raw, str):
                            if raw.endswith('Z'):
                                raw = raw.replace('Z', '+00:00')
                            if 'T' in raw:
                                dt = datetime.fromisoformat(raw)
                            else:
                                dt = datetime.strptime(raw, '%Y-%m-%d')
                        elif hasattr(raw, 'tzinfo') or hasattr(raw, 'strftime'):
                            dt = raw
                    except Exception:
                        dt = None
                if isinstance(dt, datetime) and dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt or d.updated_at or d.discovered_at or datetime.min
            
            def schema_key(asset: Asset):
                cols = normalize_columns(asset.columns or [])
                names = []
                for c in cols:
                    if not isinstance(c, dict):
                        continue
                    n = c.get("name")
                    if not n:
                        continue
                    names.append(str(n).strip().lower())
                return tuple(sorted(set(names)))
            
            groups = {}
            for d in discoveries:
                if not d.asset:
                    continue
                key = schema_key(d.asset)
                if not key:
                    continue
                groups.setdefault(key, []).append((d.id, parse_last_modified(d)))
            
            updates_list = []
            hidden = 0
            groups_deduped = 0
            
            for _, items in groups.items():
                if len(items) <= 1:
                    continue
                groups_deduped += 1
                items_sorted = sorted(items, key=lambda x: x[1], reverse=True)
                for discovery_id, _ in items_sorted[1:]:
                    updates_list.append({'id': discovery_id, 'is_visible': False})
                    hidden += 1
            
            if updates_list:
                # Chunk updates even for small datasets
                CHUNK_SIZE = 2000
                for i in range(0, len(updates_list), CHUNK_SIZE):
                    chunk = updates_list[i:i + CHUNK_SIZE]
                    db.bulk_update_mappings(DataDiscovery, chunk)
                db.commit()
            
            total_time = time.time() - start_time
            
            logger.info('FN:deduplicate_discoveries_by_schema sync_mode total_time:{:.2f}s total_discoveries:{} groups_deduped:{} hidden:{}'.format(
                total_time, len(discoveries), groups_deduped, hidden
            ))
            
            return jsonify({
                "success": True,
                "job_id": None,  # No job for sync mode
                "groups_deduped": groups_deduped,
                "hidden": hidden,
                "total_processed": len(discoveries),
                "processing_time_seconds": round(total_time, 2)
            }), 200
        
        # For large datasets (>=5000), run asynchronously
        job = DeduplicationJob(
            status='queued',
            total_discoveries=total_count
        )
        db.add(job)
        db.commit()
        job_id = job.id
        
        # Start background thread
        thread = threading.Thread(target=_run_deduplication_worker, args=(job_id,), daemon=True)
        thread.start()
        
        logger.info('FN:deduplicate_discoveries_by_schema async_mode job_id:{} total_discoveries:{}'.format(
            job_id, total_count
        ))
        
        return jsonify({
            "success": True,
            "job_id": job_id,
            "status": "queued",
            "message": "Deduplication started. Use /api/discovery/deduplicate/status/<job_id> to check progress.",
            "total_discoveries": total_count
        }), 202  # 202 Accepted for async operations
        
    except Exception as e:
        db.rollback()
        logger.error('FN:deduplicate_discoveries_by_schema error:{}'.format(str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()


@discovery_bp.route('/api/discovery/deduplicate/status/<int:job_id>', methods=['GET'])
@handle_error
def get_deduplication_status(job_id: int):
    """Get status of a deduplication job."""
    db = SessionLocal()
    try:
        job = db.query(DeduplicationJob).filter(DeduplicationJob.id == job_id).first()
        if not job:
            return jsonify({"error": "Job not found"}), 404
        
        return jsonify({
            "job_id": job.id,
            "status": job.status,
            "total_discoveries": job.total_discoveries,
            "groups_deduped": job.groups_deduped,
            "hidden_count": job.hidden_count,
            "progress_percent": float(job.progress_percent) if job.progress_percent else 0.0,
            "error_message": job.error_message,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "created_at": job.created_at.isoformat() if job.created_at else None,
        }), 200
    except Exception as e:
        logger.error('FN:get_deduplication_status job_id:{} error:{}'.format(job_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()


@discovery_bp.route('/api/discovery/duplicates/hidden', methods=['GET'])
@handle_error
def list_hidden_duplicates():
    """List hidden discoveries so users can review/restore them. Supports pagination."""
    db = SessionLocal()
    try:
        # Pagination parameters
        page = request.args.get('page', type=int, default=1)
        per_page = request.args.get('per_page', type=int, default=50)
        
        # Validate pagination
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 1
        if per_page > 500:
            per_page = 500
        
        offset = (page - 1) * per_page

        # Get total count for pagination
        total_count = (
            db.query(DataDiscovery)
            .filter(
                DataDiscovery.is_visible.is_(False),
                DataDiscovery.asset_id.isnot(None),
            )
            .count()
        )

        # Get paginated results
        hidden = (
            db.query(DataDiscovery)
            .options(joinedload(DataDiscovery.asset).load_only(Asset.columns))
            .filter(
                DataDiscovery.is_visible.is_(False),
                DataDiscovery.asset_id.isnot(None),
            )
            .order_by(DataDiscovery.updated_at.desc(), DataDiscovery.id.desc())
            .limit(per_page)
            .offset(offset)
            .all()
        )

        def parse_last_modified(d: DataDiscovery):
            file_metadata = d.file_metadata or {}
            timestamps = (file_metadata.get("timestamps") or {}) if isinstance(file_metadata, dict) else {}
            storage_metadata = d.storage_metadata or {}
            azure = (storage_metadata.get("azure") or {}) if isinstance(storage_metadata, dict) else {}

            raw = timestamps.get("last_modified") or azure.get("last_modified")
            dt = None
            if raw:
                try:
                    if isinstance(raw, str):
                        if raw.endswith('Z'):
                            raw = raw.replace('Z', '+00:00')
                        if 'T' in raw:
                            dt = datetime.fromisoformat(raw)
                        else:
                            dt = datetime.strptime(raw, '%Y-%m-%d')
                    elif hasattr(raw, 'tzinfo') or hasattr(raw, 'strftime'):
                        dt = raw
                except Exception:
                    dt = None
            if isinstance(dt, datetime) and dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt

        results = []
        for d in hidden:
            storage_location = d.storage_location or {}
            file_metadata = d.file_metadata or {}
            basic = (file_metadata.get("basic") or {}) if isinstance(file_metadata, dict) else {}

            results.append({
                "discovery_id": d.id,
                "asset_id": d.asset_id,
                "asset_name": d.asset.name if d.asset else None,
                "asset_type": d.asset.type if d.asset else None,
                "schema_hash": d.schema_hash,
                "storage_path": storage_location.get("path"),
                "file_name": basic.get("name"),
                "file_last_modified": (parse_last_modified(d) or d.updated_at or d.discovered_at).isoformat() if (d.updated_at or d.discovered_at or parse_last_modified(d)) else None,
                "discovered_at": d.discovered_at.isoformat() if d.discovered_at else None,
                "updated_at": d.updated_at.isoformat() if d.updated_at else None,
            })

        total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
        
        return jsonify({
            "total": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1,
            "hidden_duplicates": results,
        }), 200
    finally:
        db.close()


@discovery_bp.route('/api/discovery/<int:discovery_id>/restore', methods=['PUT'])
@handle_error
def restore_hidden_duplicate(discovery_id: int):
    """Restore a hidden discovery back to the discovery UI."""
    db = SessionLocal()
    try:
        d = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
        if not d:
            return jsonify({"error": "Discovery record not found"}), 404

        d.is_visible = True
        db.commit()

        logger.info('FN:restore_hidden_duplicate discovery_id:{} restored:True'.format(discovery_id))
        return jsonify({"success": True, "discovery_id": discovery_id}), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:restore_hidden_duplicate discovery_id:{} error:{}'.format(discovery_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()

