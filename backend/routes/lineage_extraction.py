"""
Lineage extraction routes.
Production-level route handlers for extracting data lineage.
"""

import os
import sys
import logging
from flask import Blueprint, request, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import Connection
from utils.helpers import handle_error
from utils.shared_state import try_start_lineage_job, finish_lineage_job
from flask import current_app

logger = logging.getLogger(__name__)

lineage_extraction_bp = Blueprint('lineage_extraction', __name__)

@lineage_extraction_bp.route('/api/connections/<int:connection_id>/extract-lineage', methods=['POST'])
@handle_error
def extract_oracle_lineage(connection_id):
    """
    Extract comprehensive Oracle lineage using advanced methods.
    Runs in background - returns immediately to avoid 502 timeouts.
    """
    try:
        db = SessionLocal()
        try:
            # Get connection
            connection = db.query(Connection).filter(Connection.id == connection_id).first()
            if not connection:
                return jsonify({"error": "Connection not found"}), 404
            
            if connection.connector_type != 'oracle_db':
                return jsonify({"error": "This endpoint is only for Oracle DB connections"}), 400
            
            # Capture connection data before closing DB
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
            
            import threading
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

@lineage_extraction_bp.route('/api/connections/<int:connection_id>/extract-azure-lineage', methods=['POST'])
@handle_error
def extract_azure_blob_lineage(connection_id):
    """
    Extract comprehensive Azure Blob Storage lineage using advanced methods.
    Runs in background - returns immediately to avoid 502 timeouts.
    """
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
            
            import threading
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
                            logger.warning(f'FN:extract_azure_blob_lineage ml_inference_error error:{str(e)}', exc_info=True)
                        
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

@lineage_extraction_bp.route('/api/lineage/extract-cross-platform', methods=['POST'])
@handle_error
def extract_cross_platform_lineage():
    """
    Cross-platform lineage extraction has been disabled.
    """
    return jsonify({
        "success": False,
        "message": "Cross-platform lineage extraction has been disabled",
        "status": "disabled"
    }), 200


