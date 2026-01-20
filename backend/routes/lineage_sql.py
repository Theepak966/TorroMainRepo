"""
SQL lineage parsing routes.
Production-level route handlers for SQL lineage parsing.
"""

import os
import sys
import logging
from flask import Blueprint, request, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import SQLQuery
from utils.helpers import handle_error
from utils.sql_lineage_extractor import extract_lineage_from_sql
from flask import current_app

logger = logging.getLogger(__name__)

lineage_sql_bp = Blueprint('lineage_sql', __name__)

@lineage_sql_bp.route('/api/lineage/sql/parse', methods=['POST'])
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
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to parse SQL lineage"}), 400

# OLD LINEAGE INFERENCE ENDPOINT REMOVED
# Use new lineage system at /api/lineage/process for ingestion instead



@lineage_sql_bp.route('/api/lineage/sql/parse-and-create', methods=['POST'])
@handle_error
def parse_sql_and_create_lineage():
    """
    DEPRECATED: This endpoint uses the old lineage system.
    Use /api/lineage/sql/parse-and-ingest instead for the new lineage system.
    """
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
                "message": "No target or source tables found in query",
                "deprecation_warning": "This endpoint uses the old lineage system. Use /api/lineage/sql/parse-and-ingest for the new system."
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
                "message": f"Target asset '{target_table}' not found",
                "deprecation_warning": "This endpoint uses the old lineage system. Use /api/lineage/sql/parse-and-ingest for the new system."
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
            "relationships_created": created_count,
            "deprecation_warning": "This endpoint uses the old lineage system. Use /api/lineage/sql/parse-and-ingest for the new system."
        }), 200
        
    except Exception as e:
        db.rollback()
        logger.error('FN:parse_sql_and_create_lineage error:{}'.format(str(e)), exc_info=True)
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to parse SQL and create lineage"}), 400
    finally:
        db.close()

# OLD IMPACT ANALYSIS ENDPOINT REMOVED - Use new lineage system traversal instead
# 
