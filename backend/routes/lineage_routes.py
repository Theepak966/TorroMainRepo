"""
Flask API routes for best-in-class data lineage system.
Supports process ingestion, dataset lineage queries, manual lineage, and diagram generation.
"""

from flask import Blueprint, request, jsonify
from datetime import datetime
from functools import wraps
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.lineage_traversal import LineageTraversalService
from services.lineage_ingestion import LineageIngestionService
from services.manual_lineage_service import ManualLineageService
from services.lineage_visualization import LineageDiagramGenerator
from database import SessionLocal
from models_lineage.models_lineage import ColumnLineage, LineageEdge
from models import Asset, DataDiscovery

logger = logging.getLogger(__name__)

lineage_bp = Blueprint('lineage', __name__, url_prefix='/api/lineage')

# Initialize services
traversal_service = LineageTraversalService(max_depth=3, cache_enabled=True)
ingestion_service = LineageIngestionService()
manual_lineage_service = ManualLineageService()
diagram_generator = LineageDiagramGenerator()


def require_auth(f):
    """Decorator for authentication (implement based on your auth system)"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # TODO: Implement authentication
        user_id = request.headers.get('X-User-ID', 'system')
        return f(*args, **kwargs, user_id=user_id)
    return decorated_function


@lineage_bp.route('/process', methods=['POST'])
@require_auth
def ingest_process(user_id: str):
    """
    Ingest process lineage.
    
    Payload:
    {
        "process": {
            "urn": "urn:process:spark:job_123",
            "name": "Customer Data Transformation",
            "type": "spark",
            "source_system": "spark",
            "job_id": "job_123",
            "job_name": "customer_transform",
            "process_definition": {...}
        },
        "input_datasets": ["urn:dataset:oracle:db.schema.customers"],
        "output_datasets": ["urn:dataset:azure_blob:container/path/output.parquet"],
        "column_lineage": [
            {
                "source_column": "customer_id",
                "target_column": "customer_id",
                "transformation_type": "pass_through"
            }
        ],
        "ingestion_id": "optional-idempotency-key"
    }
    """
    try:
        data = request.json
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        process_data = data.get('process')
        input_datasets = data.get('input_datasets', [])
        output_datasets = data.get('output_datasets', [])
        column_lineage = data.get('column_lineage')
        ingestion_id = data.get('ingestion_id')
        
        if not process_data:
            return jsonify({'error': 'process is required'}), 400
        if not input_datasets:
            return jsonify({'error': 'input_datasets is required'}), 400
        if not output_datasets:
            return jsonify({'error': 'output_datasets is required'}), 400
        
        result = ingestion_service.ingest_process_lineage(
            process_data=process_data,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
            column_lineage=column_lineage,
            ingestion_id=ingestion_id,
            user_id=user_id
        )
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"Process ingestion failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/dataset/<path:urn>', methods=['GET'])
def get_dataset_lineage(urn: str):
    """
    Get lineage for a dataset.
    
    Query params:
    - direction: upstream|downstream (default: downstream)
    - depth: 1-5 (default: 3)
    - as_of: ISO datetime string (optional, for temporal lineage)
    """
    try:
        direction = request.args.get('direction', 'downstream')
        depth = int(request.args.get('depth', 3))
        as_of_str = request.args.get('as_of')
        
        # Validate depth
        depth = min(max(depth, 1), 5)
        
        as_of = None
        if as_of_str:
            try:
                as_of = datetime.fromisoformat(as_of_str.replace('Z', '+00:00'))
            except ValueError:
                return jsonify({'error': 'Invalid as_of format. Use ISO 8601 format.'}), 400
        
        if direction == 'upstream':
            result = traversal_service.get_upstream_lineage(urn, depth=depth, as_of=as_of)
        else:
            result = traversal_service.get_downstream_lineage(urn, depth=depth, as_of=as_of)
        
        return jsonify({
            'dataset_urn': urn,
            'direction': direction,
            'lineage': result
        }), 200
        
    except Exception as e:
        logger.error(f"Lineage query failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/column', methods=['GET'])
def get_column_lineage():
    """
    Get column-level lineage (on-demand, lazy-loaded).
    
    Query params:
    - edge_id: Lineage edge ID
    - source_urn, target_urn: Alternative to edge_id
    """
    try:
        edge_id = request.args.get('edge_id')
        source_urn = request.args.get('source_urn')
        target_urn = request.args.get('target_urn')
        
        db = SessionLocal()
        try:
            if edge_id:
                column_lineage = db.query(ColumnLineage).filter(
                    ColumnLineage.edge_id == edge_id
                ).all()
            elif source_urn and target_urn:
                edge = db.query(LineageEdge).filter(
                    LineageEdge.source_urn == source_urn,
                    LineageEdge.target_urn == target_urn
                ).first()
                if edge:
                    column_lineage = db.query(ColumnLineage).filter(
                        ColumnLineage.edge_id == edge.id
                    ).all()
                else:
                    column_lineage = []
            else:
                return jsonify({'error': 'edge_id or (source_urn, target_urn) required'}), 400
            
            return jsonify({
                'column_lineage': [
                    {
                        'source_column': cl.source_column,
                        'target_column': cl.target_column,
                        'source_table': cl.source_table,
                        'target_table': cl.target_table,
                        'transformation_type': cl.transformation_type,
                        'transformation_expression': cl.transformation_expression
                    }
                    for cl in column_lineage
                ]
            }), 200
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Column lineage query failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/manual/schema-level', methods=['POST'])
@require_auth
def create_schema_level_lineage(user_id: str):
    """
    Create high-level schema-to-schema lineage for legacy systems.
    
    Body:
    {
        "source_schema": "legacy_schema",
        "target_schema": "new_schema",
        "source_catalog": "legacy_db",
        "target_catalog": "new_db",
        "description": "Legacy system migration"
    }
    """
    try:
        data = request.json
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        result = manual_lineage_service.create_schema_level_lineage(
            source_schema=data.get('source_schema'),
            target_schema=data.get('target_schema'),
            source_catalog=data.get('source_catalog'),
            target_catalog=data.get('target_catalog'),
            description=data.get('description'),
            user_id=user_id
        )
        
        if result.get('status') == 'error':
            return jsonify(result), 400
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"Schema-level lineage creation failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/manual/table-level', methods=['POST'])
@require_auth
def create_table_level_lineage(user_id: str):
    """
    Create table-level lineage for ETL/ELT pipelines.
    
    Body:
    {
        "source_tables": ["urn:dataset:oracle:db.schema.table1"],
        "target_tables": ["urn:dataset:oracle:db.schema.table2"],
        "process_name": "ETL Process Name",
        "column_mappings": [
            {
                "source_column": "col1",
                "target_column": "col1",
                "transformation_type": "pass_through"
            }
        ]
    }
    """
    try:
        data = request.json
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        result = manual_lineage_service.create_table_level_lineage(
            source_tables=data.get('source_tables', []),
            target_tables=data.get('target_tables', []),
            process_name=data.get('process_name', 'Manual Process'),
            column_mappings=data.get('column_mappings'),
            user_id=user_id
        )
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"Table-level lineage creation failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/manual/bulk-upload', methods=['POST'])
@require_auth
def bulk_upload_lineage(user_id: str):
    """
    Bulk upload lineage from CSV/JSON file.
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        file_format = request.form.get('format', 'csv')
        
        if file_format not in ['csv', 'json']:
            return jsonify({'error': 'format must be csv or json'}), 400
        
        result = manual_lineage_service.bulk_upload_lineage(
            file.read(),
            file_format=file_format
        )
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"Bulk upload failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/sync-discovered-assets', methods=['POST'])
def sync_discovered_assets():
    """
    Sync all discovered assets to the lineage system.
    Useful for backfilling existing assets.
    """
    try:
        from services.asset_lineage_integration import AssetLineageIntegration
        
        db = SessionLocal()
        try:
            # Get all assets with their discoveries
            assets = db.query(Asset).all()
            discoveries = db.query(DataDiscovery).all()
            
            # Create mapping
            discoveries_dict = {d.asset_id: d for d in discoveries if d.asset_id}
            
            # Register in lineage
            lineage_integration = AssetLineageIntegration()
            lineage_result = lineage_integration.register_batch_assets(
                assets, 
                [discoveries_dict.get(a.id) for a in assets]
            )
            
            return jsonify({
                'status': 'success',
                'total_assets': len(assets),
                'lineage_registered': lineage_result.get('registered', 0),
                'lineage_updated': lineage_result.get('updated', 0),
                'lineage_failed': lineage_result.get('failed', 0)
            }), 200
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Failed to sync discovered assets: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@lineage_bp.route('/diagram/<path:dataset_urn>', methods=['GET'])
def generate_lineage_diagram(dataset_urn: str):
    """
    Generate automatic lineage diagram.
    
    Query params:
    - direction: upstream|downstream|both (default: both)
    - depth: 1-5 (default: 3)
    - format: reactflow|mermaid|graphviz (default: reactflow)
    - include_column_lineage: true|false (default: false)
    """
    try:
        direction = request.args.get('direction', 'both')
        depth = int(request.args.get('depth', 3))
        format_type = request.args.get('format', 'reactflow')
        include_column_lineage = request.args.get('include_column_lineage', 'false').lower() == 'true'
        
        if direction not in ['upstream', 'downstream', 'both']:
            return jsonify({'error': 'direction must be upstream, downstream, or both'}), 400
        
        if format_type not in ['reactflow', 'mermaid', 'graphviz']:
            return jsonify({'error': 'format must be reactflow, mermaid, or graphviz'}), 400
        
        diagram = diagram_generator.generate_lineage_diagram(
            root_dataset_urn=dataset_urn,
            direction=direction,
            depth=depth,
            include_column_lineage=include_column_lineage,
            format=format_type
        )
        
        return jsonify(diagram), 200
        
    except Exception as e:
        logger.error(f"Diagram generation failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/sql/parse-and-ingest', methods=['POST'])
@require_auth
def parse_sql_and_ingest(user_id: str):
    """
    Parse SQL query and ingest lineage into the new lineage system.
    
    Body:
    {
        "sql_query": "CREATE VIEW v_customers AS SELECT * FROM customers",
        "target_asset_id": "asset_123",
        "source_system": "sql_parsing",
        "job_id": "job_123",
        "job_name": "Create Customer View",
        "dialect": "mysql"
    }
    """
    try:
        data = request.json
        if not data or not data.get('sql_query'):
            return jsonify({'error': 'sql_query is required'}), 400
        
        from services.sql_lineage_integration import SQLLineageIntegration
        
        sql_integration = SQLLineageIntegration()
        result = sql_integration.parse_and_ingest_sql_lineage(
            sql_query=data['sql_query'],
            target_asset_id=data.get('target_asset_id'),
            target_urn=data.get('target_urn'),
            source_system=data.get('source_system', 'sql_parsing'),
            job_id=data.get('job_id'),
            job_name=data.get('job_name'),
            dialect=data.get('dialect', 'mysql'),
            user_id=user_id
        )
        
        if result.get('status') == 'error':
            return jsonify(result), 400
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"SQL parse and ingest failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/sql/scan-asset/<asset_id>', methods=['POST'])
@require_auth
def scan_asset_sql(user_id: str, asset_id: str):
    """
    Scan an asset for SQL queries and automatically extract lineage.
    Looks in view_sql_commands, technical_metadata, etc.
    """
    try:
        from services.sql_lineage_integration import SQLLineageIntegration
        
        sql_integration = SQLLineageIntegration()
        result = sql_integration.scan_asset_sql_and_extract_lineage(
            asset_id=asset_id,
            user_id=user_id
        )
        
        if result.get('status') == 'error':
            return jsonify(result), 400
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Asset SQL scan failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


@lineage_bp.route('/procedure/parse-and-ingest', methods=['POST'])
@require_auth
def parse_procedure_and_ingest(user_id: str):
    """
    Parse stored procedure and ingest lineage into the new lineage system.
    
    Body:
    {
        "procedure_code": "CREATE OR REPLACE PROCEDURE proc_name AS BEGIN ... END;",
        "procedure_name": "proc_name",
        "language": "plsql",
        "target_asset_id": "asset_123",
        "source_system": "oracle"
    }
    """
    try:
        data = request.json
        if not data or not data.get('procedure_code'):
            return jsonify({'error': 'procedure_code is required'}), 400
        
        from services.sql_lineage_integration import SQLLineageIntegration
        
        sql_integration = SQLLineageIntegration()
        result = sql_integration.parse_and_ingest_procedure_lineage(
            procedure_code=data['procedure_code'],
            procedure_name=data.get('procedure_name', 'unknown_procedure'),
            language=data.get('language', 'plsql'),
            target_asset_id=data.get('target_asset_id'),
            source_system=data.get('source_system', 'stored_procedure'),
            user_id=user_id
        )
        
        if result.get('status') == 'error':
            return jsonify(result), 400
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"Procedure parse and ingest failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


