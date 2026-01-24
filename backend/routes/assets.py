"""
Asset management routes.
Production-level route handlers for asset CRUD and operations.
"""

import os
import sys
import logging
from flask import Blueprint, request, jsonify
from datetime import datetime
from sqlalchemy.orm.attributes import flag_modified

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import Asset, DataDiscovery, Connection
from utils.helpers import handle_error, normalize_columns, normalize_column_schema, generate_view_sql_commands
from flask import current_app

logger = logging.getLogger(__name__)

# Optional Starburst / Trino client support
try:
    from trino.dbapi import connect as trino_connect
    from trino.auth import BasicAuthentication

    STARBURST_AVAILABLE = True
except ImportError:
    trino_connect = None
    BasicAuthentication = None
    STARBURST_AVAILABLE = False


assets_bp = Blueprint('assets', __name__)


def _quote_starburst_identifier(identifier: str) -> str:
    """
    Quote an identifier for Starburst/Trino using double quotes and escape inner quotes.
    """
    if identifier is None:
        return '""'
    value = str(identifier)
    value = value.replace('"', '""')
    return f'"{value}"'


def generate_starburst_masked_view_sql(
    asset,
    columns,
    catalog: str,
    schema: str,
    table_name: str,
    view_name: str,
    mode: str = "analytical",
):
    """
    Generate a Starburst/Trino-compatible CREATE VIEW statement with masking applied.

    - Uses \"catalog\".\"schema\".\"table\" naming.
    - Applies '***MASKED***' for PII columns unless masking logic explicitly allows full visibility.
    - Always adds SECURITY INVOKER as requested.
    """
    if not catalog or not schema:
        raise ValueError("catalog and schema are required for Starburst view generation")

    mode_normalized = (mode or "analytical").lower()
    if mode_normalized not in ("analytical", "operational"):
        mode_normalized = "analytical"

    if not table_name:
        table_name = asset.name
    if not view_name:
        # Default view name depends on masking mode
        if mode_normalized == "analytical":
            view_name = f"{table_name}_masked_analytical"
        else:
            view_name = f"{table_name}_masked_operational"

    q_catalog = _quote_starburst_identifier(catalog)
    q_schema = _quote_starburst_identifier(schema)
    q_table = _quote_starburst_identifier(table_name)
    q_view = _quote_starburst_identifier(view_name)

    full_table = f"{q_catalog}.{q_schema}.{q_table}"
    full_view = f"{q_catalog}.{q_schema}.{q_view}"

    select_lines = []
    masking_summary = []

    for col in columns:
        col_name = col.get("name")
        if not col_name:
            continue

        q_col = _quote_starburst_identifier(col_name)
        pii_detected = bool(col.get("pii_detected"))
        if mode_normalized == "analytical":
            masking_logic = col.get("masking_logic_analytical")
        else:
            masking_logic = col.get("masking_logic_operational")
        masking_logic_str = str(masking_logic) if masking_logic not in (None, "") else ""
        logic_norm = masking_logic_str.strip().lower()

        # Decide effective expression for this column
        if pii_detected:
            if logic_norm in ("show_full", "none", "no_mask", "unmasked", "original"):
                expr = q_col
                effective_mode = "unmasked"
            else:
                # Default to full redaction for PII in Starburst view
                expr = "'***MASKED***'"
                effective_mode = "redacted"
        else:
            expr = q_col
            effective_mode = "unmasked"

        if expr == q_col:
            select_lines.append(f"    {q_col}")
        else:
            select_lines.append(f"    {expr} AS {q_col}")

        masking_summary.append(
            {
                "name": col_name,
                "pii_detected": pii_detected,
                "masking_logic_analytical": masking_logic_str,
                "effective_mode": effective_mode,
                "expression": expr,
            }
        )

    if not select_lines:
        raise ValueError("No columns available to generate Starburst view")

    comma_newline = ",\n"
    select_sql = comma_newline.join(select_lines)

    view_sql = f"""CREATE OR REPLACE VIEW {full_view}
SECURITY INVOKER
AS
SELECT
{select_sql}
FROM {full_table};"""

    return view_sql, masking_summary


def execute_starburst_view_sql(
    host: str,
    port: int,
    user: str,
    password: str,
    http_scheme: str,
    sql: str,
    catalog: str = None,
    schema: str = None,
    verify: bool = True,
):
    """
    Execute a CREATE VIEW statement in Starburst/Trino using basic authentication.
    Connection details are provided per request and are not stored.
    """
    if not STARBURST_AVAILABLE or trino_connect is None:
        raise RuntimeError("Starburst/Trino client library (trino) is not installed on the server")

    conn = None
    try:
        auth = None
        if password:
            auth = BasicAuthentication(user, password)

        conn = trino_connect(
            host=host,
            port=port,
            user=user,
            http_scheme=http_scheme or "https",
            auth=auth,
            catalog=catalog,
            schema=schema,
            verify=verify,
        )
        cur = conn.cursor()
        cur.execute(sql)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def test_starburst_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    http_scheme: str,
    catalog: str = None,
    schema: str = None,
    verify: bool = True,
):
    """
    Authenticate to Starburst/Trino and run a cheap query to validate credentials.
    """
    execute_starburst_view_sql(
        host=host,
        port=port,
        user=user,
        password=password,
        http_scheme=http_scheme,
        sql="SELECT 1",
        catalog=catalog,
        schema=schema,
        verify=verify,
    )

@assets_bp.route('/api/assets', methods=['GET'])
@handle_error
def get_assets():
    db = SessionLocal()
    try:
        discovery_id = request.args.get('discovery_id', type=int)
        minimal = request.args.get('minimal', '').lower() in ('1', 'true', 'yes')
        
        if discovery_id:

            discovery = db.query(DataDiscovery).filter(DataDiscovery.id == discovery_id).first()
            if not discovery:
                return jsonify({"error": "Discovery record not found"}), 404
            
            if discovery.asset_id:
                asset = db.query(Asset).filter(Asset.id == discovery.asset_id).first()
                if not asset:
                    return jsonify({"error": "Asset not found for this discovery_id"}), 404
                
                # Get application_name from connection config
                application_name = None
                if asset.connector_id:
                    try:
                        # Extract connection name from connector_id (format: "type_name")
                        parts = asset.connector_id.split('_', 1)
                        if len(parts) == 2:
                            connector_type, connection_name = parts
                            connection = db.query(Connection).filter(
                                Connection.name == connection_name,
                                Connection.connector_type == connector_type
                            ).first()
                            if connection and connection.config:
                                application_name = connection.config.get('application_name')
                    except Exception as e:
                        logger.warning('FN:get_assets error_fetching_connection_for_single_asset:{}'.format(str(e)))
                
                # Quality score calculation removed - data quality detection has been removed
                operational_metadata = asset.operational_metadata or {}
                
                asset_data = {
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog,
                    "connector_id": asset.connector_id,
                    "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
                    "technical_metadata": asset.technical_metadata,
                    "operational_metadata": operational_metadata,
                    "business_metadata": asset.business_metadata,
                    "columns": normalize_columns(asset.columns or []),
                    "custom_columns": asset.custom_columns or {},
                    "discovery_id": discovery.id,
                    "discovery_status": discovery.status,
                    "discovery_approval_status": discovery.approval_status,
                    "application_name": application_name  # From connection config
                }
                # Add data_source_type from discovery
                if discovery.data_source_type:
                    asset_data["data_source_type"] = discovery.data_source_type
                elif asset.connector_id:
                    # Derive from connector_id
                    if asset.connector_id.startswith('azure_blob_'):
                        asset_data["data_source_type"] = "azure_blob"
                    elif asset.connector_id.startswith('adls_gen2_') or 'datalake' in asset.connector_id.lower():
                        asset_data["data_source_type"] = "adls_gen2"
                    else:
                        parts = asset.connector_id.split('_')
                        if parts:
                            asset_data["data_source_type"] = parts[0]
                
                return jsonify([asset_data])
            else:
                return jsonify({"error": "No asset linked to this discovery_id"}), 404

        # FAST PATH: minimal asset payload (used by lineage UI). Avoid expensive joins/counts.
        if minimal:
            page = request.args.get('page', type=int) or 1
            per_page = request.args.get('per_page', type=int) or 500
            if page < 1:
                return jsonify({"error": "Page must be >= 1"}), 400
            if per_page < 1:
                return jsonify({"error": "Per page must be >= 1"}), 400
            if per_page > 1000:
                per_page = 1000
            offset = (page - 1) * per_page

            rows = (
                db.query(
                    Asset.id,
                    Asset.name,
                    Asset.type,
                    Asset.catalog,
                    Asset.connector_id,
                    Asset.discovered_at,
                    Asset.columns,
                )
                .order_by(Asset.discovered_at.desc())
                .limit(per_page)
                .offset(offset)
                .all()
            )

            assets = []
            for r in rows:
                assets.append({
                    "id": r.id,
                    "name": r.name,
                    "type": r.type,
                    "catalog": r.catalog,
                    "connector_id": r.connector_id,
                    "discovered_at": r.discovered_at.isoformat() if r.discovered_at else None,
                    "columns": normalize_columns(r.columns or []),
                })

            return jsonify({
                "assets": assets,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    # Intentionally omitted: "total" / "total_pages" (avoids expensive count)
                    "total": None,
                    "total_pages": None,
                    "has_next": len(assets) == per_page,
                    "has_prev": page > 1
                }
            })
        
        # OPTIMIZATION 8: Optional pagination - backward compatible
        # If pagination params are provided, use them. Otherwise, return all (backward compatible)
        page = request.args.get('page', type=int)
        per_page = request.args.get('per_page', type=int)
        
        use_pagination = page is not None and per_page is not None
        
        if use_pagination:
            # Validate pagination parameters
            if page < 1:
                return jsonify({"error": "Page must be >= 1"}), 400
            if per_page < 1:
                return jsonify({"error": "Per page must be >= 1"}), 400
            if per_page > 1000:
                return jsonify({"error": "Per page cannot exceed 1000"}), 400
            
            per_page = min(per_page, 1000)  # Max 1000 per page (generous)
            offset = (page - 1) * per_page
        else:
            # Backward compatible: return all if pagination not requested
            offset = None
            per_page = None

        # NEW: Filter parameters from query string
        search_term = request.args.get('search', type=str)
        type_filter = request.args.getlist('type')  # Multiple types: ?type=table&type=view
        catalog_filter = request.args.getlist('catalog')
        approval_status_filter = request.args.getlist('approval_status')
        application_name_filter = request.args.getlist('application_name')

        from sqlalchemy import case, func, or_
        
        # Get latest *visible* discovery IDs for all assets (for joining).
        # This ensures assets don't disappear just because their newest discovery was hidden.
        latest_discovery_subq = db.query(
            DataDiscovery.asset_id,
            func.max(DataDiscovery.id).label('latest_discovery_id')
        ).filter(
            DataDiscovery.asset_id.isnot(None),
            DataDiscovery.is_visible.is_(True),
        ).group_by(DataDiscovery.asset_id).subquery()
        
        # Build base query
        query = db.query(Asset, DataDiscovery).outerjoin(
            latest_discovery_subq, Asset.id == latest_discovery_subq.c.asset_id
        ).outerjoin(
            DataDiscovery, DataDiscovery.id == latest_discovery_subq.c.latest_discovery_id
        )

        # Hide assets that have discoveries but none of them are visible (e.g. deduplicated-away).
        # Still allow assets with no discovery record at all (e.g., non-DataDiscovery sources).
        # Logic: Show assets if they have a visible discovery OR they have no discoveries at all
        from sqlalchemy import and_
        # Subquery to find assets that have any discoveries
        assets_with_any_discovery = (
            db.query(DataDiscovery.asset_id)
            .filter(DataDiscovery.asset_id.isnot(None))
            .distinct()
            .subquery()
        )
        
        # Use LEFT JOIN to check if asset has any discoveries
        query = query.outerjoin(
            assets_with_any_discovery,
            Asset.id == assets_with_any_discovery.c.asset_id
        )
        
        # Filter: Show if has visible discovery OR has no discoveries at all
        query = query.filter(
            or_(
                DataDiscovery.id.isnot(None),  # Has visible discovery
                and_(
                    DataDiscovery.id.is_(None),  # No visible discovery
                    assets_with_any_discovery.c.asset_id.is_(None)  # No discoveries at all
                )
            )
        )
        
        # Apply filters at database level (before pagination)
        if search_term:
            search_lower = f"%{search_term.lower()}%"
            query = query.filter(
                or_(
                    Asset.name.ilike(search_lower),
                    Asset.catalog.ilike(search_lower)
                )
            )
        
        if type_filter:
            query = query.filter(Asset.type.in_(type_filter))
        
        if catalog_filter:
            query = query.filter(Asset.catalog.in_(catalog_filter))
        
        # Apply JSON field filters using MySQL JSON functions
        if approval_status_filter:
            from sqlalchemy import text
            # Build conditions for each status
            status_conditions = []
            for idx, status in enumerate(approval_status_filter):
                # Use unique parameter names
                param_name = f"status_{idx}"
                # MySQL JSON extraction with NULL handling
                sql_str = (
                    "(JSON_UNQUOTE(JSON_EXTRACT(assets.operational_metadata, '$.approval_status')) = :{param} "
                    "OR (assets.operational_metadata IS NULL AND :{param} = 'pending_review'))"
                ).format(param=param_name)
                # Use params() method for parameter binding
                condition = text(sql_str).params(**{param_name: status})
                status_conditions.append(condition)
            
            if status_conditions:
                query = query.filter(or_(*status_conditions))
        
        if application_name_filter:
            from sqlalchemy import text
            # Build conditions for each application name
            # Check: 1) connection config application_name, 2) technical_metadata, 3) business_metadata
            app_conditions = []
            for idx, app_name in enumerate(application_name_filter):
                param_name = f"app_name_{idx}"
                
                # Combine all three sources with OR
                # 1. Connection config application_name (most reliable source)
                # 2. Technical metadata application_name
                # 3. Business metadata application_name
                # Use proper parameter binding with :param_name format
                combined_condition = text(
                    "(EXISTS ("
                    "SELECT 1 FROM connections "
                    "WHERE CONCAT(connections.connector_type, '_', connections.name) = assets.connector_id "
                    f"AND JSON_UNQUOTE(JSON_EXTRACT(connections.config, '$.application_name')) = :{param_name}"
                    ") "
                    f"OR JSON_UNQUOTE(JSON_EXTRACT(assets.technical_metadata, '$.application_name')) = :{param_name} "
                    f"OR JSON_UNQUOTE(JSON_EXTRACT(assets.business_metadata, '$.application_name')) = :{param_name})"
                ).params(**{param_name: app_name})
                
                app_conditions.append(combined_condition)
            
            if app_conditions:
                query = query.filter(or_(*app_conditions))
        
        # Apply ordering
        query = query.order_by(
            Asset.discovered_at.desc(),
            case((DataDiscovery.id.is_(None), 1), else_=0),
            DataDiscovery.id.desc()
        )
        
        # Get filtered count (for pagination) - must be done after filters
        total_count = query.count()
        
        # Apply pagination if requested
        if use_pagination:
            assets_with_discovery = query.limit(per_page).offset(offset).all()
        else:
            assets_with_discovery = query.all()
        
        result = []
        seen_asset_ids = set()
        # Get all connections to map connector_id to application_name
        connections_map = {}
        try:
            connections = db.query(Connection).all()
            for conn in connections:
                connector_id_prefix = f"{conn.connector_type}_{conn.name}"
                config = conn.config or {}
                application_name = config.get('application_name')
                if application_name:
                    connections_map[connector_id_prefix] = application_name
        except Exception as e:
            logger.warning('FN:get_assets error_fetching_connections:{}'.format(str(e)))
        
        for asset, discovery in assets_with_discovery:

            if asset.id in seen_asset_ids:
                continue
            seen_asset_ids.add(asset.id)
            
            # Quality score calculation removed - data quality detection has been removed
            operational_metadata = asset.operational_metadata or {}
            
            # Get application_name from connection config
            application_name = None
            if asset.connector_id:
                # Try exact match first
                if asset.connector_id in connections_map:
                    application_name = connections_map[asset.connector_id]
                else:
                    # Try prefix match (connector_id format: "type_name")
                    for conn_prefix, app_name in connections_map.items():
                        if asset.connector_id.startswith(conn_prefix):
                            application_name = app_name
                            break
            
            asset_data = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "technical_metadata": asset.technical_metadata,
            "operational_metadata": operational_metadata,
            "business_metadata": asset.business_metadata,
            "columns": normalize_columns(asset.columns or []),
            "custom_columns": asset.custom_columns or {},
            "application_name": application_name  # From connection config
            }
            if discovery:
                asset_data["discovery_id"] = discovery.id
                asset_data["discovery_status"] = discovery.status
                asset_data["discovery_approval_status"] = discovery.approval_status
                asset_data["data_source_type"] = discovery.data_source_type
            else:
                # Derive data_source_type from connector_id if discovery not available
                if asset.connector_id:
                    if asset.connector_id.startswith('azure_blob_'):
                        asset_data["data_source_type"] = "azure_blob"
                    elif asset.connector_id.startswith('adls_gen2_') or 'datalake' in asset.connector_id.lower():
                        asset_data["data_source_type"] = "adls_gen2"
                    else:
                        # Extract from connector_id format: "type_name"
                        parts = asset.connector_id.split('_')
                        if parts:
                            asset_data["data_source_type"] = parts[0]
            result.append(asset_data)
        
        # Return response with optional pagination info
        if use_pagination:
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
        else:
            # Backward compatible: return all assets without pagination info
            return jsonify(result)
    finally:
        db.close()



@assets_bp.route('/api/assets', methods=['POST'])
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
        # OPTIMIZATION: Remove unnecessary refresh calls - data already committed and available

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
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to create assets"}), 400
    finally:
        db.close()



@assets_bp.route('/api/assets/<asset_id>', methods=['GET'])
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
        
        # Get application_name from connection config
        application_name = None
        if asset.connector_id:
            try:
                # Extract connection name from connector_id (format: "type_name")
                parts = asset.connector_id.split('_', 1)
                if len(parts) == 2:
                    connector_type, connection_name = parts
                    connection = db.query(Connection).filter(
                        Connection.name == connection_name,
                        Connection.connector_type == connector_type
                    ).first()
                    if connection and connection.config:
                        application_name = connection.config.get('application_name')
            except Exception as e:
                logger.warning('FN:get_asset_by_id error_fetching_connection:{}'.format(str(e)))
        
        # Quality score calculation removed - data quality detection has been removed
        operational_metadata = asset.operational_metadata or {}
        columns = normalize_columns(asset.columns or [])
        
        result = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "columns": columns,
            "custom_columns": asset.custom_columns or {},
            "technical_metadata": asset.technical_metadata or {},
            "operational_metadata": operational_metadata,
            "business_metadata": asset.business_metadata or {},
            "application_name": application_name  # From connection config
        }
        
        # Generate SQL CREATE VIEW commands based on masking logic
        view_sql = generate_view_sql_commands(asset, columns)
        result["view_sql_commands"] = {
            "analytical_sql": view_sql['analytical_sql'],
            "operational_sql": view_sql['operational_sql'],
            "has_masked_columns": view_sql['has_masked_columns']
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
            # data_quality_score removed - data quality detection has been removed
            result["validation_status"] = discovery.validation_status
            result["validated_at"] = discovery.validated_at.isoformat() if discovery.validated_at else None
        
        return jsonify(result), 200
    except Exception as e:
        logger.error('FN:get_asset_by_id asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()



@assets_bp.route('/api/assets/<asset_id>', methods=['PUT'])
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
            flag_modified(asset, "business_metadata")
        if 'technical_metadata' in data:
            asset.technical_metadata = data['technical_metadata']
            flag_modified(asset, "technical_metadata")
        if 'operational_metadata' in data:
            asset.operational_metadata = data['operational_metadata']
            flag_modified(asset, "operational_metadata")
        if 'columns' in data:
            asset.columns = data['columns']
            flag_modified(asset, "columns")
        if 'custom_columns' in data:
            asset.custom_columns = data['custom_columns']
            flag_modified(asset, "custom_columns")

        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh - data already in session after commit

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
            "columns": asset.columns,
            "custom_columns": asset.custom_columns or {},
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:update_asset error:{}'.format(str(e)), exc_info=True)
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to update asset"}), 400
    finally:
        db.close()



@assets_bp.route('/api/assets/<asset_id>/columns/<column_name>/pii', methods=['PUT'])
@handle_error
def update_column_pii(asset_id, column_name):
    """Update PII status for a specific column"""
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404

        data = request.json
        if not data:
            return jsonify({"error": "Request body is required"}), 400

        columns = asset.columns or []
        column_found = False
        
        # Update the specific column's PII status and description
        for col in columns:
            if col.get('name') == column_name:
                # Update description if provided
                if 'description' in data:
                    description = data.get('description')
                    if isinstance(description, str):
                        col['description'] = description.strip() if description.strip() else None
                    else:
                        col['description'] = description
                
                pii_detected = data.get('pii_detected', False)
                col['pii_detected'] = pii_detected
                # Set pii_types based on pii_detected status
                if pii_detected:
                    # If marking as PII, use provided types or default to ['PII']
                    col['pii_types'] = data.get('pii_types', ['PII'])
                    # Store masking logic for analytical and operational users
                    # Always save masking logic when provided in request (even if empty string)
                    # Convert empty/whitespace strings to None for consistency
                    if 'masking_logic_analytical' in data:
                        masking_analytical = data.get('masking_logic_analytical')
                        if isinstance(masking_analytical, str):
                            col['masking_logic_analytical'] = masking_analytical.strip() if masking_analytical.strip() else None
                        else:
                            col['masking_logic_analytical'] = masking_analytical
                    else:
                        # If not provided and column was non-PII, initialize to None
                        if 'masking_logic_analytical' not in col:
                            col['masking_logic_analytical'] = None
                    
                    if 'masking_logic_operational' in data:
                        masking_operational = data.get('masking_logic_operational')
                        if isinstance(masking_operational, str):
                            col['masking_logic_operational'] = masking_operational.strip() if masking_operational.strip() else None
                        else:
                            col['masking_logic_operational'] = masking_operational
                    else:
                        # If not provided and column was non-PII, initialize to None
                        if 'masking_logic_operational' not in col:
                            col['masking_logic_operational'] = None
                else:
                    # If marking as non-PII, clear the types and masking logic
                    col['pii_types'] = None
                    col['masking_logic_analytical'] = None
                    col['masking_logic_operational'] = None
                column_found = True
                break
        
        if not column_found:
            return jsonify({"error": f"Column '{column_name}' not found in asset"}), 404

        asset.columns = columns
        flag_modified(asset, "columns")  # Mark JSON column as modified for SQLAlchemy
        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh - data already in session after commit

        # Log masking logic save for debugging - show what was received vs what was saved
        updated_col = next((col for col in columns if col.get('name') == column_name), None)
        masking_analytical = updated_col.get('masking_logic_analytical') if updated_col else None
        masking_operational = updated_col.get('masking_logic_operational') if updated_col else None
        
        received_analytical = data.get('masking_logic_analytical')
        received_operational = data.get('masking_logic_operational')
        
        logger.info('FN:update_column_pii asset_id:{} column_name:{} pii_detected:{} received_analytical:{} received_operational:{} saved_analytical:{} saved_operational:{}'.format(
            asset_id, column_name, data.get('pii_detected'), 
            received_analytical, received_operational, masking_analytical, masking_operational
        ))

        updated_column = next((col for col in columns if col.get('name') == column_name), None)
        return jsonify({
            "success": True,
            "message": f"PII status updated for column '{column_name}'",
            "column": normalize_column_schema(updated_column) if updated_column else None
        }), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:update_column_pii asset_id:{} column_name:{} error:{}'.format(asset_id, column_name, str(e)), exc_info=True)
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to update column PII status"}), 400
    finally:
        db.close()



@assets_bp.route('/api/assets/<asset_id>/approve', methods=['POST'])
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
        

        # Only update existing discovery records - don't create new ones
        # Discovery records should be created during the discovery process, not during approval
        # Use the same logic as GET endpoint: get the discovery record with the highest ID
        # This ensures we update the same record that the GET endpoint will return
        discovery = db.query(DataDiscovery).filter(
            DataDiscovery.asset_id == asset_id
        ).order_by(DataDiscovery.id.desc()).first()
        if discovery:
            discovery.approval_status = "approved"
            discovery.status = "approved"
            if not discovery.approval_workflow:
                discovery.approval_workflow = {}
            discovery.approval_workflow["approved_at"] = approval_time.isoformat()
            discovery.approval_workflow["approved_by"] = "user"
            flag_modified(discovery, "approval_workflow")
        
        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh calls - data already committed and available

        logger.info('FN:approve_asset asset_id:{} approval_status:{} saved_to_db:True'.format(
            asset_id, asset.operational_metadata.get("approval_status")
        ))
        

        response_data = {
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
            "updated_at": approval_time.isoformat()
        }
        
        # Only include discovery_id if a discovery record exists
        if discovery:
            response_data["discovery_id"] = discovery.id
        
        return jsonify(response_data), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:approve_asset asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()



@assets_bp.route('/api/assets/<asset_id>/reject', methods=['POST'])
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
        

        # Only update existing discovery records - don't create new ones
        # Discovery records should be created during the discovery process, not during rejection
        # Use the same logic as GET endpoint: get the discovery record with the highest ID
        # This ensures we update the same record that the GET endpoint will return
        discovery = db.query(DataDiscovery).filter(
            DataDiscovery.asset_id == asset_id
        ).order_by(DataDiscovery.id.desc()).first()
        if discovery:
            discovery.approval_status = "rejected"
            discovery.status = "rejected"
            if not discovery.approval_workflow:
                discovery.approval_workflow = {}
            discovery.approval_workflow["rejected_at"] = rejection_time.isoformat()
            discovery.approval_workflow["rejected_by"] = "user"
            discovery.approval_workflow["rejection_reason"] = reason
            flag_modified(discovery, "approval_workflow")
        
        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh calls - data already in session after commit

        logger.info('FN:reject_asset asset_id:{} approval_status:{} saved_to_db:True'.format(
            asset_id, asset.operational_metadata.get("approval_status")
        ))
        
        response_data = {
            "id": asset.id,
            "name": asset.name,
            "approval_status": "rejected",
            "rejection_reason": reason,
            "updated_at": rejection_time.isoformat()
        }
        
        # Only include discovery_id if a discovery record exists
        if discovery:
            response_data["discovery_id"] = discovery.id
        
        return jsonify(response_data), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:reject_asset asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()



@assets_bp.route('/api/assets/<asset_id>/publish', methods=['POST'])
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
        

        # Only update existing discovery records - don't create new ones
        # Discovery records should be created during the discovery process, not during publishing
        # Use the same logic as GET endpoint: get the discovery record with the highest ID
        # This ensures we update the same record that the GET endpoint will return
        discovery = db.query(DataDiscovery).filter(
            DataDiscovery.asset_id == asset_id
        ).order_by(DataDiscovery.id.desc()).first()
        if discovery:
            discovery.status = "published"
            discovery.published_at = publish_time
            discovery.published_to = published_to
        
        db.commit()
        # OPTIMIZATION: Remove unnecessary refresh calls - data already in session after commit
        
        response_data = {
            "id": asset.id,
            "name": asset.name,
            "status": "published",
            "published_to": published_to,
            "published_at": publish_time.isoformat()
        }
        
        # Only include discovery_id if a discovery record exists
        if discovery:
            response_data["discovery_id"] = discovery.id
        
        return jsonify(response_data), 200
    except Exception as e:
        db.rollback()
        logger.error('FN:publish_asset asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        return jsonify({"error": str(e)}), 400
    finally:
        db.close()


@assets_bp.route('/api/assets/<asset_id>/starburst/ingest', methods=['POST'])
@handle_error
def ingest_asset_to_starburst(asset_id):
    """
    Generate a Starburst masking view for the given asset and optionally execute it in Starburst Enterprise.

    Expected request JSON body:
    {
      "connection": {
        "host": "...",           # required for ingest
        "port": 443,
        "user": "starburst_user",
        "password": "secret",
        "http_scheme": "https"
      },
      "catalog": "lz_lakehouse",
      "schema": "en_visionplus",
      "table_name": "ath5_bkp_18112025",
      "view_name": "ath5_bkp_18112025_masked",
      "preview_only": true/false
    }
    """
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404

        payload = request.json or {}
        preview_only = bool(payload.get("preview_only", False))

        catalog = (payload.get("catalog") or "").strip()
        schema = (payload.get("schema") or "").strip()
        table_name = (payload.get("table_name") or "").strip() or asset.name

        # Optional explicit names for analytical and operational views.
        view_name_analytical = (payload.get("view_name_analytical") or "").strip()
        view_name_operational = (payload.get("view_name_operational") or "").strip()
        # Backwards compatibility with older single view_name field.
        raw_view_name = (payload.get("view_name") or "").strip()

        if not catalog or not schema:
            return jsonify({"error": "Both catalog and schema are required"}), 400

        columns = normalize_columns(asset.columns or [])

        conn_cfg = payload.get("connection") or {}
        host = (conn_cfg.get("host") or "").strip()
        try:
            port_raw = conn_cfg.get("port")
            port = int(port_raw) if port_raw not in (None, "") else 443
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid Starburst port"}), 400
        user = (conn_cfg.get("user") or "torro_user").strip() or "torro_user"
        password = conn_cfg.get("password") or ""
        http_scheme = (conn_cfg.get("http_scheme") or "https").strip() or "https"
        verify_ssl = conn_cfg.get("verify_ssl", True)
        if "skip_ssl_verification" in conn_cfg:
            try:
                verify_ssl = not bool(conn_cfg.get("skip_ssl_verification"))
            except Exception:
                pass
        verify_ssl = bool(verify_ssl)

        # If requested, authenticate to Starburst first (so UI can show auth errors before SQL preview/ingest)
        validate_connection = bool(payload.get("validate_connection", False))
        if validate_connection:
            if not host:
                return jsonify({"error": "Starburst host is required to authenticate"}), 400
            if not STARBURST_AVAILABLE:
                return jsonify({"error": "Starburst/Trino client library (trino) is not installed on the server."}), 503
            try:
                test_starburst_connection(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    http_scheme=http_scheme,
                    catalog=catalog,
                    schema=schema,
                    verify=verify_ssl,
                )
            except Exception as e:
                logger.error(
                    "FN:ingest_asset_to_starburst_auth_failed asset_id:%s host:%s port:%s error:%s",
                    asset_id,
                    host,
                    port,
                    str(e),
                    exc_info=True,
                )
                return jsonify({"error": f"Starburst authentication failed: {str(e)}"}), 400

        try:
            # Derive analytical and operational view names from the base table / user input.
            if view_name_analytical and view_name_operational:
                analytical_view_name = view_name_analytical
                operational_view_name = view_name_operational
            elif view_name_analytical:
                analytical_view_name = view_name_analytical
                suffix = "_analytical"
                if view_name_analytical.endswith(suffix):
                    operational_view_name = view_name_analytical[: -len(suffix)] + "_operational"
                else:
                    operational_view_name = f"{view_name_analytical}_operational"
            elif view_name_operational:
                operational_view_name = view_name_operational
                suffix = "_operational"
                if view_name_operational.endswith(suffix):
                    analytical_view_name = view_name_operational[: -len(suffix)] + "_analytical"
                else:
                    analytical_view_name = f"{view_name_operational}_analytical"
            elif raw_view_name:
                analytical_view_name = raw_view_name
                suffix = "_analytical"
                if raw_view_name.endswith(suffix):
                    operational_view_name = raw_view_name[: -len(suffix)] + "_operational"
                else:
                    operational_view_name = f"{raw_view_name}_operational"
            else:
                analytical_view_name = f"{table_name}_masked_analytical"
                operational_view_name = f"{table_name}_masked_operational"

            analytical_sql, analytical_summary = generate_starburst_masked_view_sql(
                asset,
                columns,
                catalog=catalog,
                schema=schema,
                table_name=table_name,
                view_name=analytical_view_name,
                mode="analytical",
            )

            operational_sql, operational_summary = generate_starburst_masked_view_sql(
                asset,
                columns,
                catalog=catalog,
                schema=schema,
                table_name=table_name,
                view_name=operational_view_name,
                mode="operational",
            )
        except Exception as e:
            logger.error(
                "FN:ingest_asset_to_starburst_generate_sql asset_id:%s error:%s",
                asset_id,
                str(e),
                exc_info=True,
            )
            return jsonify({"error": f"Failed to generate Starburst view SQL: {str(e)}"}), 400

        combined_sql = f"{analytical_sql}\n\n-- Operational masked view\n{operational_sql}"

        response_data = {
            "success": True,
            "preview_only": preview_only,
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "view_name_analytical": analytical_view_name,
            "view_name_operational": operational_view_name,
            "view_sql_analytical": analytical_sql,
            "view_sql_operational": operational_sql,
            "view_sql": combined_sql,
            "masking_summary_analytical": analytical_summary,
            "masking_summary_operational": operational_summary,
        }

        if preview_only:
            # Only return the generated SQL and masking summary, do not connect to Starburst
            return jsonify(response_data), 200

        # Ingest into Starburst Enterprise (execute CREATE OR REPLACE VIEW for both analytical and operational)
        if not STARBURST_AVAILABLE:
            return jsonify({"error": "Starburst/Trino client library (trino) is not installed on the server."}), 503

        if not host:
            return jsonify({"error": "Starburst host is required to ingest the view"}), 400

        try:
            # Analytical view
            execute_starburst_view_sql(
                host=host,
                port=port,
                user=user,
                password=password,
                http_scheme=http_scheme,
                sql=analytical_sql,
                catalog=catalog,
                schema=schema,
                verify=verify_ssl,
            )
            # Operational view
            execute_starburst_view_sql(
                host=host,
                port=port,
                user=user,
                password=password,
                http_scheme=http_scheme,
                sql=operational_sql,
                catalog=catalog,
                schema=schema,
                verify=verify_ssl,
            )
        except Exception as e:
            logger.error(
                "FN:ingest_asset_to_starburst_execute asset_id:%s host:%s port:%s error:%s",
                asset_id,
                host,
                port,
                str(e),
                exc_info=True,
            )
            return jsonify({"error": f"Failed to create view in Starburst: {str(e)}"}), 400

        response_data["ingested"] = True
        return jsonify(response_data), 200
    finally:
        db.close()


# OLD LINEAGE ENDPOINTS REMOVED - Use new lineage system at /api/lineage/* instead
# Old endpoints were:
# - GET /api/lineage/relationships
# - POST /api/lineage/relationships
# These have been replaced by the new lineage system endpoints in routes/lineage_routes.py


