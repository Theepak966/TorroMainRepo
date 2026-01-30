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


def _enrich_s3_technical_metadata(technical_metadata, connector_id):
    """Backfill bucket, key, s3_uri, arn, aws_region for S3 assets when missing (e.g. discovered before these fields existed)."""
    if not connector_id or not connector_id.startswith("aws_s3_"):
        return technical_metadata
    # Copy to plain dict (SQLAlchemy JSON may return mutable dict-like; ensure we can add keys)
    raw = technical_metadata or {}
    tech = dict(raw) if isinstance(raw, dict) else {}
    bucket = tech.get("bucket")
    key = tech.get("key") or tech.get("full_path")
    # Derive bucket and key from location/storage_path (e.g. s3://bucket-name/path/to/key.parquet) if missing
    if not bucket or not key:
        loc = tech.get("location") or tech.get("storage_path") or ""
        if isinstance(loc, str) and loc.startswith("s3://"):
            rest = loc[5:].lstrip("/")  # after "s3://", keep leading slash for key
            if "/" in rest:
                b, k = rest.split("/", 1)
                bucket = bucket or b
                key = key or k
            else:
                bucket = bucket or rest
                key = key or ""
        if bucket:
            tech["bucket"] = bucket
        if key:
            tech["key"] = key
    if not bucket or not key:
        return tech
    if not tech.get("s3_uri"):
        tech["s3_uri"] = "s3://{}/{}".format(bucket, key)
    if not tech.get("arn"):
        tech["arn"] = "arn:aws:s3:::{}/{}".format(bucket, key)
    if not tech.get("aws_region"):
        tech["aws_region"] = "us-east-1"
    return tech


def _quote_starburst_identifier(identifier: str) -> str:
    """
    Quote an identifier for Starburst/Trino using double quotes and escape inner quotes.
    """
    if identifier is None:
        return '""'
    value = str(identifier)
    value = value.replace('"', '""')
    return f'"{value}"'


def convert_masking_logic_to_starburst_sql(masking_logic: str, column_name: str, column_type: str = None) -> str:
    """
    Convert masking logic string to Starburst/Trino-compatible SQL expression.
    Uses Trino/Starburst SQL functions instead of MySQL-specific ones.
    """
    import logging
    
    logger = logging.getLogger(__name__)
    
    if not masking_logic:
        return column_name
    
    masking_logic = masking_logic.lower().strip()
    column_lower = column_name.lower()
    q_col = _quote_starburst_identifier(column_name)
    
    # Standard masking options
    if masking_logic in ['redact', 'mask_all', 'pii_redact', 'mask', 'hide', 'nullify']:
        return "'***MASKED***'"
    elif masking_logic in ['encrypt', 'pii_encrypt', 'encryption', 'aes_encrypt']:
        # Note: Starburst/Trino encryption functions may vary by connector
        # Using a generic approach - may need adjustment based on actual connector
        logger.warning(f'FN:convert_masking_logic_to_starburst_sql column:{column_name} masking_logic:{masking_logic} message:Encryption not fully supported in Starburst, using hash instead')
        return f"md5(CAST({q_col} AS VARCHAR))"
    elif masking_logic in ['hash', 'hashing', 'pii_hashing', 'pii_hash', 'md5']:
        return f"md5(CAST({q_col} AS VARCHAR))"
    elif masking_logic == 'sha256':
        return f"sha256(CAST({q_col} AS VARCHAR))"
    elif masking_logic in ['show_full', 'none', 'no_mask', 'unmasked', 'original']:
        return q_col
    elif masking_logic in ['mask_domain', 'email_mask', 'hide_domain']:
        if 'email' in column_lower or 'mail' in column_lower:
            # Trino: Extract username@***.extension (e.g., user@***.com)
            return f"CONCAT(SPLIT_PART({q_col}, '@', 1), '@***.', SPLIT_PART(SPLIT_PART({q_col}, '@', -1), '.', -1))"
        else:
            return f"CONCAT(SUBSTR({q_col}, 1, 2), '***', SUBSTR({q_col}, GREATEST(1, LENGTH({q_col}) - 1)))"
    elif masking_logic in ['show_year_only', 'year_only', 'mask_date', 'hide_date']:
        if 'date' in column_lower or 'birth' in column_lower or 'dob' in column_lower or 'timestamp' in column_lower:
            return f"YEAR({q_col})"
        else:
            return f"CASE WHEN REGEXP_LIKE({q_col}, '^[0-9]{{4}}') THEN CAST(YEAR(CAST({q_col} AS DATE)) AS VARCHAR) ELSE '***MASKED***' END"
    elif masking_logic in ['show_last_4', 'last_4', 'phone_mask', 'mask_phone']:
        if 'phone' in column_lower or 'mobile' in column_lower or 'tel' in column_lower:
            return f"CONCAT('***', SUBSTR(REGEXP_REPLACE({q_col}, '[^0-9]', ''), GREATEST(1, LENGTH(REGEXP_REPLACE({q_col}, '[^0-9]', '')) - 3)))"
        else:
            return f"CONCAT('***', SUBSTR({q_col}, GREATEST(1, LENGTH({q_col}) - 3)))"
    elif masking_logic in ['show_first_octet', 'ip_mask', 'mask_ip', 'first_octet']:
        if 'ip' in column_lower and ('address' in column_lower or 'addr' in column_lower):
            return f"CONCAT(SPLIT_PART({q_col}, '.', 1), '.xxx.xxx.xxx')"
        else:
            return f"CONCAT(SUBSTR({q_col}, 1, 3), '***')"
    elif masking_logic in ['partial_mask', 'partial', 'mask_partial']:
        return f"CONCAT(SUBSTR({q_col}, 1, 2), '***', SUBSTR({q_col}, GREATEST(1, LENGTH({q_col}) - 1)))"
    elif masking_logic in ['show_first_letter', 'first_letter', 'mask_first_letter']:
        return f"CONCAT(SUBSTR({q_col}, 1, 1), '***')"
    elif masking_logic in ['show_age_range', 'age_range', 'age_only']:
        if 'birth' in column_lower or 'dob' in column_lower or 'age' in column_lower:
            return f"CONCAT(CAST(FLOOR(DATE_DIFF('day', CAST({q_col} AS DATE), CURRENT_DATE) / 365.25) AS VARCHAR), '-', CAST(FLOOR(DATE_DIFF('day', CAST({q_col} AS DATE), CURRENT_DATE) / 365.25) + 9 AS VARCHAR), ' years')"
        else:
            return f"CASE WHEN REGEXP_LIKE({q_col}, '^[0-9]{{4}}') THEN CONCAT(CAST(FLOOR(DATE_DIFF('day', CAST({q_col} AS DATE), CURRENT_DATE) / 365.25) AS VARCHAR), ' years') ELSE '***MASKED***' END"
    elif masking_logic in ['default', 'auto', 'smart', 'intelligent']:
        if 'email' in column_lower or 'mail' in column_lower:
            return f"CONCAT(SPLIT_PART({q_col}, '@', 1), '@***.', SPLIT_PART(SPLIT_PART({q_col}, '@', -1), '.', -1))"
        elif 'phone' in column_lower or 'mobile' in column_lower or 'tel' in column_lower:
            return f"CONCAT('***', SUBSTR(REGEXP_REPLACE({q_col}, '[^0-9]', ''), GREATEST(1, LENGTH(REGEXP_REPLACE({q_col}, '[^0-9]', '')) - 3)))"
        elif 'birth' in column_lower or 'dob' in column_lower:
            return f"YEAR({q_col})"
        elif 'ip' in column_lower and 'address' in column_lower:
            return f"CONCAT(SPLIT_PART({q_col}, '.', 1), '.xxx.xxx.xxx')"
        elif column_lower.endswith('_id') or (column_lower.endswith('id') and len(column_lower) > 2):
            return f"md5(CAST({q_col} AS VARCHAR))"
        elif 'name' in column_lower and ('first' in column_lower or 'last' in column_lower or 'full' in column_lower):
            return f"CONCAT(SUBSTR({q_col}, 1, 1), '***')"
        elif 'address' in column_lower or 'street' in column_lower or 'city' in column_lower:
            return f"CONCAT(SUBSTR({q_col}, 1, 3), '***', SUBSTR({q_col}, GREATEST(1, LENGTH({q_col}) - 2)))"
        elif 'ssn' in column_lower or 'social' in column_lower:
            return "'***-**-****'"
        elif 'card' in column_lower or 'credit' in column_lower:
            return f"CONCAT('****-****-****-', SUBSTR(REGEXP_REPLACE({q_col}, '[^0-9]', ''), GREATEST(1, LENGTH(REGEXP_REPLACE({q_col}, '[^0-9]', '')) - 3)))"
        else:
            return f"md5(CAST({q_col} AS VARCHAR))"
    elif masking_logic.startswith('show_first_') or masking_logic.startswith('first_'):
        try:
            n = int(masking_logic.split('_')[-1])
            return f"CONCAT(SUBSTR({q_col}, 1, {n}), '***')"
        except (ValueError, IndexError):
            return f"CONCAT(SUBSTR({q_col}, 1, 2), '***')"
    elif masking_logic.startswith('show_last_') or masking_logic.startswith('last_'):
        try:
            n = int(masking_logic.split('_')[-1])
            return f"CONCAT('***', SUBSTR({q_col}, GREATEST(1, LENGTH({q_col}) - {n} + 1)))"
        except (ValueError, IndexError):
            return f"CONCAT('***', SUBSTR({q_col}, GREATEST(1, LENGTH({q_col}) - 3)))"
    elif masking_logic in ['show_initials', 'initials', 'show_initials_only']:
        if 'name' in column_lower:
            # Trino: Extract first letter, then find space and get next letter
            return f"CONCAT(SUBSTR({q_col}, 1, 1), CASE WHEN POSITION(' ' IN {q_col}) > 0 THEN CONCAT('.', SUBSTR({q_col}, POSITION(' ' IN {q_col}) + 1, 1)) ELSE '' END)"
        else:
            return f"CONCAT(SUBSTR({q_col}, 1, 1), '***')"
    elif masking_logic in ['show_city_only', 'city_only', 'mask_address_keep_city']:
        if 'address' in column_lower or 'street' in column_lower:
            # Extract city from comma-separated address
            return f"TRIM(ELEMENT_AT(SPLIT({q_col}, ','), -2))"
        else:
            return f"CONCAT(SUBSTR({q_col}, 1, 3), '***')"
    elif masking_logic in ['show_state_only', 'state_only', 'mask_address_keep_state']:
        if 'address' in column_lower or 'street' in column_lower:
            # Extract state from comma-separated address (last element)
            return f"TRIM(ELEMENT_AT(SPLIT({q_col}, ','), -1))"
        else:
            return f"CONCAT(SUBSTR({q_col}, 1, 2), '***')"
    elif '(' in masking_logic or masking_logic.upper() in ['NULL', 'TRUE', 'FALSE']:
        # Allow custom SQL expressions
        return masking_logic
    else:
        logger.warning(f'FN:convert_masking_logic_to_starburst_sql column:{column_name} masking_logic:{masking_logic} message:Unknown masking logic, defaulting to MD5 hash')
        return f"md5(CAST({q_col} AS VARCHAR))"


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
    - Applies user-defined masking logic for PII columns (hash, partial mask, show_full, etc.)
    - Defaults to '***MASKED***' for PII columns if no masking logic is specified.
    - Always adds SECURITY INVOKER as requested.
    """
    if not catalog or not schema:
        raise ValueError("catalog and schema are required for Starburst view generation")

    mode_normalized = (mode or "analytical").lower()
    if mode_normalized not in ("analytical", "operational"):
        mode_normalized = "analytical"

    # Track if table_name was provided
    has_table_name = bool(table_name and table_name.strip())
    
    # If no table_name provided, try to auto-detect by using schema name as table name
    # This allows views to work: FROM "catalog"."schema"."schema" 
    # (table name = schema name is a common pattern)
    if not has_table_name:
        # Use schema name as the table name (so FROM becomes catalog.schema.schema)
        table_name = schema if schema else (asset.name if asset else "default_table")
        has_table_name = True
    
    if not view_name:
        # Default view name depends on masking mode
        base_name = table_name
        if mode_normalized == "analytical":
            view_name = f"{base_name}_masked_analytical"
        else:
            view_name = f"{base_name}_masked_operational"

    q_catalog = _quote_starburst_identifier(catalog)
    q_schema = _quote_starburst_identifier(schema)
    q_view = _quote_starburst_identifier(view_name)
    q_table = _quote_starburst_identifier(table_name)
    
    full_view = f"{q_catalog}.{q_schema}.{q_view}"
    # Always use full 3-part table reference: catalog.schema.table
    full_table = f"{q_catalog}.{q_schema}.{q_table}"

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
        col_type = col.get("type", "string")

        # Always include the original column
        # Always reference column by name (we always have a table now)
        select_lines.append(f"    {q_col}")

        # If PII detected, also add a masked column with _masked suffix
        if pii_detected:
            if masking_logic_str:
                # Use the user-defined masking logic
                masked_expr = convert_masking_logic_to_starburst_sql(masking_logic_str, col_name, col_type)
                # Check if masking logic results in same as original (e.g., "show_full")
                if masked_expr == q_col:
                    effective_mode = "unmasked"
                    # If masking logic is "show_full", don't add masked column (just original)
                else:
                    effective_mode = "masked"
                    # Add masked column with _masked suffix
                    q_masked_col = _quote_starburst_identifier(f"{col_name}_masked")
                    # Always use the masking expression (we always have a table now)
                    select_lines.append(f"    {masked_expr} AS {q_masked_col}")
                    logger.debug(f'FN:generate_starburst_masked_view_sql column:{col_name} mode:{mode_normalized} masking_logic:{masking_logic_str} masked_expression:{masked_expr}')
            else:
                # PII detected but no masking logic specified - add default masked column
                effective_mode = "redacted"
                q_masked_col = _quote_starburst_identifier(f"{col_name}_masked")
                # Always use the masked value (we always have a table now)
                select_lines.append(f"    '***MASKED***' AS {q_masked_col}")
                logger.debug(f'FN:generate_starburst_masked_view_sql column:{col_name} mode:{mode_normalized} no_masking_logic_using_default')
        else:
            # Not PII - only original column (no masked version)
            effective_mode = "unmasked"

        masking_summary.append(
            {
                "name": col_name,
                "pii_detected": pii_detected,
                "masking_logic_analytical": masking_logic_str if mode_normalized == "analytical" else col.get("masking_logic_analytical", ""),
                "masking_logic_operational": masking_logic_str if mode_normalized == "operational" else col.get("masking_logic_operational", ""),
                "effective_mode": effective_mode,
                "has_masked_column": pii_detected and (masking_logic_str or True),  # True if PII detected
            }
        )

    if not select_lines:
        raise ValueError("No columns available to generate Starburst view")

    comma_newline = ",\n"
    select_sql = comma_newline.join(select_lines)

    # Always reference a table (table_name is always set - either provided or schema name)
    # This ensures views always have a FROM clause and can return data
    from_clause = f"FROM {full_table}"
    
    view_sql = f"""CREATE OR REPLACE VIEW {full_view}
SECURITY INVOKER
AS
SELECT
{select_sql}
{from_clause};"""

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
    roles: dict = None,
):
    """
    Execute a CREATE VIEW statement in Starburst/Trino using basic authentication.
    Connection details are provided per request and are not stored.
    """
    if not STARBURST_AVAILABLE or trino_connect is None:
        raise RuntimeError("Starburst/Trino client library (trino) is not installed on the server")

    conn = None
    rows = []
    try:
        auth = None
        if password:
            auth = BasicAuthentication(user, password)

        # Build connection parameters
        conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "http_scheme": http_scheme or "https",
            "catalog": catalog,
            "schema": schema,
            "verify": verify,
        }
        if auth:
            conn_params["auth"] = auth
        conn = trino_connect(**conn_params)
        cur = conn.cursor()

        # Apply roles via SQL for compatibility across trino client versions.
        # Starburst/Trino syntax: SET ROLE <role> IN <catalog>
        if roles:
            for role_catalog, role_name in roles.items():
                if not role_catalog or not role_name:
                    continue
                q_role_name = _quote_starburst_identifier(role_name)
                # Starburst deployments vary:
                # - Some support global roles: SET ROLE <role>
                # - Some support catalog-scoped roles: SET ROLE <role> IN <catalog>
                if str(role_catalog).strip().lower() in ("system",):
                    cur.execute(f"SET ROLE {q_role_name}")
                else:
                    q_role_catalog = _quote_starburst_identifier(role_catalog)
                    cur.execute(f"SET ROLE {q_role_name} IN {q_role_catalog}")
                try:
                    _ = cur.fetchall()
                except Exception:
                    pass

        cur.execute(sql)

        # Important: ensure the statement fully executes before closing the connection.
        # The Trino DB-API may only surface errors (or even perform work) during fetch.
        try:
            rows = cur.fetchall() or []
        except Exception:
            # Many statements (e.g., CREATE VIEW) return no rows; still force completion.
            try:
                one = cur.fetchone()
                if one is not None:
                    rows = [one]
            except Exception:
                pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return rows


def test_starburst_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    http_scheme: str,
    catalog: str = None,
    schema: str = None,
    verify: bool = True,
    roles: dict = None,
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
        roles=roles,
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
                technical_metadata_single = _enrich_s3_technical_metadata(
                    asset.technical_metadata, asset.connector_id
                )
                asset_data = {
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog,
                    "connector_id": asset.connector_id,
                    "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
                    "technical_metadata": technical_metadata_single,
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
            try:
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
                connector_ids = set()
                for r in rows:
                    if r.connector_id:
                        connector_ids.add((r.connector_id or "").strip())
                    assets.append({
                        "id": r.id,
                        "name": r.name,
                        "type": r.type,
                        "catalog": r.catalog,
                        "connector_id": r.connector_id,
                        "discovered_at": r.discovered_at.isoformat() if r.discovered_at else None,
                        "columns": normalize_columns(r.columns or []),
                    })
                # Map connector_id -> application_name from connection config
                app_name_map = {}
                conn_by_type_name = {}
                if connector_ids:
                    try:
                        all_conns = db.query(Connection).filter(Connection.connector_type.isnot(None), Connection.name.isnot(None)).all()
                        for conn in all_conns:
                            ct = (conn.connector_type or "").strip()
                            nm = (conn.name or "").strip()
                            cfg = conn.config or {}
                            an = cfg.get("application_name") or cfg.get("applicationName")
                            if an:
                                conn_by_type_name[(ct, nm)] = an
                            cid = "{}_{}".format(ct, nm)
                            if cid in connector_ids and an:
                                app_name_map[cid] = an
                            cid_raw = "{}_{}".format(conn.connector_type or "", conn.name or "")
                            if cid_raw in connector_ids and an and cid_raw not in app_name_map:
                                app_name_map[cid_raw] = an
                        for cid in connector_ids:
                            if cid in app_name_map:
                                continue
                            if "_" in cid:
                                parts = cid.split("_", 2)
                                if len(parts) >= 2:
                                    if parts[0] == "aws" and parts[1] == "s3":
                                        conn_type, conn_name = "aws_s3", (parts[2] if len(parts) > 2 else "")
                                    elif parts[0] == "azure" and parts[1] == "blob":
                                        conn_type, conn_name = "azure_blob", (parts[2] if len(parts) > 2 else "")
                                    elif parts[0] == "oracle" and parts[1] == "db":
                                        conn_type, conn_name = "oracle_db", (parts[2] if len(parts) > 2 else "")
                                    else:
                                        conn_type, conn_name = "_".join(parts[:-1]), parts[-1]
                                    if conn_name:
                                        an = conn_by_type_name.get((conn_type, conn_name))
                                        if an:
                                            app_name_map[cid] = an
                    except Exception as e:
                        logger.debug("FN:get_assets minimal application_name map error:%s", str(e))
                for a in assets:
                    cid_key = (a.get("connector_id") or "").strip() or a.get("connector_id")
                    a["application_name"] = (app_name_map.get(cid_key) or app_name_map.get(a.get("connector_id"))) if cid_key else None

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
            except Exception as e:
                logger.error("FN:get_assets minimal path error:%s", str(e), exc_info=True)
                return jsonify({"error": "Failed to load assets", "detail": str(e) if current_app.config.get("DEBUG") else None}), 500
        
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

        from sqlalchemy import and_, case, func, or_

        def _build_assets_listing_id_query():
            """
            Build an efficient, paginatable query that returns (asset_id, latest_visible_discovery_id).

            Why:
            - The previous implementation selected *all* Asset + DataDiscovery columns and then ordered
              the joined result set. On larger datasets, MySQL can exhaust sort buffer memory and
              return: "Out of sort memory, consider increasing server sort buffer size".
            - This query keeps the ordered row width tiny (two integers), then we fetch the full
              Asset and DataDiscovery rows in follow-up queries after pagination.
            """
            latest_visible_discovery_subq = (
                db.query(
                    DataDiscovery.asset_id.label("asset_id"),
                    func.max(DataDiscovery.id).label("latest_discovery_id"),
                )
                .filter(
                    DataDiscovery.asset_id.isnot(None),
                    DataDiscovery.is_visible.is_(True),
                )
                .group_by(DataDiscovery.asset_id)
                .subquery()
            )

            assets_with_any_discovery_subq = (
                db.query(DataDiscovery.asset_id.label("asset_id"))
                .filter(DataDiscovery.asset_id.isnot(None))
                .distinct()
                .subquery()
            )

            id_query = (
                db.query(
                    Asset.id.label("asset_id"),
                    latest_visible_discovery_subq.c.latest_discovery_id.label("latest_discovery_id"),
                )
                .outerjoin(
                    latest_visible_discovery_subq,
                    Asset.id == latest_visible_discovery_subq.c.asset_id,
                )
                .outerjoin(
                    assets_with_any_discovery_subq,
                    Asset.id == assets_with_any_discovery_subq.c.asset_id,
                )
                .filter(
                    or_(
                        latest_visible_discovery_subq.c.latest_discovery_id.isnot(None),
                        and_(
                            latest_visible_discovery_subq.c.latest_discovery_id.is_(None),
                            assets_with_any_discovery_subq.c.asset_id.is_(None),
                        ),
                    )
                )
                .order_by(
                    Asset.discovered_at.desc(),
                    case((latest_visible_discovery_subq.c.latest_discovery_id.is_(None), 1), else_=0),
                    latest_visible_discovery_subq.c.latest_discovery_id.desc(),
                )
            )

            return id_query, latest_visible_discovery_subq

        id_query, latest_visible_discovery_subq = _build_assets_listing_id_query()
        
        # Apply filters at database level (before pagination)
        if search_term:
            search_lower = f"%{search_term.lower()}%"
            id_query = id_query.filter(
                or_(
                    Asset.name.ilike(search_lower),
                    Asset.catalog.ilike(search_lower)
                )
            )
        
        if type_filter:
            id_query = id_query.filter(Asset.type.in_(type_filter))
        
        if catalog_filter:
            id_query = id_query.filter(Asset.catalog.in_(catalog_filter))
        
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
                id_query = id_query.filter(or_(*status_conditions))
        
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
                id_query = id_query.filter(or_(*app_conditions))

        # Get filtered count (for pagination) after all filters.
        # Remove ORDER BY for the count query to avoid unnecessary sort work.
        total_count = id_query.order_by(None).count()

        # Apply pagination if requested (still only selecting tiny rows).
        if use_pagination:
            page_rows = id_query.limit(per_page).offset(offset).all()
        else:
            page_rows = id_query.all()

        asset_ids_in_order = [r.asset_id for r in page_rows]
        latest_discovery_ids_in_order = [r.latest_discovery_id for r in page_rows]

        assets_by_id = {}
        if asset_ids_in_order:
            assets = db.query(Asset).filter(Asset.id.in_(asset_ids_in_order)).all()
            assets_by_id = {a.id: a for a in assets}

        discoveries_by_id = {}
        latest_discovery_ids = [d_id for d_id in latest_discovery_ids_in_order if d_id]
        if latest_discovery_ids:
            discoveries = db.query(DataDiscovery).filter(DataDiscovery.id.in_(latest_discovery_ids)).all()
            discoveries_by_id = {d.id: d for d in discoveries}
        
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
        
        for asset_id, discovery_id in zip(asset_ids_in_order, latest_discovery_ids_in_order):
            asset = assets_by_id.get(asset_id)
            discovery = discoveries_by_id.get(discovery_id) if discovery_id else None

            if not asset:
                # Defensive: if an asset was deleted between the ID query and the fetch.
                continue

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
            
            technical_metadata = _enrich_s3_technical_metadata(
                asset.technical_metadata, asset.connector_id
            )
            asset_data = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "technical_metadata": technical_metadata,
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
        technical_metadata = _enrich_s3_technical_metadata(
            asset.technical_metadata or {}, asset.connector_id
        )
        # Belt-and-suspenders: ensure S3 fields present when connector is S3 (SQLAlchemy JSON copy can miss keys)
        if asset.connector_id and str(asset.connector_id).startswith("aws_s3_"):
            tm_raw = asset.technical_metadata or {}
            bucket = technical_metadata.get("bucket") or tm_raw.get("bucket")
            key = technical_metadata.get("key") or tm_raw.get("key") or tm_raw.get("full_path")
            if not bucket or not key:
                loc = technical_metadata.get("location") or tm_raw.get("location") or tm_raw.get("storage_path") or ""
                if isinstance(loc, str) and loc.startswith("s3://") and "/" in loc[5:].lstrip("/"):
                    parts = loc[5:].lstrip("/").split("/", 1)
                    bucket = bucket or parts[0]
                    key = key or parts[1]
            if bucket and key:
                if not technical_metadata.get("s3_uri"):
                    technical_metadata["s3_uri"] = "s3://{}/{}".format(bucket, key)
                if not technical_metadata.get("arn"):
                    technical_metadata["arn"] = "arn:aws:s3:::{}/{}".format(bucket, key)
                if not technical_metadata.get("aws_region"):
                    technical_metadata["aws_region"] = "us-east-1"

        result = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type,
            "catalog": asset.catalog,
            "connector_id": asset.connector_id,
            "discovered_at": asset.discovered_at.isoformat() if asset.discovered_at else None,
            "columns": columns,
            "custom_columns": asset.custom_columns or {},
            "technical_metadata": technical_metadata,
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
        # If table_name is empty, it will default to asset.name in generate_starburst_masked_view_sql
        table_name = (payload.get("table_name") or "").strip()

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
        
        # Optional: allow caller to request a Starburst/Trino role.
        # By default we assume the built-in \"sysadmin\" role in the \"system\" catalog,
        # so the session has the same privileges as the Starburst UI user.
        roles = None
        raw_role = conn_cfg.get("role")
        if raw_role is None:
            # No role explicitly provided → default to sysadmin
            role_name = "sysadmin"
        else:
            role_name = raw_role.strip()

        if role_name:
            role_catalog = (conn_cfg.get("role_catalog") or "system").strip() or "system"
            roles = {role_catalog: role_name}

        # Always generate masking view SQL first (no auth required) so it can be shown regardless of auth/ingest outcome
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

        # Optional: validate connection; on failure still return 200 with SQL so user can copy manually
        validate_connection = bool(payload.get("validate_connection", False))
        if validate_connection:
            if not host:
                response_data["success"] = False
                response_data["error"] = "Starburst host is required to authenticate"
                return jsonify(response_data), 200
            if not STARBURST_AVAILABLE:
                response_data["success"] = False
                response_data["error"] = "Starburst/Trino client library (trino) is not installed on the server."
                return jsonify(response_data), 200
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
                    roles=roles,
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
                response_data["success"] = False
                response_data["error"] = f"Starburst authentication failed: {str(e)}"
                return jsonify(response_data), 200

        if preview_only:
            # Only return the generated SQL and masking summary, do not connect to Starburst
            return jsonify(response_data), 200

        # Ingest into Starburst Enterprise (execute CREATE OR REPLACE VIEW for both analytical and operational)
        if not STARBURST_AVAILABLE:
            response_data["success"] = False
            response_data["error"] = "Starburst/Trino client library (trino) is not installed on the server."
            return jsonify(response_data), 200

        if not host:
            response_data["success"] = False
            response_data["error"] = "Starburst host is required to ingest the view"
            return jsonify(response_data), 200

        try:
            # Determine the actual table name to use
            # If not provided, use schema name as table name (matches generate_starburst_masked_view_sql logic)
            has_table_name = bool(table_name and table_name.strip())
            if not has_table_name:
                table_name = schema if schema else (asset.name if asset else "default_table")
            
            q_catalog = _quote_starburst_identifier(catalog)
            q_schema = _quote_starburst_identifier(schema)
            q_table = _quote_starburst_identifier(table_name)

            # This is the actual object referenced in the FROM clause
            from_target = f"{q_catalog}.{q_schema}.{q_table}"
            
            # For file-based catalogs (like catalog_fs_azure), tables come from files, not CREATE TABLE
            # So we skip table creation and just create the view - it will work when data files exist
            # For other catalogs, try to create the table if it doesn't exist
            is_file_based_catalog = catalog and ("fs_" in catalog.lower() or "file" in catalog.lower() or "hive" in catalog.lower())
            
            if not is_file_based_catalog:
                # For non-file-based catalogs, try to create the table
                if not has_table_name:
                    logger.info(
                        "FN:ingest_asset_to_starburst no_table_name_provided skipping_table_creation for non_file_catalog from_target:%s",
                        from_target,
                    )
                else:
                    col_defs = []
                    for col in columns:
                        col_name = col.get("name")
                        if not col_name:
                            continue
                        col_type = col.get("type", "string")
                        # Map types to Trino/Starburst types
                        if col_type.lower() in ("string", "varchar", "text"):
                            trino_type = "VARCHAR"
                        elif col_type.lower() in ("int", "integer"):
                            trino_type = "INTEGER"
                        elif col_type.lower() in ("bigint", "long"):
                            trino_type = "BIGINT"
                        elif col_type.lower() in ("double", "float"):
                            trino_type = "DOUBLE"
                        elif col_type.lower() in ("boolean", "bool"):
                            trino_type = "BOOLEAN"
                        elif col_type.lower() in ("date", "timestamp"):
                            trino_type = "TIMESTAMP"
                        else:
                            trino_type = "VARCHAR"

                        q_col = _quote_starburst_identifier(col_name)
                        col_defs.append(f'{q_col} {trino_type}')

                    if col_defs:
                        # Create empty table with proper structure
                        create_table_sql = f'CREATE TABLE IF NOT EXISTS {q_catalog}.{q_schema}.{q_table} ({", ".join(col_defs)})'

                        try:
                            execute_starburst_view_sql(
                                host=host,
                                port=port,
                                user=user,
                                password=password,
                                http_scheme=http_scheme,
                                sql=create_table_sql,
                                catalog=catalog,
                                schema=schema,
                                verify=verify_ssl,
                                roles=roles,
                            )
                            logger.info(f'FN:ingest_asset_to_starburst created_table_if_not_exists table:{table_name}')
                        except Exception as table_error:
                            error_msg = str(table_error)
                            if "already exists" in error_msg.lower() or "table already" in error_msg.lower():
                                logger.info(f'FN:ingest_asset_to_starburst table_already_exists table:{table_name}')
                            else:
                                logger.warning(f'FN:ingest_asset_to_starburst table_creation_failed table:{table_name} error:{error_msg}')
            else:
                # For file-based catalogs, tables come from files - just create the view
                # The view will work once the data files exist in the storage location
                logger.info(
                    "FN:ingest_asset_to_starburst file_based_catalog_detected catalog:%s skipping_table_creation from_target:%s",
                    catalog,
                    from_target,
                )
            
            # Create views - they will reference the FROM target (which may or may not exist yet)
            # For file-based catalogs, tables appear when data files are present
            # For other catalogs, table should have been created above
            view_errors = []
            
            # Analytical view
            try:
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
                    roles=roles,
                )
                logger.info(f'FN:ingest_asset_to_starburst created_analytical_view view:{analytical_view_name}')
            except Exception as view_error:
                error_msg = str(view_error)
                if ("does not exist" in error_msg.lower()) or ("not exist" in error_msg.lower()):
                    if is_file_based_catalog:
                        view_errors.append(
                            f"Analytical view creation failed: Source '{from_target}' not found. "
                            f"For file-based catalogs, ensure the underlying table/files exist so this reference resolves."
                        )
                    else:
                        view_errors.append(
                            f"Analytical view creation failed: Source '{from_target}' does not exist. "
                            f"Please provide a valid table name or ensure the source exists."
                        )
                else:
                    view_errors.append(f"Analytical view creation failed: {error_msg}")
            
            # Operational view
            try:
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
                    roles=roles,
                )
                logger.info(f'FN:ingest_asset_to_starburst created_operational_view view:{operational_view_name}')
            except Exception as view_error:
                error_msg = str(view_error)
                if ("does not exist" in error_msg.lower()) or ("not exist" in error_msg.lower()):
                    if is_file_based_catalog:
                        view_errors.append(
                            f"Operational view creation failed: Source '{from_target}' not found. "
                            f"For file-based catalogs, ensure the underlying table/files exist so this reference resolves."
                        )
                    else:
                        view_errors.append(
                            f"Operational view creation failed: Source '{from_target}' does not exist. "
                            f"Please provide a valid table name or ensure the source exists."
                        )
                else:
                    view_errors.append(f"Operational view creation failed: {error_msg}")
            
            # If there were view creation errors, return SQL so user can copy and run manually
            if view_errors:
                error_message = "Some views could not be created:\n" + "\n".join(view_errors)
                logger.warning(f'FN:ingest_asset_to_starburst view_creation_errors: {error_message}')
                response_data["success"] = False
                response_data["error"] = error_message
                return jsonify(response_data), 200
        except Exception as e:
            logger.error(
                "FN:ingest_asset_to_starburst_execute asset_id:%s host:%s port:%s error:%s",
                asset_id,
                host,
                port,
                str(e),
                exc_info=True,
            )
            response_data["success"] = False
            response_data["error"] = f"Failed to create view in Starburst: {str(e)}"
            return jsonify(response_data), 200

        response_data["ingested"] = True
        return jsonify(response_data), 200
    finally:
        db.close()


# OLD LINEAGE ENDPOINTS REMOVED - Use new lineage system at /api/lineage/* instead
# Old endpoints were:
# - GET /api/lineage/relationships
# - POST /api/lineage/relationships
# These have been replaced by the new lineage system endpoints in routes/lineage_routes.py


