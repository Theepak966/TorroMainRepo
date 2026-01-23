"""
Shared utility functions used across route handlers.
Production-level helper functions for common operations.
"""

from functools import wraps
from flask import jsonify
import logging

logger = logging.getLogger(__name__)


def normalize_column_schema(column):
    """Ensure all expected fields are present in column schema, including masking logic"""
    if not isinstance(column, dict):
        return column
    
    normalized = dict(column)  # Copy existing fields
    # Always ensure masking logic fields are present in the response schema
    # Set to None if not present or if empty string
    if 'masking_logic_analytical' not in normalized or normalized.get('masking_logic_analytical') == '':
        normalized['masking_logic_analytical'] = None
    if 'masking_logic_operational' not in normalized or normalized.get('masking_logic_operational') == '':
        normalized['masking_logic_operational'] = None
    return normalized


def normalize_columns(columns):
    """Normalize a list of columns to ensure consistent schema with masking logic fields"""
    if not columns:
        return []
    return [normalize_column_schema(col) for col in columns]


def sanitize_connection_config(config):
    """Remove sensitive fields from connection config before returning in API responses"""
    if not config or not isinstance(config, dict):
        return config
    
    # Create a copy to avoid modifying the original
    sanitized = config.copy()
    
    # List of sensitive keys that should be masked or removed
    sensitive_keys = [
        'password', 'pwd', 'pass', 'secret', 'key', 'api_key', 'apiKey',
        'access_key', 'accessKey', 'secret_key', 'secretKey', 'private_key',
        'privateKey', 'connection_string', 'connectionString', 'token',
        'auth_token', 'authToken', 'credential', 'credentials'
    ]
    
    # Remove or mask sensitive keys
    for key in list(sanitized.keys()):
        key_lower = key.lower()
        if any(sensitive in key_lower for sensitive in sensitive_keys):
            sanitized[key] = "***REDACTED***"
    
    return sanitized


def handle_error(f):
    """Decorator for error handling in route handlers"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error('FN:handle_error function_name:{} error:{}'.format(f.__name__, str(e)), exc_info=True)
            # Import app here to avoid circular imports
            from main import app
            if app.config.get("DEBUG"):
                return jsonify({"error": str(e)}), 500
            else:
                return jsonify({"error": "An internal error occurred"}), 500
    return decorated_function


def clean_for_json(obj):
    """Clean object for JSON serialization, handling datetime and other non-serializable types"""
    import json
    import base64
    from datetime import datetime, date
    
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode('utf-8')
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, (int, float, str, bool, type(None))):
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)
    else:
        # Try to convert to string as fallback
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)


def convert_masking_logic_to_sql(masking_logic: str, column_name: str, column_type: str = None) -> str:
    """
    Convert masking logic string to actual SQL expression.
    Ensures ALL masking logic values are converted to SQL - never returns literal strings.
    """
    import os
    import logging
    
    logger = logging.getLogger(__name__)
    
    if not masking_logic:
        return column_name
    
    masking_logic = masking_logic.lower().strip()
    column_lower = column_name.lower()
    
    # Standard masking options
    if masking_logic in ['redact', 'mask_all', 'pii_redact', 'mask', 'hide', 'nullify']:
        return "'***MASKED***'"
    elif masking_logic in ['encrypt', 'pii_encrypt', 'encryption', 'aes_encrypt']:
        encryption_key = os.getenv('ENCRYPTION_KEY', 'default_encryption_key_change_in_production')
        if encryption_key == 'default_encryption_key_change_in_production':
            logger.warning('FN:convert_masking_logic_to_sql message:Using default encryption key')
        escaped_key = encryption_key.replace("'", "''")
        return f"AES_ENCRYPT({column_name}, UNHEX(SHA2('{escaped_key}', 256)))"
    elif masking_logic in ['hash', 'hashing', 'pii_hashing', 'pii_hash', 'md5', 'sha256']:
        if masking_logic == 'sha256':
            return f"SHA2({column_name}, 256)"
        else:
            return f"MD5({column_name})"
    elif masking_logic in ['show_full', 'none', 'no_mask', 'unmasked', 'original']:
        return column_name
    elif masking_logic in ['mask_domain', 'email_mask', 'hide_domain']:
        if 'email' in column_lower or 'mail' in column_lower:
            return f"CONCAT(SUBSTRING_INDEX({column_name}, '@', 1), '@***.', SUBSTRING_INDEX(SUBSTRING_INDEX({column_name}, '@', -1), '.', -1))"
        else:
            return f"CONCAT(LEFT({column_name}, 2), '***', RIGHT({column_name}, 2))"
    elif masking_logic in ['show_year_only', 'year_only', 'mask_date', 'hide_date']:
        if 'date' in column_lower or 'birth' in column_lower or 'dob' in column_lower or 'timestamp' in column_lower:
            return f"YEAR({column_name})"
        else:
            return f"CASE WHEN {column_name} REGEXP '^[0-9]{4}' THEN YEAR({column_name}) ELSE '***MASKED***' END"
    elif masking_logic in ['show_last_4', 'last_4', 'phone_mask', 'mask_phone']:
        if 'phone' in column_lower or 'mobile' in column_lower or 'tel' in column_lower:
            return f"CONCAT('***', RIGHT(REGEXP_REPLACE({column_name}, '[^0-9]', ''), 4))"
        else:
            return f"CONCAT('***', RIGHT({column_name}, 4))"
    elif masking_logic in ['show_first_octet', 'ip_mask', 'mask_ip', 'first_octet']:
        if 'ip' in column_lower and ('address' in column_lower or 'addr' in column_lower):
            return f"CONCAT(SUBSTRING_INDEX({column_name}, '.', 1), '.xxx.xxx.xxx')"
        else:
            return f"CONCAT(LEFT({column_name}, 3), '***')"
    elif masking_logic in ['partial_mask', 'partial', 'mask_partial']:
        return f"CONCAT(LEFT({column_name}, 2), '***', RIGHT({column_name}, 2))"
    elif masking_logic in ['show_first_letter', 'first_letter', 'mask_first_letter']:
        return f"CONCAT(LEFT({column_name}, 1), '***')"
    elif masking_logic in ['show_age_range', 'age_range', 'age_only']:
        if 'birth' in column_lower or 'dob' in column_lower or 'age' in column_lower:
            return f"CONCAT(FLOOR(DATEDIFF(CURDATE(), {column_name}) / 365.25), '-', FLOOR(DATEDIFF(CURDATE(), {column_name}) / 365.25) + 9, ' years')"
        else:
            return f"CASE WHEN {column_name} REGEXP '^[0-9]{4}' THEN CONCAT(FLOOR(DATEDIFF(CURDATE(), {column_name}) / 365.25), ' years') ELSE '***MASKED***' END"
    elif masking_logic in ['default', 'auto', 'smart', 'intelligent']:
        if 'email' in column_lower or 'mail' in column_lower:
            return f"CONCAT(SUBSTRING_INDEX({column_name}, '@', 1), '@***.', SUBSTRING_INDEX(SUBSTRING_INDEX({column_name}, '@', -1), '.', -1))"
        elif 'phone' in column_lower or 'mobile' in column_lower or 'tel' in column_lower:
            return f"CONCAT('***', RIGHT(REGEXP_REPLACE({column_name}, '[^0-9]', ''), 4))"
        elif 'birth' in column_lower or 'dob' in column_lower:
            return f"YEAR({column_name})"
        elif 'ip' in column_lower and 'address' in column_lower:
            return f"CONCAT(SUBSTRING_INDEX({column_name}, '.', 1), '.xxx.xxx.xxx')"
        elif column_lower.endswith('_id') or (column_lower.endswith('id') and len(column_lower) > 2):
            return f"MD5({column_name})"
        elif 'name' in column_lower and ('first' in column_lower or 'last' in column_lower or 'full' in column_lower):
            return f"CONCAT(LEFT({column_name}, 1), '***')"
        elif 'address' in column_lower or 'street' in column_lower or 'city' in column_lower:
            return f"CONCAT(LEFT({column_name}, 3), '***', RIGHT({column_name}, 3))"
        elif 'ssn' in column_lower or 'social' in column_lower:
            return "'***-**-****'"
        elif 'card' in column_lower or 'credit' in column_lower:
            return f"CONCAT('****-****-****-', RIGHT(REGEXP_REPLACE({column_name}, '[^0-9]', ''), 4))"
        else:
            return f"MD5({column_name})"
    elif masking_logic.startswith('show_first_') or masking_logic.startswith('first_'):
        try:
            n = int(masking_logic.split('_')[-1])
            return f"CONCAT(LEFT({column_name}, {n}), '***')"
        except (ValueError, IndexError):
            return f"CONCAT(LEFT({column_name}, 2), '***')"
    elif masking_logic.startswith('show_last_') or masking_logic.startswith('last_'):
        try:
            n = int(masking_logic.split('_')[-1])
            return f"CONCAT('***', RIGHT({column_name}, {n}))"
        except (ValueError, IndexError):
            return f"CONCAT('***', RIGHT({column_name}, 4))"
    elif masking_logic in ['show_initials', 'initials', 'show_initials_only']:
        if 'name' in column_lower:
            return f"CONCAT(LEFT({column_name}, 1), IF(LOCATE(' ', {column_name}) > 0, CONCAT('.', LEFT(SUBSTRING({column_name}, LOCATE(' ', {column_name}) + 1), 1)), ''), IF(LOCATE(' ', SUBSTRING({column_name}, LOCATE(' ', {column_name}) + 1)) > 0, CONCAT('.', LEFT(SUBSTRING({column_name}, LOCATE(' ', SUBSTRING({column_name}, LOCATE(' ', {column_name}) + 1)) + LOCATE(' ', {column_name}) + 1), 1)), ''), '.')"
        else:
            return f"CONCAT(LEFT({column_name}, 1), '***')"
    elif masking_logic in ['show_city_only', 'city_only', 'mask_address_keep_city']:
        if 'address' in column_lower or 'street' in column_lower:
            return f"TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX({column_name}, ',', -2), ',', 1))"
        else:
            return f"CONCAT(LEFT({column_name}, 3), '***')"
    elif masking_logic in ['show_state_only', 'state_only', 'mask_address_keep_state']:
        if 'address' in column_lower or 'street' in column_lower:
            return f"TRIM(SUBSTRING_INDEX({column_name}, ',', -1))"
        else:
            return f"CONCAT(LEFT({column_name}, 2), '***')"
    elif '(' in masking_logic or masking_logic.upper() in ['NULL', 'TRUE', 'FALSE']:
        return masking_logic
    else:
        logger.warning(f'FN:convert_masking_logic_to_sql column:{column_name} masking_logic:{masking_logic} message:Unknown masking logic, defaulting to MD5 hash')
        return f"MD5({column_name})"


def generate_view_sql_commands(asset, columns):
    """Generate SQL CREATE VIEW statements for analytical and operational views"""
    if not asset or not columns:
        return {
            'analytical_sql': None,
            'operational_sql': None,
            'has_masked_columns': False
        }
    
    table_name = asset.name
    catalog = asset.catalog or ''
    if catalog:
        full_table_name = f"{catalog}.{table_name}"
    else:
        full_table_name = table_name
    
    has_analytical_masking = False
    has_operational_masking = False
    
    analytical_selects = []
    operational_selects = []
    
    for col in columns:
        col_name = col.get('name', '')
        if not col_name:
            continue
            
        pii_detected = col.get('pii_detected', False)
        masking_analytical = col.get('masking_logic_analytical')
        masking_operational = col.get('masking_logic_operational')
        col_type = col.get('type', 'string')
        
        # Always include the original column
        analytical_selects.append(f"    {col_name}")
        operational_selects.append(f"    {col_name}")
        
        # If masking logic is applied, add masked column
        if pii_detected and masking_analytical:
            sql_expr = convert_masking_logic_to_sql(masking_analytical, col_name, col_type)
            masked_col_name = f"{col_name}_masked"
            analytical_selects.append(f"    {sql_expr} AS {masked_col_name}")
            has_analytical_masking = True
        elif pii_detected:
            # PII detected but no masking logic - add default masked column
            masked_col_name = f"{col_name}_masked"
            analytical_selects.append(f"    '***MASKED***' AS {masked_col_name}")
            has_analytical_masking = True
        
        if pii_detected and masking_operational:
            sql_expr = convert_masking_logic_to_sql(masking_operational, col_name, col_type)
            masked_col_name = f"{col_name}_masked"
            operational_selects.append(f"    {sql_expr} AS {masked_col_name}")
            has_operational_masking = True
        elif pii_detected:
            # PII detected but no masking logic for operational - just original column
            has_operational_masking = True
    
    analytical_sql = None
    operational_sql = None
    
    if has_analytical_masking:
        comma_newline = ',\n'
        analytical_sql = f"""CREATE OR REPLACE VIEW {full_table_name}_masked_analytical AS
SELECT
{comma_newline.join(analytical_selects)}
FROM {full_table_name};"""
    
    if has_operational_masking:
        comma_newline = ',\n'
        operational_sql = f"""CREATE OR REPLACE VIEW {full_table_name}_masked_operational AS
SELECT
{comma_newline.join(operational_selects)}
FROM {full_table_name};"""
    
    return {
        'analytical_sql': analytical_sql,
        'operational_sql': operational_sql,
        'has_masked_columns': has_analytical_masking or has_operational_masking
    }


def build_technical_metadata(asset_id, blob_info, file_extension, blob_path, container_name, storage_account, file_hash, schema_hash, metadata, current_date):
    """Build technical metadata for Azure Blob assets"""
    import logging
    logger = logging.getLogger(__name__)
    
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
    
    azure_metadata_dict = blob_info.get("metadata", {})
    if not isinstance(azure_metadata_dict, dict):
        azure_metadata_dict = {}
    
    azure_metadata_dict = clean_for_json(azure_metadata_dict)
    
    file_hash_str = str(file_hash) if file_hash else ""
    schema_hash_str = str(schema_hash) if schema_hash else ""
    
    size_bytes = blob_info.get("size") or blob_info.get("size_bytes") or 0
    if size_bytes is None or size_bytes == "":
        size_bytes = 0
    try:
        size_bytes = int(size_bytes)
        if size_bytes == 0:
            logger.warning('FN:build_technical_metadata blob_path:{} message:Size is 0'.format(blob_path))
    except (ValueError, TypeError) as e:
        logger.warning('FN:build_technical_metadata blob_path:{} size_value:{} error:{}'.format(blob_path, size_bytes, str(e)))
        size_bytes = 0
    
    format_value = file_extension or "unknown"
    if format_value == "unknown" or not format_value:
        content_type = blob_info.get("content_type", "")
        if content_type and "/" in content_type:
            format_value = content_type.split("/")[-1]
        elif content_type:
            format_value = content_type
    
    # Extract application name from location path (last two folders before filename)
    application_name = None
    if blob_path and "/" in blob_path:
        path_parts = blob_path.split("/")
        # Remove filename (last part)
        if path_parts:
            path_parts = path_parts[:-1]
        # Get last two folders
        if len(path_parts) >= 2:
            application_name = "/".join(path_parts[-2:])
        elif len(path_parts) == 1:
            application_name = path_parts[0]
    
    tech_meta = {
        "asset_id": asset_id,
        "asset_type": file_extension or "blob",
        "format": format_value,
        "content_type": blob_info.get("content_type", "application/octet-stream"),
        "size_bytes": size_bytes,
        "size": size_bytes,
        "location": blob_path,
        "container": container_name,
        "storage_account": storage_account,
        "created_at": created_at or current_date,
        "last_modified": last_modified or current_date,
        "file_extension": f".{file_extension}" if file_extension else "",
        "file_hash": file_hash_str,
        "schema_hash": schema_hash_str,
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
        "application_name": application_name,
        "azure_metadata": azure_metadata_dict,
        **(metadata.get("file_metadata", {}).get("format_specific", {}) if metadata else {}),
        "azure_storage_metadata": metadata.get("storage_metadata", {}).get("azure", {}) if metadata else {}
    }
    
    return clean_for_json(tech_meta)


def build_operational_metadata(azure_properties, current_date):
    """Build operational metadata for Azure Blob assets"""
    owner = azure_properties.get("metadata", {}).get("owner") if azure_properties else None
    if not owner:
        owner = "workspace_owner@hdfc.bank.in"
    
    access_level = "internal"
    if azure_properties:
        lease_status = azure_properties.get("lease_status")
        if lease_status and isinstance(lease_status, str):
            lease_status = lease_status.lower()
        if lease_status == "locked":
            access_level = "restricted"
        elif azure_properties.get("access_tier") == "Archive":
            access_level = "archived"
    
    return clean_for_json({
        "owner": str(owner),
        "created_by": str(azure_properties.get("metadata", {}).get("created_by", "azure_blob_discovery") if azure_properties else "azure_blob_discovery"),
        "last_updated_by": str(azure_properties.get("metadata", {}).get("last_updated_by", "azure_blob_discovery") if azure_properties else "azure_blob_discovery"),
        "last_updated_at": current_date,
        "access_level": access_level,
        "approval_status": "pending_review",
        "lease_status": azure_properties.get("lease_status") if azure_properties else None,
        "access_tier": azure_properties.get("access_tier") if azure_properties else None,
        "etag": azure_properties.get("etag", "").strip('"') if azure_properties and azure_properties.get("etag") else None
    })


def build_business_metadata(blob_info, azure_properties, file_extension, container_name, application_name=None):
    """Build business metadata for Azure Blob assets"""
    azure_metadata = azure_properties.get("metadata", {}) if azure_properties else {}
    azure_metadata = clean_for_json(azure_metadata)
    
    description = azure_metadata.get("description") or f"Azure Blob Storage file: {blob_info.get('name', 'unknown')}"
    business_owner = azure_metadata.get("business_owner") or azure_metadata.get("owner") or "workspace_owner@hdfc.bank.in"
    department = azure_metadata.get("department") or "Data Engineering"
    classification = azure_metadata.get("classification") or "internal"
    sensitivity_level = azure_metadata.get("sensitivity_level") or azure_metadata.get("sensitivity") or "medium"
    
    tags = []
    if azure_metadata.get("tags"):
        tags_value = azure_metadata["tags"]
        if isinstance(tags_value, str):
            tags = [t.strip() for t in tags_value.split(",")]
        elif isinstance(tags_value, list):
            tags = [str(t) for t in tags_value]
    
    if container_name and container_name not in tags:
        tags.append(container_name)
    
    return clean_for_json({
        "description": str(description),
        "data_type": file_extension or "unknown",
        "business_owner": str(business_owner),
        "department": str(department),
        "classification": str(classification),
        "sensitivity_level": str(sensitivity_level),
        "tags": tags,
        "application_name": application_name,
        "container": container_name,
        "content_language": azure_properties.get("content_language") if azure_properties else None,
        "azure_metadata_tags": azure_metadata
    })

