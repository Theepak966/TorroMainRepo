import hashlib
import json
import logging
from typing import Dict, Optional, List
import pyarrow.parquet as pq
import io
import csv
from collections import Counter

logger = logging.getLogger(__name__)


try:
    from .azure_dlp_client import detect_pii_in_column
    AZURE_DLP_AVAILABLE = True
except ImportError:
    logger.warning('FN:metadata_extractor_import AZURE_DLP_AVAILABLE:{}'.format(False))
    AZURE_DLP_AVAILABLE = False
    
    def detect_pii_in_column(column_name: str) -> Dict:
        return {"pii_detected": False, "pii_types": []}


def generate_file_hash(file_content: bytes) -> str:

    hash_obj = hashlib.shake_128(file_content)
    return hash_obj.hexdigest(16)


def extract_parquet_schema(file_content: bytes) -> Dict:



    try:
        parquet_file = pq.ParquetFile(io.BytesIO(file_content))
        schema = parquet_file.schema_arrow
        

        sample_data = None
        try:
            table = parquet_file.read_row_group(0) if parquet_file.num_row_groups > 0 else None
            if table is not None:

                sample_table = table.slice(0, min(10, len(table)))
                sample_data = sample_table.to_pandas().to_dict('list')
        except Exception as e:
            logger.debug('FN:extract_parquet_schema message:Could not read sample data error:{}'.format(str(e)))
            sample_data = None
        
        columns = []
        for i in range(len(schema)):
            field = schema.field(i)
            

            column_samples = None
            if sample_data and field.name in sample_data:
                column_samples = [str(val) for val in sample_data[field.name][:10] if val is not None]
            

            pii_result = detect_pii_in_column(field.name, column_samples)
            
            column_data = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }
            

            if AZURE_DLP_AVAILABLE and pii_result.get("pii_detected"):
                column_data["pii_detected"] = True
                column_data["pii_types"] = pii_result.get("pii_types", [])
            else:
                column_data["pii_detected"] = False
                column_data["pii_types"] = None
            
            columns.append(column_data)
        
        schema_dict = {
            "columns": columns,
            "num_columns": len(columns),
        }
        
        try:
            metadata = parquet_file.metadata
            if metadata and hasattr(metadata, 'num_rows'):
                schema_dict["num_rows"] = metadata.num_rows
        except:
            pass
        
        return schema_dict
        
    except Exception as e:
        logger.warning('FN:extract_parquet_schema file_content_size:{} error:{}'.format(len(file_content) if file_content else 0, str(e)))
        return {"columns": [], "num_columns": 0}


def generate_schema_hash(schema_json: Dict) -> str:
    schema_str = json.dumps(schema_json, sort_keys=True)
    hash_obj = hashlib.shake_128(schema_str.encode())
    return hash_obj.hexdigest(16)


def extract_csv_schema(file_content: bytes, sample_size: int = 0) -> Dict:

    try:
        content_str = file_content.decode('utf-8', errors='ignore')
        lines = content_str.split('\n')
        
        if not lines:
            return {"columns": [], "num_columns": 0}
        

        reader = csv.reader([lines[0]])
        headers = next(reader, None)
        
        if not headers:
            return {"columns": [], "num_columns": 0}
        

        sample_rows = []
        for line in lines[1:11]:
            if line.strip():
                try:
                    row_reader = csv.reader([line])
                    row = next(row_reader, None)
                    if row and len(row) == len(headers):
                        sample_rows.append(row)
                except:
                    continue
        
        columns = []
        
        for i, header in enumerate(headers):
            header = header.strip()
            if not header:
                header = f"column_{i+1}"
            

            column_samples = []
            if sample_rows:
                for row in sample_rows:
                    if i < len(row) and row[i]:
                        column_samples.append(str(row[i]).strip())
            

            pii_result = detect_pii_in_column(header, column_samples if column_samples else None)
            
            column_data = {
                "name": header,
                "type": "string",
                "nullable": True
            }
            

            if AZURE_DLP_AVAILABLE and pii_result.get("pii_detected"):
                column_data["pii_detected"] = True
                column_data["pii_types"] = pii_result.get("pii_types", [])
            else:
                column_data["pii_detected"] = False
                column_data["pii_types"] = None
            
            columns.append(column_data)
        
        return {
            "columns": columns,
            "num_columns": len(columns),
            "num_rows": None,
            "has_header": True,
            "delimiter": ","
        }
        
    except Exception as e:
        logger.warning('FN:extract_csv_schema file_content_size:{} sample_size:{} error:{}'.format(len(file_content) if file_content else 0, sample_size, str(e)))
        return {"columns": [], "num_columns": 0}


def infer_column_type(values: List[str]) -> str:
    if not values:
        return "string"
    
    non_empty = [v.strip() for v in values if v and v.strip()]
    if not non_empty:
        return "string"
    
    int_count = 0
    float_count = 0
    bool_count = 0
    date_count = 0
    
    for value in non_empty[:100]:
        value = value.strip()
        
        if value.lower() in ['true', 'false', 'yes', 'no', '1', '0']:
            bool_count += 1
        
        try:
            int(value)
            int_count += 1
        except ValueError:
            pass
        
        try:
            float(value)
            float_count += 1
        except ValueError:
            pass
        
        if '/' in value or '-' in value:
            parts = value.replace('/', '-').split('-')
            if len(parts) == 3:
                try:
                    int(parts[0])
                    int(parts[1])
                    int(parts[2])
                    date_count += 1
                except ValueError:
                    pass
    
    total = len(non_empty)
    threshold = total * 0.8
    
    if int_count >= threshold:
        return "int64"
    elif float_count >= threshold:
        return "double"
    elif bool_count >= threshold:
        return "bool"
    elif date_count >= threshold:
        return "date"
    else:
        return "string"


def extract_json_schema(file_content: bytes) -> Dict:
    try:
        content_str = file_content.decode('utf-8', errors='ignore').strip()
        
        if not content_str:
            return {"columns": [], "num_columns": 0}
        
        try:
            data = json.loads(content_str)
        except json.JSONDecodeError:
            return {"columns": [], "num_columns": 0, "error": "Invalid JSON"}
        
        columns = []
        
        if isinstance(data, dict):

            for key in data.keys():

                sample_value = str(data.get(key, "")) if data.get(key) is not None else None
                column_samples = [sample_value] if sample_value else None
                

                pii_result = detect_pii_in_column(str(key), column_samples)
                
                column_data = {
                    "name": str(key),
                    "type": "string",
                    "nullable": True
                }
                

                if AZURE_DLP_AVAILABLE and pii_result.get("pii_detected"):
                    column_data["pii_detected"] = True
                    column_data["pii_types"] = pii_result.get("pii_types", [])
                else:
                    column_data["pii_detected"] = False
                    column_data["pii_types"] = None
                
                columns.append(column_data)
        elif isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            if isinstance(first_item, dict):

                for key in first_item.keys():

                    column_samples = []
                    for item in data[:10]:
                        if isinstance(item, dict) and key in item and item[key] is not None:
                            column_samples.append(str(item[key]))
                    

                    pii_result = detect_pii_in_column(str(key), column_samples if column_samples else None)
                    
                    column_data = {
                        "name": str(key),
                        "type": "string",
                        "nullable": True
                    }
                    

                    if AZURE_DLP_AVAILABLE and pii_result.get("pii_detected"):
                        column_data["pii_detected"] = True
                        column_data["pii_types"] = pii_result.get("pii_types", [])
                    else:
                        column_data["pii_detected"] = False
                        column_data["pii_types"] = None
                    
                    columns.append(column_data)
            else:
                columns.append({
                    "name": "value",
                    "type": "string",
                    "nullable": True,
                    "pii_detected": False,
                    "pii_types": None
                })
        
        return {
            "columns": columns,
            "num_columns": len(columns),
            "structure": "object" if isinstance(data, dict) else "array",
            "num_items": len(data) if isinstance(data, list) else None
        }
        
    except Exception as e:
        logger.warning('FN:extract_json_schema file_content_size:{} error:{}'.format(len(file_content) if file_content else 0, str(e)))
        return {"columns": [], "num_columns": 0}


def infer_json_type(value) -> str:
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int64"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        if len(value) > 0:
            return f"array<{infer_json_type(value[0])}>"
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return "string"


def extract_file_metadata(blob_info: Dict, file_content: Optional[bytes] = None) -> Dict:
    from datetime import datetime
    
    file_name = blob_info["name"]
    file_extension = "." + file_name.split(".")[-1] if "." in file_name else ""
    file_format = file_extension[1:].lower() if file_extension else "unknown"
    
    file_hash = None
    if file_content:
        file_hash = generate_file_hash(file_content)
    else:
        file_hash = generate_file_hash(b"")
    
    file_metadata = {
        "basic": {
            "name": file_name,
            "extension": file_extension,
            "format": file_format,
            "size_bytes": blob_info["size"],
            "content_type": blob_info.get("content_type", "application/octet-stream"),
            "mime_type": blob_info.get("content_type", "application/octet-stream")
        },
        "hash": {
            "algorithm": "shake128",
            "value": file_hash,
            "computed_at": datetime.utcnow().isoformat() + "Z"
        },
        "timestamps": {
            "created_at": blob_info["created_at"].isoformat() if blob_info.get("created_at") else None,
            "last_modified": blob_info["last_modified"].isoformat() if blob_info.get("last_modified") else None
        }
    }
    
    format_specific = {}
    if file_format == "parquet" and file_content:
        try:
            parquet_schema = extract_parquet_schema(file_content)
            format_specific["parquet"] = {
                "row_groups": parquet_schema.get("num_rows", 0),
                "compression": "unknown",
                "schema_version": "1.0"
            }
        except Exception as e:
            logger.warning('FN:extract_file_metadata file_name:{} file_format:{} error:{}'.format(file_name, file_format, str(e)))
    elif file_format == "csv":
        format_specific["csv"] = {
            "delimiter": ",",
            "has_header": True,
            "encoding": "utf-8"
        }
    elif file_format == "json":
        format_specific["json"] = {
            "format": "unknown"
        }
    
    if format_specific:
        file_metadata["format_specific"] = format_specific
    
    schema_json = None
    schema_hash = None
    
    if file_format == "parquet" and file_content:
        try:
            schema_json = extract_parquet_schema(file_content)
            schema_hash = generate_schema_hash(schema_json)
        except Exception as e:
            logger.warning('FN:extract_file_metadata file_name:{} file_format:{} error:{}'.format(file_name, file_format, str(e)))
            schema_json = {}
            schema_hash = hashlib.shake_128(b"").hexdigest(16)
    elif file_format == "csv" and file_content:
        try:
            schema_json = extract_csv_schema(file_content)
            schema_hash = generate_schema_hash(schema_json)
        except Exception as e:
            logger.warning('FN:extract_file_metadata file_name:{} file_format:{} error:{}'.format(file_name, file_format, str(e)))
            schema_json = {}
            schema_hash = hashlib.shake_128(b"").hexdigest(16)
    elif file_format == "json" and file_content:
        try:
            schema_json = extract_json_schema(file_content)
            schema_hash = generate_schema_hash(schema_json)
        except Exception as e:
            logger.warning('FN:extract_file_metadata file_name:{} file_format:{} error:{}'.format(file_name, file_format, str(e)))
            schema_json = {}
            schema_hash = hashlib.shake_128(b"").hexdigest(16)
    else:
        schema_json = {}
        schema_hash = hashlib.shake_128(json.dumps({}).encode()).hexdigest(16)
    
    storage_metadata = {
        "azure": {
            "type": blob_info.get("blob_type", "Block blob"),
            "etag": blob_info.get("etag", "").strip('"') if blob_info.get("etag") else None,
            "access_tier": blob_info.get("access_tier"),
            "creation_time": blob_info.get("created_at").isoformat() if blob_info.get("created_at") else None,
            "last_modified": blob_info.get("last_modified").isoformat() if blob_info.get("last_modified") else None,
            "lease_status": blob_info.get("lease_status"),
            "content_encoding": blob_info.get("content_encoding"),
            "content_language": blob_info.get("content_language"),
            "cache_control": blob_info.get("cache_control"),
            "metadata": blob_info.get("metadata", {})
        }
    }
    
    return {
        "file_metadata": file_metadata,
        "schema_json": schema_json,
        "schema_hash": schema_hash,
        "file_hash": file_hash,
        "storage_metadata": storage_metadata
    }
