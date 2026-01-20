"""
Enhanced manual lineage service with schema/table level support.
Supports legacy systems that don't have automated lineage capabilities.
"""

from typing import List, Dict, Optional
from datetime import datetime
import sys
import os
import csv
import json
import io
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models_lineage.models_lineage import Dataset, Process, LineageEdge, ColumnLineage
from services.lineage_ingestion import LineageIngestionService

logger = logging.getLogger(__name__)


class ManualLineageService:
    """Enhanced manual lineage service with schema/table level support"""
    
    def __init__(self):
        self.ingestion_service = LineageIngestionService()
    
    def create_schema_level_lineage(
        self,
        source_schema: str,
        target_schema: str,
        source_catalog: str,
        target_catalog: str,
        relationship_type: str = 'schema_migration',
        description: Optional[str] = None,
        user_id: str = 'manual'
    ) -> Dict:
        """
        Create high-level schema-to-schema lineage for legacy systems.
        Useful for regulatory compliance when detailed lineage is unavailable.
        """
        db = SessionLocal()
        try:
            # Find all datasets in source schema
            source_datasets = db.query(Dataset).filter(
                Dataset.catalog == source_catalog,
                Dataset.schema_name == source_schema
            ).all()
            
            # Find all datasets in target schema
            target_datasets = db.query(Dataset).filter(
                Dataset.catalog == target_catalog,
                Dataset.schema_name == target_schema
            ).all()
            
            if not source_datasets or not target_datasets:
                return {
                    'status': 'error',
                    'message': f'No datasets found in source schema {source_catalog}.{source_schema} or target schema {target_catalog}.{target_schema}'
                }
            
            # Create process for schema-level transformation
            process_urn = f"urn:process:manual:schema_{source_schema}_to_{target_schema}"
            process_data = {
                'urn': process_urn,
                'name': f"Schema Migration: {source_schema} -> {target_schema}",
                'type': 'manual',
                'source_system': 'manual',
                'process_definition': {
                    'description': description or f"High-level schema migration from {source_schema} to {target_schema}",
                    'level': 'schema',
                    'source_schema': source_schema,
                    'target_schema': target_schema
                }
            }
            
            # Create edges: all source datasets -> process -> all target datasets
            source_urns = [d.urn for d in source_datasets]
            target_urns = [d.urn for d in target_datasets]
            
            result = self.ingestion_service.ingest_process_lineage(
                process_data=process_data,
                input_datasets=source_urns,
                output_datasets=target_urns,
                ingestion_id=f"manual_schema_{source_schema}_{target_schema}",
                user_id=user_id
            )
            
            return {
                'status': 'success',
                'process_urn': process_urn,
                'source_datasets_count': len(source_datasets),
                'target_datasets_count': len(target_datasets),
                'edges_created': result.get('edges_created', 0)
            }
            
        finally:
            db.close()
    
    def create_table_level_lineage(
        self,
        source_tables: List[str],  # List of table URNs or qualified names
        target_tables: List[str],
        process_name: str,
        relationship_type: str = 'table_transformation',
        column_mappings: Optional[List[Dict]] = None,
        user_id: str = 'manual'
    ) -> Dict:
        """
        Create table-level lineage for ETL/ELT pipelines.
        Supports column-level mappings if provided.
        """
        # Convert table names to URNs if needed
        source_urns = [self._ensure_urn(urn) for urn in source_tables]
        target_urns = [self._ensure_urn(urn) for urn in target_tables]
        
        process_urn = f"urn:process:manual:table_{process_name.replace(' ', '_').lower()}"
        process_data = {
            'urn': process_urn,
            'name': process_name,
            'type': 'manual',
            'source_system': 'manual',
            'process_definition': {
                'level': 'table',
                'source_tables': source_tables,
                'target_tables': target_tables
            }
        }
        
        result = self.ingestion_service.ingest_process_lineage(
            process_data=process_data,
            input_datasets=source_urns,
            output_datasets=target_urns,
            column_lineage=column_mappings,
            ingestion_id=f"manual_table_{process_name}",
            user_id=user_id
        )
        
        return {
            'status': 'success',
            'process_urn': process_urn,
            'edges_created': result.get('edges_created', 0),
            'column_lineage_ingested': result.get('column_lineage_ingested', 0)
        }
    
    def bulk_upload_lineage(self, lineage_file: bytes, file_format: str = 'csv') -> Dict:
        """
        Bulk upload lineage from CSV/JSON file.
        
        CSV format:
        source_table,target_table,process_name,source_column,target_column,transformation_type
        
        JSON format:
        [
            {
                "source_table": "...",
                "target_table": "...",
                "process_name": "...",
                "column_lineage": [...]
            }
        ]
        """
        if file_format == 'csv':
            reader = csv.DictReader(io.StringIO(lineage_file.decode('utf-8')))
            lineage_records = list(reader)
        else:
            lineage_records = json.loads(lineage_file.decode('utf-8'))
        
        ingested_count = 0
        errors = []
        
        for record in lineage_records:
            try:
                # Group by process
                process_name = record.get('process_name', 'Bulk Upload Process')
                source_tables = record.get('source_table', '').split(';')
                target_tables = record.get('target_table', '').split(';')
                
                column_mappings = None
                if record.get('source_column') and record.get('target_column'):
                    column_mappings = [{
                        'source_column': record['source_column'],
                        'target_column': record['target_column'],
                        'transformation_type': record.get('transformation_type', 'pass_through')
                    }]
                
                result = self.create_table_level_lineage(
                    source_tables=source_tables,
                    target_tables=target_tables,
                    process_name=process_name,
                    column_mappings=column_mappings
                )
                
                if result['status'] == 'success':
                    ingested_count += 1
            except Exception as e:
                errors.append({'record': record, 'error': str(e)})
                logger.error(f"Error processing lineage record: {e}", exc_info=True)
        
        return {
            'status': 'completed',
            'ingested_count': ingested_count,
            'error_count': len(errors),
            'errors': errors
        }
    
    def _ensure_urn(self, identifier: str) -> str:
        """Ensure identifier is in URN format"""
        if identifier.startswith('urn:'):
            return identifier
        
        # Try to parse as qualified name
        parts = identifier.split('.')
        if len(parts) >= 3:
            return f"urn:dataset:oracle:{identifier}"
        elif len(parts) == 2:
            return f"urn:dataset:azure_blob:{identifier}"
        
        return f"urn:dataset:unknown:{identifier}"










