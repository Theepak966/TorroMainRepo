"""
SQL Lineage Integration Service
Integrates SQL parsing with the new lineage system to automatically extract
and ingest lineage from SQL queries, views, stored procedures, etc.

IMPORTANT: This service does NOT read SQL files from blob storage.
It extracts SQL from:
1. Generated SQL (from asset metadata - view_sql_commands)
2. Technical metadata (manually added or from external systems)
3. API input (from ETL jobs, Airflow, etc.)
4. Stored procedures (PL/SQL, T-SQL)

Only requires Storage Blob Reader role for metadata access, not SQL file reading.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models import Asset, DataDiscovery
from models_lineage.models_lineage import Dataset
from services.lineage_ingestion import LineageIngestionService
from services.asset_lineage_integration import AssetLineageIntegration
from utils.sql_lineage_extractor import SQLLineageExtractor
from utils.stored_procedure_parser import StoredProcedureParser

logger = logging.getLogger(__name__)


class SQLLineageIntegration:
    """Integrates SQL parsing with the new lineage system"""
    
    def __init__(self):
        self.sql_extractor = SQLLineageExtractor()
        self.procedure_parser = StoredProcedureParser()
        self.lineage_ingestion = LineageIngestionService()
        self.asset_integration = AssetLineageIntegration()
    
    def parse_and_ingest_sql_lineage(
        self,
        sql_query: str,
        target_asset_id: Optional[str] = None,
        target_urn: Optional[str] = None,
        source_system: str = 'sql_parsing',
        job_id: Optional[str] = None,
        job_name: Optional[str] = None,
        dialect: str = 'mysql',
        user_id: str = 'system'
    ) -> Dict:
        """
        Parse SQL query and ingest lineage into the new lineage system.
        
        Args:
            sql_query: SQL query to parse
            target_asset_id: Asset ID of the target (if known)
            target_urn: Dataset URN of the target (if known)
            source_system: Source system name (default: 'sql_parsing')
            job_id: Job/task ID
            job_name: Job/task name
            dialect: SQL dialect (default: 'mysql')
            user_id: User ID for audit logging
        
        Returns:
            Dict with parsing results and ingestion status
        """
        db = SessionLocal()
        try:
            # Parse SQL to extract lineage
            lineage_result = self.sql_extractor.extract_lineage(sql_query, dialect)
            
            if not lineage_result.get('source_tables') and not lineage_result.get('target_table'):
                return {
                    'status': 'no_lineage',
                    'message': 'No source or target tables found in SQL query',
                    'parsed_lineage': lineage_result
                }
            
            # Resolve target dataset URN
            target_urn_resolved = target_urn
            if not target_urn_resolved and target_asset_id:
                target_urn_resolved = self.asset_integration.get_dataset_urn_for_asset(target_asset_id)
            
            if not target_urn_resolved and lineage_result.get('target_table'):
                # Try to find asset by name
                target_table = lineage_result['target_table']
                target_asset = db.query(Asset).filter(
                    Asset.name.ilike(f'%{target_table}%')
                ).first()
                if target_asset:
                    target_urn_resolved = self.asset_integration.get_dataset_urn_for_asset(target_asset.id)
            
            # Resolve source dataset URNs
            source_urns = []
            for source_table in lineage_result.get('source_tables', []):
                source_asset = db.query(Asset).filter(
                    Asset.name.ilike(f'%{source_table}%')
                ).first()
                if source_asset:
                    source_urn = self.asset_integration.get_dataset_urn_for_asset(source_asset.id)
                    if source_urn:
                        source_urns.append(source_urn)
            
            if not source_urns:
                return {
                    'status': 'no_source_datasets',
                    'message': 'No source datasets found in database',
                    'parsed_lineage': lineage_result,
                    'target_urn': target_urn_resolved
                }
            
            if not target_urn_resolved:
                return {
                    'status': 'no_target_dataset',
                    'message': 'Target dataset not found in database',
                    'parsed_lineage': lineage_result,
                    'source_urns': source_urns
                }
            
            # Prepare column lineage
            column_lineage = []
            for col_lineage in lineage_result.get('column_lineage', []):
                column_lineage.append({
                    'source_column': col_lineage.get('source_column'),
                    'target_column': col_lineage.get('target_column'),
                    'transformation_type': col_lineage.get('transformation_type', 'pass_through')
                })
            
            # Create process URN
            process_urn = f"urn:process:sql:{source_system}:{job_id or 'unknown'}:{datetime.utcnow().timestamp()}"
            process_name = job_name or f"SQL {lineage_result.get('query_type', 'TRANSFORMATION')}"
            
            # Ingest into new lineage system
            ingestion_result = self.lineage_ingestion.ingest_process_lineage(
                process_data={
                    'urn': process_urn,
                    'name': process_name,
                    'type': 'sql',
                    'source_system': source_system,
                    'job_id': job_id,
                    'job_name': job_name,
                    'process_definition': {
                        'sql_query': sql_query,
                        'query_type': lineage_result.get('query_type'),
                        'dialect': dialect,
                        'extraction_method': 'sql_parsing',
                        'confidence_score': lineage_result.get('confidence_score', 0.8)
                    }
                },
                input_datasets=source_urns,
                output_datasets=[target_urn_resolved],
                column_lineage=column_lineage if column_lineage else None,
                ingestion_id=f"sql_{job_id}_{hash(sql_query) % 1000000}" if job_id else None,
                user_id=user_id
            )
            
            return {
                'status': 'success',
                'parsed_lineage': lineage_result,
                'ingestion_result': ingestion_result,
                'source_urns': source_urns,
                'target_urn': target_urn_resolved,
                'process_urn': process_urn
            }
            
        except Exception as e:
            logger.error(f"Failed to parse and ingest SQL lineage: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'parsed_lineage': lineage_result if 'lineage_result' in locals() else None
            }
        finally:
            db.close()
    
    def scan_asset_sql_and_extract_lineage(
        self,
        asset_id: str,
        user_id: str = 'system'
    ) -> Dict:
        """
        Scan an asset for SQL queries and automatically extract lineage.
        
        SQL Sources (in order checked):
        1. Generated SQL from view_sql_commands (in discovery.schema_json)
           - These are CREATE VIEW statements generated from asset metadata
           - Generated by generate_view_sql_commands() based on columns and masking logic
        2. Technical metadata fields (sql_query, view_definition, create_statement, ddl)
           - Manually added or from external systems
        
        IMPORTANT: Does NOT read SQL files from blob storage.
        Only requires Storage Blob Reader role for metadata access.
        
        Args:
            asset_id: Asset ID to scan
            user_id: User ID for audit logging
        
        Returns:
            Dict with scan results and extracted lineage
        """
        db = SessionLocal()
        try:
            asset = db.query(Asset).filter(Asset.id == asset_id).first()
            if not asset:
                return {'status': 'error', 'error': f'Asset {asset_id} not found'}
            
            # Get discovery record for SQL commands
            discovery = db.query(DataDiscovery).filter(
                DataDiscovery.asset_id == asset_id
            ).order_by(DataDiscovery.id.desc()).first()
            
            results = {
                'asset_id': asset_id,
                'asset_name': asset.name,
                'sql_queries_found': 0,
                'lineage_extracted': 0,
                'sql_sources': [],
                'errors': []
            }
            
            # Source 1: Check for GENERATED view SQL commands (from asset metadata)
            # These are CREATE VIEW statements generated by generate_view_sql_commands()
            # based on the asset's columns and PII masking logic
            if discovery and discovery.schema_json:
                schema_data = discovery.schema_json
                if isinstance(schema_data, dict):
                    # Check for view_sql_commands (generated SQL)
                    view_sql = schema_data.get('view_sql_commands', {})
                    if isinstance(view_sql, dict):
                        for sql_type, sql_query in view_sql.items():
                            if sql_type in ['analytical_sql', 'operational_sql'] and sql_query:
                                results['sql_queries_found'] += 1
                                results['sql_sources'].append({
                                    'type': sql_type,
                                    'source': 'generated_from_metadata',
                                    'description': f'Generated CREATE VIEW from asset columns and masking logic'
                                })
                                try:
                                    result = self.parse_and_ingest_sql_lineage(
                                        sql_query=sql_query,
                                        target_asset_id=asset_id,
                                        source_system='asset_view',
                                        job_name=f"{asset.name} {sql_type}",
                                        user_id=user_id
                                    )
                                    if result.get('status') == 'success':
                                        results['lineage_extracted'] += 1
                                    else:
                                        results['errors'].append({
                                            'sql_type': sql_type,
                                            'source': 'generated_from_metadata',
                                            'error': result.get('message', result.get('error', 'Unknown error'))
                                        })
                                except Exception as e:
                                    results['errors'].append({
                                        'sql_type': sql_type,
                                        'source': 'generated_from_metadata',
                                        'error': str(e)
                                    })
            
            # Source 2: Check technical_metadata for SQL (manually added or from external systems)
            if asset.technical_metadata:
                tech_meta = asset.technical_metadata
                if isinstance(tech_meta, dict):
                    # Check for SQL in various fields
                    sql_fields = ['sql_query', 'view_definition', 'create_statement', 'ddl']
                    for field in sql_fields:
                        sql_query = tech_meta.get(field)
                        if sql_query and isinstance(sql_query, str):
                            results['sql_queries_found'] += 1
                            results['sql_sources'].append({
                                'type': field,
                                'source': 'technical_metadata',
                                'description': f'SQL from asset technical_metadata.{field}'
                            })
                            try:
                                result = self.parse_and_ingest_sql_lineage(
                                    sql_query=sql_query,
                                    target_asset_id=asset_id,
                                    source_system='asset_metadata',
                                    job_name=f"{asset.name} {field}",
                                    user_id=user_id
                                )
                                if result.get('status') == 'success':
                                    results['lineage_extracted'] += 1
                                else:
                                    results['errors'].append({
                                        'sql_type': field,
                                        'source': 'technical_metadata',
                                        'error': result.get('message', result.get('error', 'Unknown error'))
                                    })
                            except Exception as e:
                                results['errors'].append({
                                    'sql_type': field,
                                    'source': 'technical_metadata',
                                    'error': str(e)
                                })
            
            results['status'] = 'success' if results['lineage_extracted'] > 0 else 'no_lineage'
            return results
            
        except Exception as e:
            logger.error(f"Failed to scan asset SQL: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
        finally:
            db.close()
