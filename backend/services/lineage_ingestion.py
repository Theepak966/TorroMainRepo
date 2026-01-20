"""
Idempotent lineage ingestion service with full audit logging.
No auto-inference - all lineage must be explicitly provided.
"""

from typing import List, Dict, Optional
from datetime import datetime
import sys
import os
import hashlib
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models_lineage.models_lineage import Dataset, Process, LineageEdge, ColumnLineage, LineageAuditLog

logger = logging.getLogger(__name__)


class LineageIngestionService:
    """Idempotent lineage ingestion with full audit logging"""
    
    def ingest_process_lineage(
        self,
        process_data: Dict,
        input_datasets: List[str],  # List of dataset URNs
        output_datasets: List[str],  # List of dataset URNs
        column_lineage: Optional[List[Dict]] = None,
        ingestion_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Dict:
        """
        Ingest a process and its lineage edges.
        Idempotent: Uses ingestion_id to prevent duplicates.
        """
        db = SessionLocal()
        try:
            # Generate ingestion ID if not provided
            if not ingestion_id:
                ingestion_id = self._generate_ingestion_id(process_data, input_datasets, output_datasets)
            
            # Check if already ingested
            existing_edge = db.query(LineageEdge).filter(
                LineageEdge.ingestion_id == ingestion_id
            ).first()
            
            if existing_edge:
                logger.info(f"Lineage already ingested: {ingestion_id}")
                # Return existing edge count
                edge_count = db.query(LineageEdge).filter(
                    LineageEdge.ingestion_id == ingestion_id
                ).count()
                return {
                    'status': 'skipped', 
                    'ingestion_id': ingestion_id,
                    'edges_created': edge_count
                }
            
            # Create/update Process
            process_urn = process_data['urn']
            process = db.query(Process).filter(Process.urn == process_urn).first()
            
            if not process:
                process = Process(
                    urn=process_urn,
                    name=process_data['name'],
                    type=process_data['type'],
                    source_system=process_data.get('source_system'),
                    job_id=process_data.get('job_id'),
                    job_name=process_data.get('job_name'),
                    process_definition=process_data.get('process_definition'),
                    created_by=user_id or 'system'
                )
                db.add(process)
                self._audit_log(db, 'create', 'process', process_urn, None, process_data, user_id, ingestion_id)
            else:
                # Update existing process
                old_data = self._process_to_dict(process)
                process.name = process_data.get('name', process.name)
                process.updated_at = datetime.utcnow()
                new_data = self._process_to_dict(process)
                self._audit_log(db, 'update', 'process', process_urn, old_data, new_data, user_id, ingestion_id)
            
            # Ensure datasets exist
            all_dataset_urns = set(input_datasets + output_datasets)
            for dataset_urn in all_dataset_urns:
                dataset = db.query(Dataset).filter(Dataset.urn == dataset_urn).first()
                if not dataset:
                    # Auto-create dataset if not exists (metadata-only)
                    dataset = Dataset(
                        urn=dataset_urn,
                        name=self._extract_name_from_urn(dataset_urn),
                        type='table',  # Default, should be provided
                        created_by=user_id or 'system'
                    )
                    db.add(dataset)
                    self._audit_log(db, 'create', 'dataset', dataset_urn, None, {'urn': dataset_urn}, user_id, ingestion_id)
            
            # Create lineage edges: input_dataset -> process -> output_dataset
            edge_specs = []  # Store edge specs for later query
            for input_urn in input_datasets:
                for output_urn in output_datasets:
                    edge = LineageEdge(
                        source_urn=input_urn,
                        process_urn=process_urn,
                        target_urn=output_urn,
                        relationship_type=process_data.get('relationship_type', 'transformation'),
                        edge_metadata=process_data.get('edge_metadata'),
                        ingestion_id=ingestion_id,
                        created_by=user_id or 'system'
                    )
                    db.add(edge)
                    edge_specs.append((input_urn, process_urn, output_urn))
            
            # Commit edges first to get IDs
            db.commit()
            
            # Log audit entries for edges (re-query to get IDs)
            for input_urn, proc_urn, output_urn in edge_specs:
                edge = db.query(LineageEdge).filter(
                    LineageEdge.source_urn == input_urn,
                    LineageEdge.process_urn == proc_urn,
                    LineageEdge.target_urn == output_urn,
                    LineageEdge.ingestion_id == ingestion_id
                ).first()
                if edge:
                    self._audit_log(db, 'create', 'edge', f"{input_urn}->{proc_urn}->{output_urn}", 
                                  None, {'edge_id': edge.id}, user_id, ingestion_id)
            
            # Ingest column lineage if provided (separate storage)
            column_lineage_count = 0
            if column_lineage and edge_specs:
                for input_urn, proc_urn, output_urn in edge_specs:
                    edge = db.query(LineageEdge).filter(
                        LineageEdge.source_urn == input_urn,
                        LineageEdge.process_urn == proc_urn,
                        LineageEdge.target_urn == output_urn,
                        LineageEdge.ingestion_id == ingestion_id
                    ).first()
                    if not edge or not edge.id:
                        logger.warning(f"Could not find edge for column lineage: {input_urn}->{output_urn}")
                        continue
                    for col_mapping in column_lineage:
                        col_lineage = ColumnLineage(
                            edge_id=edge.id,
                            source_column=col_mapping['source_column'],
                            target_column=col_mapping['target_column'],
                            source_table=col_mapping.get('source_table'),
                            target_table=col_mapping.get('target_table'),
                            transformation_type=col_mapping.get('transformation_type', 'pass_through'),
                            transformation_expression=col_mapping.get('transformation_expression')
                        )
                        db.add(col_lineage)
                        column_lineage_count += 1
                db.commit()
            
            return {
                'status': 'success',
                'ingestion_id': ingestion_id,
                'process_urn': process_urn,
                'edges_created': len(edge_specs),
                'column_lineage_ingested': column_lineage_count
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"Lineage ingestion failed: {e}", exc_info=True)
            raise
        finally:
            db.close()
    
    def _generate_ingestion_id(self, process_data: Dict, inputs: List[str], outputs: List[str]) -> str:
        """Generate deterministic ingestion ID for idempotency"""
        content = f"{process_data['urn']}:{sorted(inputs)}:{sorted(outputs)}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    def _extract_name_from_urn(self, urn: str) -> str:
        """Extract name from URN (e.g., 'urn:dataset:oracle:db.schema.table' -> 'table')"""
        parts = urn.split(':')
        if len(parts) > 1:
            return parts[-1].split('.')[-1]
        return urn
    
    def _process_to_dict(self, process: Process) -> Dict:
        return {
            'urn': process.urn,
            'name': process.name,
            'type': process.type,
            'source_system': process.source_system
        }
    
    def _audit_log(self, db, action: str, entity_type: str, entity_urn: str, 
                   old_data: Optional[Dict], new_data: Dict, user_id: Optional[str], ingestion_id: Optional[str]):
        audit = LineageAuditLog(
            action=action,
            entity_type=entity_type,
            entity_urn=entity_urn,
            old_data=old_data,
            new_data=new_data,
            user_id=user_id or 'system',
            ingestion_id=ingestion_id or 'unknown'
        )
        db.add(audit)


