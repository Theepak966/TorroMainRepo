"""
Best-in-class data lineage models for bank environment.
First-class entities: Dataset and Process
Relationships: Dataset --(CONSUMED_BY)--> Process --(PRODUCES)--> Dataset
"""

from sqlalchemy import Column, String, Integer, DateTime, Text, JSON, ForeignKey, Index, UniqueConstraint, BigInteger, Boolean, DECIMAL
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
import sys
import os

try:
    from ...database import Base
except (ImportError, ValueError):
    try:
        from ..database import Base
    except (ImportError, ValueError):
        # Fallback for direct import
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from database import Base


class Dataset(Base):
    """First-class entity: Represents a data asset (table, file, view)"""
    __tablename__ = "lineage_datasets"
    
    urn = Column(String(512), primary_key=True)  # Unique Resource Name
    name = Column(String(255), nullable=False, index=True)
    type = Column(String(50), nullable=False)  # table, file, view, stream
    catalog = Column(String(255), index=True)
    schema_name = Column(String(255))
    
    # Storage metadata (read-only, metadata-only access)
    storage_type = Column(String(50))  # oracle, azure_blob, etc.
    storage_location = Column(JSON)  # {account, container, path}
    
    # Optional: Table-level lineage metadata
    table_lineage_enabled = Column(Boolean, default=False, index=True)
    
    # Temporal lineage
    valid_from = Column(DateTime, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)
    
    # Audit
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_by = Column(String(255))
    
    # Relationships
    consumed_by = relationship("LineageEdge", foreign_keys="LineageEdge.source_urn", back_populates="source_dataset")
    produces = relationship("LineageEdge", foreign_keys="LineageEdge.target_urn", back_populates="target_dataset")
    
    __table_args__ = (
        Index('idx_dataset_catalog_schema', 'catalog', 'schema_name', 'name'),
        Index('idx_dataset_validity', 'valid_from', 'valid_to'),
    )


class Process(Base):
    """First-class entity: Represents a transformation process (ETL, Spark job, SQL)"""
    __tablename__ = "lineage_processes"
    
    urn = Column(String(512), primary_key=True)
    name = Column(String(255), nullable=False, index=True)
    type = Column(String(50), nullable=False)  # etl, spark, sql, manual
    
    # Process metadata
    source_system = Column(String(100), index=True)  # spark, airflow, oracle, manual
    job_id = Column(String(255), index=True)
    job_name = Column(String(255))
    
    # Process definition (optional, for audit)
    process_definition = Column(JSON)  # {sql, config, etc.}
    
    # Temporal
    valid_from = Column(DateTime, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)
    
    # Audit
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_by = Column(String(255))
    
    # Relationships
    consumes = relationship("LineageEdge", foreign_keys="LineageEdge.process_urn", back_populates="process")
    
    __table_args__ = (
        Index('idx_process_source_job', 'source_system', 'job_id'),
        Index('idx_process_validity', 'valid_from', 'valid_to'),
    )


class LineageEdge(Base):
    """Precomputed lineage edge: Dataset -> Process -> Dataset"""
    __tablename__ = "lineage_edges"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Graph structure: Dataset --(CONSUMED_BY)--> Process --(PRODUCES)--> Dataset
    source_urn = Column(String(512), ForeignKey('lineage_datasets.urn', ondelete='CASCADE'), nullable=False, index=True)
    process_urn = Column(String(512), ForeignKey('lineage_processes.urn', ondelete='CASCADE'), nullable=False, index=True)
    target_urn = Column(String(512), ForeignKey('lineage_datasets.urn', ondelete='CASCADE'), nullable=False, index=True)
    
    # Edge metadata
    relationship_type = Column(String(50), default='transformation')  # transformation, copy, view
    edge_metadata = Column(JSON)  # Additional edge-level metadata
    
    # Temporal lineage (append-only)
    valid_from = Column(DateTime, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)
    
    # Audit
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(String(255))
    ingestion_id = Column(String(255), index=True)  # For idempotency
    
    # Relationships
    source_dataset = relationship("Dataset", foreign_keys=[source_urn], back_populates="consumed_by")
    process = relationship("Process", foreign_keys=[process_urn], back_populates="consumes")
    target_dataset = relationship("Dataset", foreign_keys=[target_urn], back_populates="produces")
    
    __table_args__ = (
        UniqueConstraint('source_urn', 'process_urn', 'target_urn', 'valid_from', name='unique_lineage_edge'),
        Index('idx_edge_source', 'source_urn', 'valid_from', 'valid_to'),
        Index('idx_edge_target', 'target_urn', 'valid_from', 'valid_to'),
        Index('idx_edge_process', 'process_urn', 'valid_from', 'valid_to'),
        Index('idx_edge_validity', 'valid_from', 'valid_to'),
        Index('idx_edge_ingestion', 'ingestion_id'),
        Index('idx_edge_composite', 'source_urn', 'target_urn', 'valid_from'),
    )


class ColumnLineage(Base):
    """Separate storage for column-level lineage (lazy-loaded, not graph-traversed)"""
    __tablename__ = "lineage_column_lineage"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Reference to edge (not part of graph traversal)
    edge_id = Column(Integer, ForeignKey('lineage_edges.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Column mappings
    source_column = Column(String(255), nullable=False)
    target_column = Column(String(255), nullable=False)
    
    # Optional: Table-level if enabled
    source_table = Column(String(255))
    target_table = Column(String(255))
    
    # Transformation details
    transformation_type = Column(String(50))  # pass_through, aggregate, join, function
    transformation_expression = Column(Text)  # SQL expression if available
    
    # Temporal
    valid_from = Column(DateTime, default=datetime.utcnow, index=True)
    valid_to = Column(DateTime, nullable=True, index=True)
    
    # Audit
    created_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (
        Index('idx_col_edge', 'edge_id', 'valid_from', 'valid_to'),
        Index('idx_col_source', 'source_column', 'source_table'),
        Index('idx_col_target', 'target_column', 'target_table'),
    )


class LineageAuditLog(Base):
    """Full audit trail for all lineage operations"""
    __tablename__ = "lineage_audit_log"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    action = Column(String(50), nullable=False, index=True)  # create, update, delete
    entity_type = Column(String(50), nullable=False)  # dataset, process, edge
    entity_urn = Column(String(512), nullable=False, index=True)
    
    # Change tracking
    old_data = Column(JSON)
    new_data = Column(JSON)
    
    # Audit metadata
    user_id = Column(String(255), index=True)
    source_system = Column(String(100))
    ingestion_id = Column(String(255), index=True)
    
    created_at = Column(DateTime, server_default=func.now(), index=True)
    
    __table_args__ = (
        Index('idx_audit_entity', 'entity_type', 'entity_urn', 'created_at'),
        Index('idx_audit_user', 'user_id', 'created_at'),
    )










