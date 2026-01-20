from sqlalchemy import Column, Integer, String, DateTime, JSON, Text, ForeignKey, DECIMAL, UniqueConstraint, BigInteger, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import sys
import os

try:
    from .database import Base
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from database import Base

class Asset(Base):
    __tablename__ = "assets"

    id = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(255), nullable=False)
    catalog = Column(String(255))
    connector_id = Column(String(255))
    discovered_at = Column(DateTime, server_default=func.now())
    technical_metadata = Column(JSON)
    operational_metadata = Column(JSON)
    business_metadata = Column(JSON)
    columns = Column(JSON)
    

    source_lineage = relationship("LineageRelationship", foreign_keys="LineageRelationship.source_asset_id", back_populates="source_asset")
    target_lineage = relationship("LineageRelationship", foreign_keys="LineageRelationship.target_asset_id", back_populates="target_asset")

class Connection(Base):
    __tablename__ = "connections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    connector_type = Column(String(255), nullable=False)
    connection_type = Column(String(255))
    config = Column(JSON)
    status = Column(String(50), default="active")
    created_at = Column(DateTime, server_default=func.now())

class LineageRelationship(Base):
    __tablename__ = "lineage_relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_asset_id = Column(String(255), ForeignKey('assets.id', ondelete='CASCADE'), nullable=False)
    target_asset_id = Column(String(255), ForeignKey('assets.id', ondelete='CASCADE'), nullable=False)
    relationship_type = Column(String(50), default='transformation')
    source_type = Column(String(50), nullable=False)
    target_type = Column(String(50), nullable=False)
    

    column_lineage = Column(JSON)
    

    transformation_type = Column(String(50))
    transformation_description = Column(Text)
    sql_query = Column(Text)
    

    source_system = Column(String(50))
    source_job_id = Column(String(255))
    source_job_name = Column(String(255))
    

    confidence_score = Column(DECIMAL(3, 2), default=1.0)
    extraction_method = Column(String(50))
    

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    discovered_at = Column(DateTime)
    

    source_asset = relationship("Asset", foreign_keys=[source_asset_id], back_populates="source_lineage")
    target_asset = relationship("Asset", foreign_keys=[target_asset_id], back_populates="target_lineage")
    
    __table_args__ = (
        UniqueConstraint('source_asset_id', 'target_asset_id', 'source_job_id', name='unique_relationship'),
    )

class LineageHistory(Base):
    __tablename__ = "lineage_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    relationship_id = Column(Integer, ForeignKey('lineage_relationships.id', ondelete='CASCADE'), nullable=False)
    action = Column(String(50), nullable=False)
    old_data = Column(JSON)
    new_data = Column(JSON)
    changed_by = Column(String(255))
    changed_at = Column(DateTime, server_default=func.now())
    
    relationship = relationship("LineageRelationship")

class SQLQuery(Base):
    __tablename__ = "sql_queries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_text = Column(Text, nullable=False)
    query_type = Column(String(50))
    source_system = Column(String(50))
    job_id = Column(String(255))
    job_name = Column(String(255))
    asset_id = Column(String(255), ForeignKey('assets.id', ondelete='SET NULL'))
    

    parsed_lineage = Column(JSON)
    parse_status = Column(String(50), default='pending')
    parse_error = Column(Text)
    

    created_at = Column(DateTime, server_default=func.now())
    executed_at = Column(DateTime)
    
    asset = relationship("Asset")

class DataDiscovery(Base):
    __tablename__ = "data_discovery"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    asset_id = Column(String(255), ForeignKey('assets.id', ondelete='CASCADE'), nullable=True, index=True)
    
    storage_location = Column(JSON, nullable=False)
    file_metadata = Column(JSON, nullable=False)
    schema_json = Column(JSON)
    schema_hash = Column(String(64), nullable=False)
    schema_version = Column(String(50))
    
    discovered_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_checked_at = Column(DateTime)
    
    status = Column(String(50), server_default='pending', nullable=False)
    approval_status = Column(String(50))
    is_visible = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True)
    deleted_at = Column(DateTime)
    
    environment = Column(String(50))
    env_type = Column(String(50))
    data_source_type = Column(String(100))
    folder_path = Column(String(1000))
    
    tags = Column(JSON)
    discovery_info = Column(JSON)
    approval_workflow = Column(JSON)
    
    notification_sent_at = Column(DateTime)
    notification_recipients = Column(JSON)
    
    storage_metadata = Column(JSON)
    storage_data_metadata = Column(JSON)
    additional_metadata = Column(JSON)
    
    data_quality_score = Column(DECIMAL(5, 2))
    validation_errors = Column(JSON)
    validation_status = Column(String(50))
    validated_at = Column(DateTime)
    
    published_at = Column(DateTime)
    published_to = Column(String(255))
    data_publishing_id = Column(BigInteger)
    
    created_by = Column(String(255))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    

    asset = relationship("Asset", foreign_keys=[asset_id])

