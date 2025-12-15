from sqlalchemy import Column, Integer, String, DateTime, JSON, Text
from sqlalchemy.sql import func
from .database import Base

class Asset(Base):
    __tablename__ = "assets"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    catalog = Column(String)
    connector_id = Column(String)
    discovered_at = Column(DateTime, server_default=func.now())
    technical_metadata = Column(JSON)
    operational_metadata = Column(JSON)
    business_metadata = Column(JSON)
    columns = Column(JSON)

class Connection(Base):
    __tablename__ = "connections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    connector_type = Column(String, nullable=False)
    connection_type = Column(String)
    config = Column(JSON)
    status = Column(String, default="active")
    created_at = Column(DateTime, server_default=func.now())

