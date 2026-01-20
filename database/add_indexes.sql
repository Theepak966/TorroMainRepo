-- OPTIMIZATION 9: Add indexes for better query performance
-- MySQL 8.0 compatible version (without IF NOT EXISTS)
-- This script will skip indexes that already exist

-- Assets table indexes
CREATE INDEX idx_assets_connector_id ON assets(connector_id);
CREATE INDEX idx_assets_name ON assets(name);
CREATE INDEX idx_assets_created_at ON assets(created_at);
CREATE INDEX idx_assets_catalog_name ON assets(catalog, name);

-- Connections table indexes
CREATE INDEX idx_connections_status ON connections(status);
CREATE INDEX idx_connections_connector_type ON connections(connector_type);

-- Data discovery indexes
CREATE INDEX idx_discovery_connection_id ON data_discovery(connection_id);
CREATE INDEX idx_discovery_status ON data_discovery(status);
CREATE INDEX idx_discovery_storage_path ON data_discovery(storage_type, storage_identifier, storage_path);
CREATE INDEX idx_discovery_asset_id ON data_discovery(asset_id);

-- Lineage indexes
CREATE INDEX idx_lineage_source_asset ON lineage_relationships(source_asset_id);
CREATE INDEX idx_lineage_target_asset ON lineage_relationships(target_asset_id);
CREATE INDEX idx_lineage_created_at ON lineage_relationships(created_at);

