-- OPTIMIZATION 9: Add indexes for better query performance
-- MySQL 8.0 compatible version (without IF NOT EXISTS)
-- 
-- NOTE: This script only adds NEW indexes that are NOT already defined in schema.sql or lineage_schema.sql
-- Indexes already defined in CREATE TABLE statements are skipped to avoid Error 1061 (Duplicate key name)

-- Assets table indexes
-- Note: idx_connector_id already exists in schema.sql (line 23), skipping
CREATE INDEX idx_assets_name ON assets(name);
-- Note: assets table uses 'discovered_at' not 'created_at'
-- Note: idx_discovered_at already exists in schema.sql (line 114) for data_discovery, but not for assets
CREATE INDEX idx_assets_discovered_at ON assets(discovered_at);
CREATE INDEX idx_assets_catalog_name ON assets(catalog, name);

-- Connections table indexes
-- Note: idx_status and idx_connector_type already exist in schema.sql (lines 36-37), skipping

-- Data discovery indexes
-- Note: idx_status and idx_asset_id already exist in schema.sql (lines 112, 45), skipping
-- Note: data_discovery table does NOT have 'connection_id' column - removed this index
-- Note: storage_path is VARCHAR(2000) which exceeds MySQL 767-byte index limit
-- The schema.sql already has idx_storage_location with storage_path(200) prefix

-- Lineage indexes
-- Note: idx_source_asset and idx_target_asset already exist in lineage_schema.sql (lines 39-40), skipping
CREATE INDEX idx_lineage_created_at ON lineage_relationships(created_at);

