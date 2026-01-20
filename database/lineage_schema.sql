-- Real Data Lineage Schema
-- This extends the existing schema with lineage tracking capabilities

USE torroforairflow;

-- Lineage Relationships Table
-- Stores actual data flow relationships extracted from SQL queries, ETL jobs, etc.
CREATE TABLE IF NOT EXISTS lineage_relationships (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_asset_id VARCHAR(255) NOT NULL,
    target_asset_id VARCHAR(255) NOT NULL,
    relationship_type VARCHAR(50) NOT NULL DEFAULT 'transformation', -- transformation, copy, view, etc.
    source_type VARCHAR(50) NOT NULL, -- table, view, file, etc.
    target_type VARCHAR(50) NOT NULL,
    
    -- Column-level lineage (JSON array)
    column_lineage JSON, -- [{"source_column": "col1", "target_column": "col1", "transformation": "pass_through"}]
    
    -- Transformation details
    transformation_type VARCHAR(50), -- pass_through, aggregate, join, filter, etc.
    transformation_description TEXT,
    sql_query TEXT, -- Original SQL query if available
    
    -- Metadata
    source_system VARCHAR(100), -- airflow, dbt, databricks, manual, etc.
    source_job_id VARCHAR(255), -- ETL job ID or task ID
    source_job_name VARCHAR(255),
    
    -- Confidence and quality
    confidence_score DECIMAL(3,2) DEFAULT 1.0, -- 0.0 to 1.0
    extraction_method VARCHAR(50), -- sql_parsing, manual, api, inferred
    
    -- Timestamps
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    discovered_at DATETIME, -- When this relationship was first discovered
    
    -- Indexes for fast lookups
    INDEX idx_source_asset (source_asset_id),
    INDEX idx_target_asset (target_asset_id),
    INDEX idx_relationship_type (relationship_type),
    INDEX idx_source_system (source_system),
    INDEX idx_confidence (confidence_score),
    
    -- Foreign key constraints (optional, for referential integrity)
    FOREIGN KEY (source_asset_id) REFERENCES assets(id) ON DELETE CASCADE,
    FOREIGN KEY (target_asset_id) REFERENCES assets(id) ON DELETE CASCADE,
    
    -- Prevent duplicate relationships
    UNIQUE KEY unique_relationship (source_asset_id, target_asset_id, source_job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Lineage History Table
-- Tracks changes to lineage over time (temporal lineage)
CREATE TABLE IF NOT EXISTS lineage_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    relationship_id INT NOT NULL,
    action VARCHAR(50) NOT NULL, -- created, updated, deleted
    old_data JSON, -- Snapshot of old relationship data
    new_data JSON, -- Snapshot of new relationship data
    changed_by VARCHAR(255), -- User who made the change
    changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_relationship (relationship_id),
    INDEX idx_changed_at (changed_at),
    
    FOREIGN KEY (relationship_id) REFERENCES lineage_relationships(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- SQL Queries Table
-- Stores SQL queries for parsing and lineage extraction
CREATE TABLE IF NOT EXISTS sql_queries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    query_text TEXT NOT NULL,
    query_type VARCHAR(50), -- SELECT, INSERT, UPDATE, CREATE TABLE, etc.
    source_system VARCHAR(100), -- airflow, dbt, databricks, etc.
    job_id VARCHAR(255),
    job_name VARCHAR(255),
    asset_id VARCHAR(255), -- Target asset this query creates/updates
    
    -- Parsed lineage (cached)
    parsed_lineage JSON,
    parse_status VARCHAR(50) DEFAULT 'pending', -- pending, parsed, failed
    parse_error TEXT,
    
    -- Metadata
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    executed_at DATETIME,
    
    INDEX idx_asset_id (asset_id),
    INDEX idx_source_system (source_system),
    INDEX idx_parse_status (parse_status),
    
    FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


