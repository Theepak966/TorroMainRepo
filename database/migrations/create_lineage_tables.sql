-- Best-in-class Data Lineage System Migration
-- Creates tables for Dataset, Process, LineageEdge, ColumnLineage, and AuditLog
-- Optimized for bank environment with performance and compliance requirements

USE torroforairflow;

-- ============================================
-- 1. Lineage Datasets Table
-- ============================================
CREATE TABLE IF NOT EXISTS lineage_datasets (
    urn VARCHAR(512) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    catalog VARCHAR(255),
    schema_name VARCHAR(255),
    
    -- Storage metadata (read-only, metadata-only access)
    storage_type VARCHAR(50),
    storage_location JSON,
    
    -- Optional: Table-level lineage metadata
    table_lineage_enabled BOOLEAN DEFAULT FALSE,
    
    -- Temporal lineage
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    
    -- Indexes for performance
    INDEX idx_dataset_name (name),
    INDEX idx_dataset_catalog_schema (catalog, schema_name, name),
    INDEX idx_dataset_validity (valid_from, valid_to),
    INDEX idx_dataset_type (type),
    INDEX idx_table_lineage_enabled (table_lineage_enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- 2. Lineage Processes Table
-- ============================================
CREATE TABLE IF NOT EXISTS lineage_processes (
    urn VARCHAR(512) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    
    -- Process metadata
    source_system VARCHAR(100),
    job_id VARCHAR(255),
    job_name VARCHAR(255),
    
    -- Process definition (optional, for audit)
    process_definition JSON,
    
    -- Temporal
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    
    -- Indexes for performance
    INDEX idx_process_name (name),
    INDEX idx_process_source_job (source_system, job_id),
    INDEX idx_process_validity (valid_from, valid_to),
    INDEX idx_process_type (type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- 3. Lineage Edges Table (Precomputed)
-- ============================================
CREATE TABLE IF NOT EXISTS lineage_edges (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Graph structure: Dataset --(CONSUMED_BY)--> Process --(PRODUCES)--> Dataset
    source_urn VARCHAR(512) NOT NULL,
    process_urn VARCHAR(512) NOT NULL,
    target_urn VARCHAR(512) NOT NULL,
    
    -- Edge metadata
    relationship_type VARCHAR(50) DEFAULT 'transformation',
    edge_metadata JSON,
    
    -- Temporal lineage (append-only)
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    ingestion_id VARCHAR(255),
    
    -- Hash column for unique constraint (to avoid key length limit)
    edge_hash VARCHAR(64) GENERATED ALWAYS AS (SHA2(CONCAT(source_urn, '|', process_urn, '|', target_urn, '|', valid_from), 256)) STORED,
    -- Unique constraint for idempotency
    UNIQUE KEY unique_lineage_edge (edge_hash),
    
    -- Indexes for fast traversal (critical for performance)
    -- Using prefix indexes (255 chars) to avoid key length limit
    INDEX idx_edge_source (source_urn(255), valid_from, valid_to),
    INDEX idx_edge_target (target_urn(255), valid_from, valid_to),
    INDEX idx_edge_process (process_urn(255), valid_from, valid_to),
    INDEX idx_edge_validity (valid_from, valid_to),
    INDEX idx_edge_ingestion (ingestion_id),
    INDEX idx_edge_composite (source_urn(255), target_urn(255), valid_from),
    INDEX idx_edge_relationship_type (relationship_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- 4. Column Lineage Table (Separate Storage)
-- ============================================
CREATE TABLE IF NOT EXISTS lineage_column_lineage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Reference to edge (not part of graph traversal)
    edge_id INT NOT NULL,
    
    -- Column mappings
    source_column VARCHAR(255) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    
    -- Optional: Table-level if enabled
    source_table VARCHAR(255),
    target_table VARCHAR(255),
    
    -- Transformation details
    transformation_type VARCHAR(50),
    transformation_expression TEXT,
    
    -- Temporal
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key
    FOREIGN KEY (edge_id) REFERENCES lineage_edges(id) ON DELETE CASCADE,
    
    -- Indexes
    INDEX idx_col_edge (edge_id, valid_from, valid_to),
    INDEX idx_col_source (source_column, source_table),
    INDEX idx_col_target (target_column, target_table),
    INDEX idx_col_transformation (transformation_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- 5. Lineage Audit Log Table
-- ============================================
CREATE TABLE IF NOT EXISTS lineage_audit_log (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    
    action VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_urn VARCHAR(512) NOT NULL,
    
    -- Change tracking
    old_data JSON,
    new_data JSON,
    
    -- Audit metadata
    user_id VARCHAR(255),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(255),
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for audit queries
    INDEX idx_audit_action (action),
    INDEX idx_audit_entity (entity_type, entity_urn, created_at),
    INDEX idx_audit_user (user_id, created_at),
    INDEX idx_audit_ingestion (ingestion_id),
    INDEX idx_audit_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Add Foreign Keys (after table creation)
-- Note: Foreign keys are optional for performance - can be added later if needed
-- ============================================
-- ALTER TABLE lineage_edges
--     ADD CONSTRAINT fk_edge_source FOREIGN KEY (source_urn) REFERENCES lineage_datasets(urn) ON DELETE CASCADE,
--     ADD CONSTRAINT fk_edge_process FOREIGN KEY (process_urn) REFERENCES lineage_processes(urn) ON DELETE CASCADE,
--     ADD CONSTRAINT fk_edge_target FOREIGN KEY (target_urn) REFERENCES lineage_datasets(urn) ON DELETE CASCADE;

-- ============================================
-- Performance Optimization Notes:
-- ============================================
-- 1. All foreign keys have CASCADE delete for data integrity
-- 2. Composite indexes on (source_urn, target_urn, valid_from) for fast lookups
-- 3. Temporal indexes on (valid_from, valid_to) for time-based queries
-- 4. Ingestion ID indexed for idempotency checks
-- 5. Column lineage stored separately (not graph-traversed by default)
-- 6. Audit log has separate indexes for compliance queries
-- ============================================


