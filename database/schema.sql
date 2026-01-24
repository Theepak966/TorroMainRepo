-- MySQL Database Schema for TorroForAirflow
-- Run this script to create the database and tables

-- Create database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS torroforairflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Use the database
USE torroforairflow;

-- Assets table
CREATE TABLE IF NOT EXISTS assets (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    catalog VARCHAR(255),
    connector_id VARCHAR(255),
    discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    technical_metadata JSON,
    operational_metadata JSON,
    business_metadata JSON,
    columns JSON,
    custom_columns JSON,
    INDEX idx_catalog (catalog),
    INDEX idx_connector_id (connector_id),
    INDEX idx_type (type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Connections table
CREATE TABLE IF NOT EXISTS connections (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    connector_type VARCHAR(255) NOT NULL,
    connection_type VARCHAR(255),
    config JSON,
    status VARCHAR(50) DEFAULT 'active',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_connector_type (connector_type),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Data Discovery table
CREATE TABLE IF NOT EXISTS data_discovery (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    asset_id VARCHAR(255),
    
    INDEX idx_asset_id (asset_id),
    
    storage_location JSON NOT NULL,
    storage_type VARCHAR(50) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.type'))) STORED,
    storage_path VARCHAR(2000) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.path'))) STORED,
    storage_identifier VARCHAR(255) GENERATED ALWAYS AS (
        COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.connection.account_name')),
            JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.bucket.name')),
            JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.container.name')),
            JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.identifier'))
        )
    ) STORED,
    
    INDEX idx_storage_location (storage_type, storage_identifier, storage_path(200)),
    
    file_metadata JSON NOT NULL,
    
    file_name VARCHAR(500) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(file_metadata, '$.basic.name'))) STORED,
    file_size_bytes BIGINT GENERATED ALWAYS AS (JSON_EXTRACT(file_metadata, '$.basic.size_bytes')) STORED,
    file_hash VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(file_metadata, '$.hash.value'))) STORED,
    file_last_modified DATETIME GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(file_metadata, '$.timestamps.last_modified'))) STORED,
    
    schema_json JSON,
    schema_hash VARCHAR(64) NOT NULL,
    schema_version VARCHAR(50),
    
    discovered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_checked_at DATETIME,
    
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    approval_status VARCHAR(50),
    is_visible BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE,
    deleted_at DATETIME,
    
    environment VARCHAR(50),
    env_type VARCHAR(50),
    data_source_type VARCHAR(100),
    folder_path VARCHAR(1000),
    
    tags JSON,
    
    discovery_info JSON,
    
    approval_workflow JSON,
    
    notification_sent_at DATETIME,
    notification_recipients JSON,
    
    storage_metadata JSON,
    storage_data_metadata JSON,
    additional_metadata JSON,
    
    data_quality_score DECIMAL(5,2),
    validation_errors JSON,
    validation_status VARCHAR(50),
    validated_at DATETIME,
    
    published_at DATETIME,
    published_to VARCHAR(255),
    data_publishing_id BIGINT UNSIGNED,
    
    created_by VARCHAR(255),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_status (status),
    INDEX idx_approval_status (approval_status),
    INDEX idx_discovered_at (discovered_at),
    INDEX idx_environment (environment),
    INDEX idx_env_type (env_type),
    INDEX idx_is_visible (is_visible),
    INDEX idx_is_active (is_active),
    INDEX idx_file_last_modified (file_last_modified),
    INDEX idx_data_source_type (data_source_type),
    INDEX idx_schema_hash (schema_hash),
    INDEX idx_file_hash (file_hash),
    INDEX idx_deleted_at (deleted_at),
    INDEX idx_storage_type (storage_type),
    INDEX idx_storage_identifier (storage_identifier),
    INDEX idx_last_checked_at (last_checked_at),
    INDEX idx_notification_sent_at (notification_sent_at),
    
    INDEX idx_common_query (is_visible, is_active, status, discovered_at),
    INDEX idx_env_status (environment, status),
    INDEX idx_dedup_check (storage_type, storage_identifier),
    
    FULLTEXT idx_fulltext_search (file_name, folder_path),
    
    INDEX idx_file_name (file_name),
    INDEX idx_file_size_bytes (file_size_bytes),
    
    CONSTRAINT chk_status CHECK (status IN ('pending', 'approved', 'rejected', 'published', 'archived'))
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

