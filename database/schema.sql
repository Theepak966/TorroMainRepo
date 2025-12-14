-- MySQL Database Schema for TorroForExcel
-- Run this script to create the database and tables

-- Create database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS torroforexcel CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Use the database
USE torroforexcel;

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

