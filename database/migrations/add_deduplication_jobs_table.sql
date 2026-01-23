-- Migration: Add deduplication_jobs table for async deduplication
-- This table tracks background deduplication jobs for large datasets (100k+ files)

CREATE TABLE IF NOT EXISTS deduplication_jobs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    status VARCHAR(50) NOT NULL DEFAULT 'queued',
    total_discoveries INT DEFAULT 0,
    groups_deduped INT DEFAULT 0,
    hidden_count INT DEFAULT 0,
    progress_percent DECIMAL(5, 2) DEFAULT 0.0,
    error_message TEXT,
    started_at DATETIME,
    completed_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
