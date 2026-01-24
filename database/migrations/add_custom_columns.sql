-- Add custom_columns field to assets table
-- This allows users to add custom columns to the columns & PII detection table
-- Structure: { columnId: { label: string, values: { columnName: value } } }
-- 
-- This migration is idempotent - safe to run multiple times
-- Run this using: mysql -u user -p database_name < database/migrations/add_custom_columns.sql

-- MySQL-safe "ADD COLUMN if not exists" using variables and prepared statements
SET @exists := 0;
SELECT COUNT(*)
INTO @exists
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'assets'
  AND COLUMN_NAME = 'custom_columns';

SET @sql := IF(
  @exists = 0,
  'ALTER TABLE assets ADD COLUMN custom_columns JSON NULL COMMENT ''Custom user-defined columns: { columnId: { label: string, values: { columnName: value } } }''',
  'SELECT 1'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
