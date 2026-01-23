-- Add custom_columns field to assets table
-- This allows users to add custom columns to the columns & PII detection table
-- Structure: { columnId: { label: string, values: { columnName: value } } }

ALTER TABLE assets 
ADD COLUMN IF NOT EXISTS custom_columns JSON NULL 
COMMENT 'Custom user-defined columns: { columnId: { label: string, values: { columnName: value } } }';
