#!/bin/bash
# Script to apply lineage system database migration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATION_FILE="$SCRIPT_DIR/create_lineage_tables.sql"

# Load database config from environment or use defaults
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_NAME="${DB_NAME:-torroforairflow}"

echo "=========================================="
echo "Applying Lineage System Migration"
echo "=========================================="
echo "Database: $DB_NAME"
echo "Host: $DB_HOST:$DB_PORT"
echo "User: $DB_USER"
echo ""

# Check if password is provided
if [ -z "$DB_PASSWORD" ]; then
    echo "Warning: DB_PASSWORD not set. Using MySQL client without password."
    MYSQL_CMD="mysql -h $DB_HOST -P $DB_PORT -u $DB_USER"
else
    MYSQL_CMD="mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p$DB_PASSWORD"
fi

# Apply migration
echo "Applying migration from: $MIGRATION_FILE"
$MYSQL_CMD $DB_NAME < "$MIGRATION_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Migration applied successfully!"
    echo ""
    echo "Lineage system tables created:"
    echo "  - lineage_datasets"
    echo "  - lineage_processes"
    echo "  - lineage_edges"
    echo "  - lineage_column_lineage"
    echo "  - lineage_audit_log"
else
    echo ""
    echo "✗ Migration failed!"
    exit 1
fi










