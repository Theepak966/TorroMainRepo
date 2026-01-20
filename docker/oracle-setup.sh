#!/bin/bash
# Oracle Database Setup Script
# This script waits for Oracle to be ready and runs initialization

set -e

echo "Waiting for Oracle database to be ready..."
sleep 30

# Wait for Oracle to be fully up
until sqlplus -S sys/Oracle123@localhost:1521/XE AS SYSDBA <<EOF
SELECT 1 FROM DUAL;
EXIT;
EOF
do
  echo "Oracle is not ready yet. Waiting..."
  sleep 10
done

echo "Oracle is ready! Running initialization script..."

# Run initialization script
sqlplus sys/Oracle123@localhost:1521/XE AS SYSDBA <<EOF
@/docker-entrypoint-initdb.d/oracle-init.sql
EXIT;
EOF

echo "Oracle initialization complete!"

