#!/bin/bash
# Quick start script for Oracle Database
# Usage: ./start-oracle.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Starting Oracle Database..."
echo "=========================================="

# Start Oracle
docker-compose -f docker-compose.oracle.yml up -d

echo ""
echo "Waiting for Oracle to be ready (this may take 2-3 minutes on first start)..."
echo ""

# Wait for Oracle to be ready
MAX_WAIT=300
WAIT_TIME=0
while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    if docker exec torro-oracle sqlplus -S sys/Oracle123@localhost:1521/XE AS SYSDBA <<EOF > /dev/null 2>&1
SELECT 1 FROM DUAL;
EXIT;
EOF
    then
        echo "Oracle is ready!"
        break
    fi
    echo -n "."
    sleep 5
    WAIT_TIME=$((WAIT_TIME + 5))
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    echo ""
    echo "Warning: Oracle may not be fully ready. Check logs with: docker logs torro-oracle"
else
    echo ""
    echo "Initializing sample data..."
    
    # Run initialization script
    docker exec -i torro-oracle sqlplus sys/Oracle123@localhost:1521/XE AS SYSDBA < oracle-init.sql > /dev/null 2>&1 || {
        echo "Note: Initialization script may have already run or encountered an error."
        echo "You can manually run it with: docker exec -i torro-oracle sqlplus sys/Oracle123@localhost:1521/XE AS SYSDBA < oracle-init.sql"
    }
    
    echo ""
    echo "=========================================="
    echo "Oracle Database is ready!"
    echo "=========================================="
    echo ""
    echo "Connection Details:"
    echo "  Host: localhost"
    echo "  Port: 1521"
    echo "  Service Name: XE"
    echo "  Username: test_user"
    echo "  Password: test_password"
    echo ""
    echo "JDBC URL: jdbc:oracle:thin:@//localhost:1521/XE"
    echo ""
    echo "Enterprise Manager: http://localhost:5500/em"
    echo "  Username: sys"
    echo "  Password: Oracle123"
    echo ""
    echo "To view logs: docker logs -f torro-oracle"
    echo "To stop: docker-compose -f docker-compose.oracle.yml down"
    echo ""
fi

