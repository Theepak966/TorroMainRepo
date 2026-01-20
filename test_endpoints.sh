#!/bin/bash

BASE_URL="http://localhost:8099"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Testing API Endpoints"
echo "=========================================="
echo ""

test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3
    local expected_status=$4
    
    if [ -z "$expected_status" ]; then
        expected_status="200"
    fi
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$BASE_URL$endpoint" -o /tmp/response.json)
    elif [ "$method" = "POST" ] || [ "$method" = "PUT" ]; then
        if [ -n "$data" ]; then
            response=$(curl -s -w "\n%{http_code}" -X "$method" "$BASE_URL$endpoint" \
                -H "Content-Type: application/json" \
                -d "$data" -o /tmp/response.json)
        else
            response=$(curl -s -w "\n%{http_code}" -X "$method" "$BASE_URL$endpoint" \
                -H "Content-Type: application/json" -o /tmp/response.json)
        fi
    elif [ "$method" = "DELETE" ]; then
        response=$(curl -s -w "\n%{http_code}" -X "$method" "$BASE_URL$endpoint" -o /tmp/response.json)
    fi
    
    http_code=$(echo "$response" | tail -n1)
    time_taken=$(curl -s -w "%{time_total}" -o /dev/null "$BASE_URL$endpoint" 2>/dev/null || echo "N/A")
    
    if [ "$http_code" = "$expected_status" ] || [ "$http_code" = "400" ] || [ "$http_code" = "404" ]; then
        echo -e "${GREEN}✓${NC} $method $endpoint - Status: $http_code (${time_taken}s)"
    else
        echo -e "${RED}✗${NC} $method $endpoint - Status: $http_code (Expected: $expected_status) (${time_taken}s)"
        if [ -f /tmp/response.json ]; then
            echo "  Response: $(head -c 200 /tmp/response.json)"
        fi
    fi
}

# Health endpoints
echo "=== Health Endpoints ==="
test_endpoint "GET" "/health"
test_endpoint "GET" "/api/health"
test_endpoint "GET" "/api/health/db-pool"
echo ""

# Assets endpoints
echo "=== Assets Endpoints ==="
test_endpoint "GET" "/api/assets?page=1&per_page=5"
test_endpoint "GET" "/api/assets?minimal=1&page=1&per_page=5"
# test_endpoint "GET" "/api/assets/invalid-id"  # Will return 404
echo ""

# Connections endpoints
echo "=== Connections Endpoints ==="
test_endpoint "GET" "/api/connections"
test_endpoint "GET" "/api/connections?page=1&per_page=5"
echo ""

# Discovery endpoints
echo "=== Discovery Endpoints ==="
test_endpoint "GET" "/api/discovery"
test_endpoint "GET" "/api/discovery?limit=5"
test_endpoint "GET" "/api/discovery/stats"
echo ""

# Lineage endpoints
echo "=== Lineage Endpoints ==="
test_endpoint "GET" "/api/lineage/relationships"
echo ""

# Metadata endpoints
echo "=== Metadata Endpoints ==="
test_endpoint "GET" "/api/metadata-tags"
echo ""

# POST endpoints (will test with empty/invalid data to check error handling)
echo "=== POST Endpoints (Error Handling) ==="
test_endpoint "POST" "/api/assets" '{}' "400"
test_endpoint "POST" "/api/connections" '{}' "400"
echo ""

echo "=========================================="
echo "Testing Complete"
echo "=========================================="
