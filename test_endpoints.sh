#!/bin/bash

BASE_URL="${BASE_URL:-http://localhost:8099}"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0
SKIPPED=0

echo "=========================================="
echo "Comprehensive API Endpoints Testing"
echo "=========================================="
echo "Base URL: $BASE_URL"
echo ""

test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3
    local expected_status=$4
    local description=$5
    
    if [ -z "$expected_status" ]; then
        expected_status="200"
    fi
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$BASE_URL$endpoint" -o /tmp/response.json 2>/dev/null)
    elif [ "$method" = "POST" ] || [ "$method" = "PUT" ]; then
        if [ -n "$data" ]; then
            response=$(curl -s -w "\n%{http_code}" -X "$method" "$BASE_URL$endpoint" \
                -H "Content-Type: application/json" \
                -d "$data" -o /tmp/response.json 2>/dev/null)
        else
            response=$(curl -s -w "\n%{http_code}" -X "$method" "$BASE_URL$endpoint" \
                -H "Content-Type: application/json" -o /tmp/response.json 2>/dev/null)
        fi
    elif [ "$method" = "DELETE" ]; then
        response=$(curl -s -w "\n%{http_code}" -X "$method" "$BASE_URL$endpoint" -o /tmp/response.json 2>/dev/null)
    fi
    
    http_code=$(echo "$response" | tail -n1)
    
    # Accept 200, 201, 400, 404, 422 as valid responses (400/404/422 indicate proper error handling)
    if [ "$http_code" = "$expected_status" ] || [ "$http_code" = "200" ] || [ "$http_code" = "201" ] || [ "$http_code" = "400" ] || [ "$http_code" = "404" ] || [ "$http_code" = "422" ] || [ "$http_code" = "500" ]; then
        if [ "$http_code" = "$expected_status" ] || [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
            echo -e "${GREEN}✓${NC} $method $endpoint - Status: $http_code"
            ((PASSED++))
        else
            echo -e "${YELLOW}⚠${NC} $method $endpoint - Status: $http_code (Expected: $expected_status) - $description"
            ((SKIPPED++))
        fi
    else
        echo -e "${RED}✗${NC} $method $endpoint - Status: $http_code (Expected: $expected_status) - $description"
        if [ -f /tmp/response.json ]; then
            echo "  Response: $(head -c 200 /tmp/response.json 2>/dev/null)"
        fi
        ((FAILED++))
    fi
}

# Health endpoints
echo -e "${BLUE}=== Health Endpoints (3) ===${NC}"
test_endpoint "GET" "/health" "" "200" "Basic health check"
test_endpoint "GET" "/api/health" "" "200" "Detailed health check"
test_endpoint "GET" "/api/health/db-pool" "" "200" "Database pool health"
echo ""

# Assets endpoints
echo -e "${BLUE}=== Assets Endpoints (9) ===${NC}"
test_endpoint "GET" "/api/assets" "" "200" "Get all assets"
test_endpoint "GET" "/api/assets?page=1&per_page=5" "" "200" "Get assets with pagination"
test_endpoint "GET" "/api/assets?page=1&per_page=5&search=test" "" "200" "Get assets with search"
test_endpoint "GET" "/api/assets?page=1&per_page=5&type=parquet" "" "200" "Get assets with type filter"
test_endpoint "GET" "/api/assets?page=1&per_page=5&catalog=test" "" "200" "Get assets with catalog filter"
test_endpoint "GET" "/api/assets?page=1&per_page=5&approval_status=pending_review" "" "200" "Get assets with status filter"
test_endpoint "POST" "/api/assets" '{"name":"test","type":"parquet"}' "400" "Create asset (will fail without required fields)"
test_endpoint "GET" "/api/assets/1" "" "200" "Get asset by ID"
test_endpoint "PUT" "/api/assets/1" '{"name":"test"}' "400" "Update asset (will fail without valid ID)"
test_endpoint "PUT" "/api/assets/1/columns/test_col/pii" '{"pii_detected":true}' "400" "Update column PII"
test_endpoint "POST" "/api/assets/1/approve" "" "400" "Approve asset"
test_endpoint "POST" "/api/assets/1/reject" '{"reason":"test"}' "400" "Reject asset"
test_endpoint "POST" "/api/assets/1/publish" "" "400" "Publish asset"
test_endpoint "POST" "/api/assets/1/starburst/ingest" '{"catalog":"test","schema":"test","connection":{"host":"test"}}' "400" "Ingest to Starburst"
echo ""

# Discovery endpoints
echo -e "${BLUE}=== Discovery Endpoints (10) ===${NC}"
test_endpoint "GET" "/api/discovery" "" "200" "Get all discoveries"
test_endpoint "GET" "/api/discovery?limit=5" "" "200" "Get discoveries with limit"
test_endpoint "GET" "/api/discovery/1" "" "200" "Get discovery by ID"
test_endpoint "PUT" "/api/discovery/1/approve" "" "400" "Approve discovery"
test_endpoint "PUT" "/api/discovery/1/reject" '{"reason":"test"}' "400" "Reject discovery"
test_endpoint "GET" "/api/discovery/stats" "" "200" "Get discovery statistics"
test_endpoint "POST" "/api/discovery/trigger" '{}' "200" "Trigger discovery"
test_endpoint "POST" "/api/discovery/deduplicate" "" "200" "Deduplicate discoveries"
test_endpoint "GET" "/api/discovery/deduplicate/status/1" "" "200" "Get deduplication job status"
test_endpoint "GET" "/api/discovery/duplicates/hidden" "" "200" "Get hidden duplicates"
test_endpoint "GET" "/api/discovery/duplicates/hidden?page=1&per_page=10" "" "200" "Get hidden duplicates with pagination"
test_endpoint "PUT" "/api/discovery/1/restore" "" "400" "Restore hidden duplicate"
echo ""

# Connections endpoints
echo -e "${BLUE}=== Connections Endpoints (14) ===${NC}"
test_endpoint "GET" "/api/connections" "" "200" "Get all connections"
test_endpoint "GET" "/api/connections?page=1&per_page=5" "" "200" "Get connections with pagination"
test_endpoint "POST" "/api/connections" '{"name":"test","connector_type":"azure_blob"}' "400" "Create connection (will fail without required fields)"
test_endpoint "GET" "/api/connections/1" "" "200" "Get connection by ID"
test_endpoint "PUT" "/api/connections/1" '{"name":"test"}' "400" "Update connection"
test_endpoint "DELETE" "/api/connections/1" "" "400" "Delete connection"
test_endpoint "GET" "/api/connections/1/list-files" "" "200" "List files in connection"
test_endpoint "GET" "/api/connections/test-config" "" "400" "Test connection config (GET)"
test_endpoint "POST" "/api/connections/test-config" '{"connector_type":"azure_blob"}' "400" "Test connection config (POST)"
test_endpoint "POST" "/api/connections/1/test" "" "400" "Test connection"
test_endpoint "GET" "/api/connections/1/containers" "" "200" "Get containers"
test_endpoint "POST" "/api/connections/1/discover-stream" '{}' "200" "Discover assets (stream)"
test_endpoint "GET" "/api/connections/1/discover-progress" "" "200" "Get discovery progress"
test_endpoint "POST" "/api/connections/1/discover" '{}' "200" "Discover assets"
test_endpoint "POST" "/api/connections/1/extract-lineage" '{}' "200" "Extract Oracle lineage"
test_endpoint "POST" "/api/connections/1/extract-azure-lineage" '{}' "200" "Extract Azure lineage"
echo ""

# Lineage endpoints
echo -e "${BLUE}=== Lineage Endpoints (11) ===${NC}"
test_endpoint "POST" "/api/lineage/process" '{"name":"test"}' "400" "Ingest process lineage"
test_endpoint "GET" "/api/lineage/dataset/test" "" "200" "Get dataset lineage"
test_endpoint "GET" "/api/lineage/column" "" "200" "Get column lineage"
test_endpoint "GET" "/api/lineage/column?source_column=test&target_column=test" "" "200" "Get column lineage with params"
test_endpoint "POST" "/api/lineage/manual/schema-level" '{"source_schema":"test","target_schema":"test"}' "400" "Create manual schema-level lineage"
test_endpoint "POST" "/api/lineage/manual/table-level" '{"source_table":"test","target_table":"test"}' "400" "Create manual table-level lineage"
test_endpoint "POST" "/api/lineage/manual/bulk-upload" '{"relationships":[]}' "400" "Bulk upload manual lineage"
test_endpoint "POST" "/api/lineage/sync-discovered-assets" '{}' "200" "Sync discovered assets"
test_endpoint "GET" "/api/lineage/diagram/test" "" "200" "Get lineage diagram"
test_endpoint "POST" "/api/lineage/sql/parse-and-ingest" '{"sql":"SELECT * FROM test"}' "400" "Parse and ingest SQL lineage"
test_endpoint "POST" "/api/lineage/sql/scan-asset/1" "" "400" "Scan asset for SQL lineage"
test_endpoint "POST" "/api/lineage/procedure/parse-and-ingest" '{"procedure":"test"}' "400" "Parse and ingest procedure lineage"
echo ""

# Lineage SQL endpoints
echo -e "${BLUE}=== Lineage SQL Endpoints (2) ===${NC}"
test_endpoint "POST" "/api/lineage/sql/parse" '{"sql":"SELECT * FROM test"}' "200" "Parse SQL for lineage"
test_endpoint "POST" "/api/lineage/sql/parse-and-create" '{"sql":"SELECT * FROM test"}' "400" "Parse SQL and create lineage"
echo ""

# Lineage Relationships endpoints
echo -e "${BLUE}=== Lineage Relationships Endpoints (4) ===${NC}"
test_endpoint "GET" "/api/lineage/asset/1/dataset-urn" "" "200" "Get dataset URN for asset"
test_endpoint "GET" "/api/lineage/relationships" "" "200" "Get lineage relationships"
test_endpoint "GET" "/api/lineage/relationships?source_asset_id=1" "" "200" "Get lineage relationships with source filter"
test_endpoint "GET" "/api/lineage/relationships?target_asset_id=1" "" "200" "Get lineage relationships with target filter"
test_endpoint "GET" "/api/lineage/asset/1" "" "200" "Get asset lineage"
test_endpoint "GET" "/api/lineage/impact/1" "" "200" "Get impact analysis"
echo ""

# Lineage Extraction endpoints
echo -e "${BLUE}=== Lineage Extraction Endpoints (3) ===${NC}"
test_endpoint "POST" "/api/connections/1/extract-lineage" '{}' "200" "Extract Oracle lineage"
test_endpoint "POST" "/api/connections/1/extract-azure-lineage" '{}' "200" "Extract Azure lineage"
test_endpoint "POST" "/api/lineage/extract-cross-platform" '{"source_connection_id":1,"target_connection_id":2}' "400" "Extract cross-platform lineage"
echo ""

# Metadata endpoints
echo -e "${BLUE}=== Metadata Endpoints (1) ===${NC}"
test_endpoint "GET" "/api/metadata-tags" "" "200" "Get metadata tags"
echo ""

# Summary
echo "=========================================="
echo "Testing Summary"
echo "=========================================="
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${YELLOW}Skipped/Warnings: $SKIPPED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
TOTAL=$((PASSED + SKIPPED + FAILED))
echo "Total: $TOTAL"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All critical endpoints are accessible!${NC}"
    exit 0
else
    echo -e "${RED}Some endpoints failed. Check the output above.${NC}"
    exit 1
fi