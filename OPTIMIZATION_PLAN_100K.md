# Optimization Plan for 100K Assets

## Current Status: ⚠️ PARTIALLY OPTIMIZED

### ✅ Good Optimizations Already in Place

1. **Discovery Service**
   - ✅ Bulk insert operations (`bulk_insert_mappings`) - 10-40x faster
   - ✅ Queue-based streaming (prevents memory buildup)
   - ✅ Adaptive batch sizing (300-500 based on dataset size)
   - ✅ Connection pooling (75 connections, 75 overflow)
   - ✅ Pre-loading existing assets for deduplication (up to 50K)

2. **Database**
   - ✅ Indexes on `discovered_at`, `connector_id`, `catalog`, `type`
   - ✅ Connection pool with proper timeouts

3. **Assets Endpoint**
   - ✅ Pagination support
   - ✅ Minimal mode for faster queries
   - ✅ Partially optimized JOIN (still needs work)

### ❌ Critical Issues for 100K Assets

#### 1. Assets Endpoint COUNT Query (Line 158)
**Current**: `total_count = db.query(Asset).count()` - Scans entire table
**Impact**: 5-10+ seconds with 100K assets
**Fix**: Already partially fixed (optional COUNT), but needs verification

#### 2. Pre-loading Limit (Line 634)
**Current**: Only pre-loads if `asset_count < 50000`
**Impact**: For 100K assets, falls back to per-file queries (slower)
**Fix Needed**: Increase limit or use smarter caching

#### 3. Batch Size for 100K
**Current**: Adaptive batch sizing caps at 300-500
**Impact**: May be too small for 100K assets
**Fix Needed**: Add 100K+ tier with larger batch size (1000-2000)

#### 4. JOIN Query Performance
**Current**: Subquery processes all assets even with pagination
**Impact**: Slow even for paginated requests
**Fix**: Already partially optimized, but needs testing

## Recommended Optimizations

### Priority 1: Immediate Fixes

1. **Increase Pre-loading Limit**
   ```python
   # Change line 634 from:
   if asset_count < 50000:
   # To:
   if asset_count < 150000:  # Support up to 150K
   ```

2. **Add 100K+ Batch Size Tier**
   ```python
   # In discovery_service.py, add:
   if estimated_total > 100000:
       batch_size = int(os.getenv("DISCOVERY_BATCH_SIZE", "2000"))  # 2000 for 100K+
   ```

3. **Verify COUNT Query is Optional**
   - Already fixed in assets.py (line 160-169)
   - Ensure frontend doesn't request total count

### Priority 2: Performance Improvements

4. **Add Composite Indexes**
   ```sql
   CREATE INDEX idx_assets_connector_discovered ON assets(connector_id, discovered_at DESC);
   CREATE INDEX idx_discovery_asset_status ON data_discovery(asset_id, status);
   ```

5. **Optimize Discovery Pre-loading**
   - Use Redis cache for >50K assets
   - Or use database query with LIMIT for recent assets only

6. **Add Response Caching**
   - Cache asset list responses for 1-5 minutes
   - Use Redis or in-memory cache

### Priority 3: Monitoring

7. **Add Performance Metrics**
   - Track discovery time per 10K assets
   - Monitor query execution times
   - Alert on slow queries (>5s)

## Expected Performance After Optimizations

### Discovery
- **Current**: ~30-60 minutes for 100K assets
- **Target**: < 20 minutes for 100K assets
- **Method**: Larger batches (2000), optimized pre-loading

### Assets List
- **Current**: 9-11 seconds (with COUNT), 0.2s (minimal mode)
- **Target**: < 1 second per page
- **Method**: Remove COUNT, optimize JOIN

### Asset Detail
- **Current**: < 0.5 seconds ✅
- **Target**: < 0.5 seconds ✅ (already good)

## Testing Plan

1. **Load Test**: Create 100K test assets
2. **Performance Test**: Measure discovery time
3. **Query Test**: Test pagination with 100K assets
4. **Memory Test**: Monitor memory usage during discovery

## Configuration Changes Needed

Add to `backend/.env`:
```
DISCOVERY_BATCH_SIZE=2000  # For 100K+ assets
DISCOVERY_PRELOAD_LIMIT=150000  # Increase pre-load limit
```
