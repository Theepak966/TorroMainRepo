# Scalability Analysis for 100K Assets

## Current Optimizations ✅

### Discovery Service
- ✅ Batch processing with adaptive batch sizing
- ✅ Queue-based processing for Azure Blob
- ✅ Bulk insert/update operations
- ✅ Connection pooling (75 connections, 75 overflow)
- ✅ Configurable batch size (default: 1500)

### Assets Endpoint
- ✅ Pagination support
- ✅ Minimal mode for faster queries
- ✅ Indexed queries (discovered_at, etc.)

## Critical Issues for 100K Assets ❌

### 1. Assets Endpoint COUNT Query
**Problem**: Line 158 in assets.py does `db.query(Asset).count()` which scans entire table
**Impact**: With 100K assets, this takes 5-10+ seconds
**Fix Needed**: Make COUNT optional or use estimated count

### 2. Complex JOIN Query
**Problem**: Lines 161-183 join with DataDiscovery subquery for ALL assets
**Impact**: Even with pagination, the subquery processes all assets
**Current Fix**: Partially optimized but still slow

### 3. No Database Indexes on Discovery
**Problem**: DataDiscovery table may lack proper indexes
**Impact**: JOINs become slow with 100K+ records

### 4. Memory Usage
**Problem**: Loading all assets without pagination could use GBs of RAM
**Impact**: Server crashes or slowdowns

## Recommendations for 100K Assets

### Immediate Fixes
1. **Remove COUNT query** - Use `has_next` based on result size
2. **Optimize JOIN** - Only join discovery for paginated assets
3. **Add database indexes** - Ensure all foreign keys are indexed
4. **Increase batch size** - For discovery, use 2000-5000 for 100K assets

### Performance Targets
- Discovery: 100K assets in < 30 minutes
- Assets list: < 1 second per page
- Asset detail: < 0.5 seconds
