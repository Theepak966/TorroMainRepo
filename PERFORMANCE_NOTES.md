# Performance Optimization Notes

## Issue
API endpoints `/api/assets` and `/api/connections` are taking 10-11 seconds to respond.

## Root Cause Analysis
1. **Database queries are fast**: Direct queries take 0.07-0.14s
2. **Azure database connection**: Working fine, low latency (~0.07s)
3. **Likely bottlenecks**:
   - Large JSON column serialization (business_metadata, technical_metadata, columns)
   - Multiple queries in sequence
   - JSON processing overhead

## Optimizations Applied
1. ✅ Removed expensive COUNT query (now optional with `?include_total=true`)
2. ✅ Optimized pagination to fetch assets first, then discovery data
3. ✅ Split complex JOIN into two simpler queries

## Further Optimizations Needed
1. **Add response caching** for frequently accessed data
2. **Reduce JSON payload size** - only return needed fields
3. **Add database query result caching**
4. **Consider using `minimal=1` parameter** for faster responses
5. **Add connection pooling optimizations**

## Quick Fix for Frontend
The frontend can use the `minimal=1` parameter for faster asset loading:
```
/api/assets?page=1&per_page=10&minimal=1
```

This uses the optimized fast path that skips discovery joins.

## Testing
- Connections endpoint: ~0.3s ✅
- Assets endpoint (optimized): Still needs work
- Database queries: 0.07-0.14s ✅
