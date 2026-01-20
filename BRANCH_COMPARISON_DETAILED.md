# Complete Branch Comparison: main vs new-connector

## Summary Statistics
- **Total Files Changed**: 67 files
- **Lines Added**: 15,348
- **Lines Removed**: 5,198
- **Net Change**: +10,150 lines
- **Commits Ahead**: 6 commits

## Commits in new-connector (not in main)
1. `45c7dfc` - feat: Add Oracle on-prem connector, lineage services, and Phase 1 SQL parsing enhancements
2. `9841692` - Optimize Oracle DB discovery with Azure Blob-style improvements
3. `cc83907` - Optimize discovery database save process with batch commits and progress tracking
4. `0644989` - feat: Remove tag functionality from AssetsPage
5. `57e8c0f` - feat: Performance optimizations for 500+ users + Azure DLP integration
6. `ed12f4d` - Add data masking logic functionality and rename database to torroforairflow

---

## Detailed File Changes

### 🗑️ DELETED Files (3)
- `BRANCH_COMPARISON.md` - Old comparison document
- `CHANGES_SUMMARY.md` - Old summary document
- `COMPLETE_BRANCH_COMPARISON.md` - Old complete comparison
- `backend/gunicorn_config.py` - Moved to `backend/config/gunicorn_config.py`

### ✨ NEW Files Added (40)

#### Backend Routes (9 new route modules)
- `backend/routes/__init__.py` - Routes package init
- `backend/routes/assets.py` - Asset management routes (770 lines)
- `backend/routes/connections.py` - Connection management routes (1,043 lines)
- `backend/routes/discovery.py` - Discovery routes (490 lines)
- `backend/routes/health.py` - Health check routes (75 lines)
- `backend/routes/lineage_extraction.py` - Lineage extraction routes (260 lines)
- `backend/routes/lineage_relationships.py` - Lineage relationship routes (335 lines)
- `backend/routes/lineage_routes.py` - Main lineage routes (504 lines)
- `backend/routes/lineage_sql.py` - SQL lineage routes (204 lines)
- `backend/routes/metadata.py` - Metadata routes (81 lines)

#### Backend Services (8 new service modules)
- `backend/services/__init__.py` - Services package init
- `backend/services/asset_lineage_integration.py` - Asset-lineage integration (244 lines)
- `backend/services/discovery_service.py` - Discovery service (1,835 lines)
- `backend/services/folder_based_lineage.py` - Folder-based lineage (271 lines)
- `backend/services/lineage_ingestion.py` - Lineage ingestion service (205 lines)
- `backend/services/lineage_traversal.py` - Lineage traversal service (230 lines)
- `backend/services/lineage_visualization.py` - Lineage visualization service (295 lines)
- `backend/services/manual_lineage_service.py` - Manual lineage service (230 lines)
- `backend/services/sql_lineage_integration.py` - SQL lineage integration (309 lines)

#### Backend Utils (8 new utility modules)
- `backend/utils/azure_blob_lineage_extractor.py` - Azure Blob lineage extraction (594 lines)
- `backend/utils/azure_utils.py` - Azure utilities (34 lines)
- `backend/utils/cross_platform_lineage_extractor.py` - Cross-platform lineage (507 lines)
- `backend/utils/helpers.py` - Helper functions (443 lines)
- `backend/utils/oracle_db_client.py` - Oracle DB client (706 lines)
- `backend/utils/oracle_lineage_extractor.py` - Oracle lineage extractor (972 lines)
- `backend/utils/shared_state.py` - Shared state management (97 lines)
- `backend/utils/stored_procedure_parser.py` - Stored procedure parser (402 lines)

#### Backend Models & Config
- `backend/models_lineage/__init__.py` - Lineage models package init
- `backend/models_lineage/models_lineage.py` - Lineage data models (204 lines)
- `backend/config/gunicorn_config.py` - Gunicorn configuration (44 lines)

#### Backend Scripts
- `backend/scripts/__init__.py` - Scripts package init
- `backend/scripts/discovery_runner.py` - Discovery runner script (moved from root)

#### Database Migrations
- `database/add_indexes.sql` - Database indexes (25 lines)
- `database/migrations/apply_lineage_migration.sh` - Migration script (59 lines)
- `database/migrations/create_lineage_tables.sql` - Lineage tables schema (200 lines)

#### Docker & Oracle Setup
- `docker/docker-compose.oracle.yml` - Oracle Docker Compose (29 lines)
- `docker/oracle-banking-data.sql` - Oracle banking test data (256 lines)
- `docker/oracle-init.sql` - Oracle initialization script (91 lines)
- `docker/oracle-setup.sh` - Oracle setup script (29 lines)
- `docker/oracle-test-data-jdbc.sql` - Oracle JDBC test data (155 lines)
- `docker/start-oracle.sh` - Oracle startup script (73 lines)

### 📝 MODIFIED Files (24)

#### Backend Core
- **`backend/main.py`** - **MAJOR REFACTOR**: Reduced from 4,108 lines to 98 lines
  - All route handlers moved to separate `routes/` modules
  - Now only handles Flask app initialization and blueprint registration
  - Much cleaner and more maintainable architecture

- `backend/config.py` - Configuration updates (4 lines changed)
- `backend/database.py` - Database connection improvements (6 lines changed)

#### Backend Utils
- `backend/utils/azure_dlp_client.py` - Azure DLP client enhancements (133 lines changed)
- `backend/utils/deduplication.py` - Deduplication improvements (82 lines changed)
- `backend/utils/metadata_extractor.py` - Metadata extraction updates (13 lines changed)
- `backend/utils/sql_lineage_extractor.py` - **MAJOR ENHANCEMENT**: Enhanced SQL parsing (894 lines changed)
  - Added Phase 1 features: stored procedure parsing, dynamic SQL extraction
  - Enhanced transformation detection (aggregation, window functions, CASE, JSON, regex, etc.)
  - Improved column-level lineage extraction

#### Airflow
- `airflow/airflow.cfg` - Airflow configuration updates (3 lines changed)
- `airflow/config/__init__.py` - Config updates (2 lines changed)
- `airflow/logs/scheduler/latest` - Log file (should not be committed)

#### Database
- `database/lineage_schema.sql` - Lineage schema updates (2 lines changed)
- `database/schema.sql` - Main schema updates (6 lines changed)

#### Docker
- `docker/Dockerfile.backend` - Backend Dockerfile updates (2 lines changed)
- `docker/docker-compose.yml` - Docker Compose updates (32 lines changed)

#### Frontend
- `frontend/package-lock.json` - Package lock updates (1 line changed)
- `frontend/src/components/ManualLineageDialog.jsx` - Manual lineage dialog updates (42 lines changed)
- `frontend/src/pages/AssetsPage.jsx` - **MAJOR UPDATE**: Assets page improvements (982 lines changed)
- `frontend/src/pages/ConnectorsPage.jsx` - **MAJOR UPDATE**: Connectors page improvements (541 lines changed)
- `frontend/src/pages/DataLineagePage.jsx` - **MAJOR UPDATE**: Lineage page enhancements (861 lines changed)
  - Added hierarchical vs actual lineage toggle
  - Improved node details display
  - Enhanced data governance integration

#### Infrastructure
- `nginx/torro-reverse-proxy.conf` - Nginx configuration updates (47 lines changed)
- `restart_services.sh` - Service restart script updates (2 lines changed)

---

## Key Architectural Changes

### 1. **Backend Refactoring** (Major)
- **Before**: Monolithic `main.py` with 4,108 lines containing all routes
- **After**: Modular architecture with separate route modules
  - Routes organized by domain (assets, connections, discovery, lineage, etc.)
  - Services layer for business logic
  - Utils layer for utilities
  - Much more maintainable and testable

### 2. **Lineage System** (Major Enhancement)
- New comprehensive lineage system with:
  - Lineage ingestion service
  - Lineage traversal service
  - Lineage visualization service
  - Manual lineage service
  - SQL lineage integration
  - Folder-based lineage
  - Asset-lineage integration

### 3. **Oracle Database Support** (New Feature)
- Oracle DB client for on-premises connections
- Oracle lineage extractor
- Oracle discovery service
- Docker setup for Oracle testing
- Banking test data

### 4. **SQL Parsing Enhancements** (Phase 1 Implementation)
- Stored procedure parser (PL/SQL, T-SQL)
- Dynamic SQL extraction
- Enhanced transformation detection:
  - Aggregations
  - Window functions
  - CASE statements
  - Mathematical functions
  - String functions
  - Date/time functions
  - Type casting
  - COALESCE/NVL
  - JSON path
  - Regex
  - Pivot/unpivot
  - Explode/flatten

### 5. **Discovery Service** (Major Enhancement)
- Comprehensive discovery service (1,835 lines)
- Optimized batch processing
- Progress tracking
- Azure Blob and Oracle support

### 6. **Frontend Enhancements**
- Improved Assets page with better UI/UX
- Enhanced Connectors page
- Major lineage page improvements:
  - Hierarchical vs Actual lineage toggle
  - Better node details
  - Data governance integration

---

## Performance Improvements

1. **Database Optimization**
   - Batch commits for discovery
   - Progress tracking
   - Connection pooling improvements

2. **Discovery Optimization**
   - Optimized Oracle DB discovery
   - Azure Blob-style improvements
   - Batch processing

3. **500+ Users Support**
   - Performance optimizations
   - Azure DLP integration

---

## Files to NOT Commit

These files should remain uncommitted:
- `airflow/logs/scheduler/latest` - Log file (auto-generated)
- `backend/.env.backup.*` - Environment backup files (should be in .gitignore)

---

## Migration Notes

If merging `new-connector` into `main`:

1. **Database Migration Required**
   - Run `database/migrations/create_lineage_tables.sql`
   - Run `database/migrations/apply_lineage_migration.sh`
   - Apply `database/add_indexes.sql`

2. **Dependencies**
   - Check `backend/requirements.txt` for new packages
   - Install Oracle client libraries if using Oracle

3. **Configuration**
   - Update `.env` with new configuration options
   - Review `backend/config.py` for new settings

4. **Frontend**
   - Run `npm install` to update dependencies

5. **Docker**
   - Review `docker/docker-compose.yml` changes
   - Oracle setup available in `docker/docker-compose.oracle.yml`

---

## Testing Recommendations

1. Test all new route endpoints
2. Test Oracle discovery functionality
3. Test lineage extraction from stored procedures
4. Test enhanced SQL parsing
5. Test frontend lineage visualization
6. Test performance with large datasets

