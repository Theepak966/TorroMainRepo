# Complete Changes Summary: Current Directory vs Main Branch

## Overview
This document provides a comprehensive list of ALL changes between the current working directory (`publishing2` branch) and the `main` branch, including both committed and uncommitted changes, as well as stashed changes.

---

## 📊 Statistics

### Current Working Directory Changes
- **16 files changed**
- **2,776 lines added**
- **1,770 lines removed**
- **Net: +1,006 lines**

### Stashed Changes (stash@{0})
- **4 files changed**
- **1 line added**
- **226 lines removed**
- **Net: -225 lines**

---

## 🗑️ Deleted Files

### From Working Directory (2 files)
1. **`README.md`** (1,165 lines)
   - Documentation file removed

2. **`clear_all_data.py`** (98 lines)
   - Database cleanup utility script removed

### From Stash (3 files)
1. **`restart_services.sh`** (62 lines)
   - Service restart script (deleted in stash, but file still exists in working directory)

2. **`start_services.sh`** (68 lines)
   - Service start script (deleted in stash, but file still exists in working directory)

3. **`stop_services.sh`** (95 lines)
   - Service stop script (deleted in stash, but file still exists in working directory)

**Note**: The service scripts were deleted in the stash but still exist in the current working directory. This suggests they were restored or the stash was created before they were restored.

---

## ✏️ Modified Files

### Backend Files (5 files)

#### 1. `backend/main.py` 
- **+463 lines, -83 lines**
- **Major Changes:**
  - ✅ Added masking logic support for analytical and operational users
  - ✅ Fixed `discovery_id` stability issue in approve/reject/publish endpoints
  - ✅ Added `normalize_column_schema()` and `normalize_columns()` functions
  - ✅ Updated `/api/assets/<asset_id>/columns/<column_name>/pii` endpoint to accept and store masking logic
  - ✅ Updated `approve_asset`, `reject_asset`, and `publish_asset` endpoints to prevent new discovery record creation
  - ✅ Ensured masking logic fields are always present in API responses
  - ✅ **Data quality score caching optimization**
    - Only calculates quality score if not cached or schema changed
    - Uses `quality_columns_hash` to detect schema changes
    - Caches `data_quality_score`, `quality_metrics`, `quality_issues`, and `quality_columns_hash` in `operational_metadata`
    - Prevents unnecessary recalculation on every API call
    - Defaults to score of 50 if calculation fails but columns exist
    - Defaults to score of 75 if columns exist but calculation fails

#### 2. `backend/utils/azure_blob_client.py`
- **+142 lines, -10 lines**
- Updates to Azure Blob Storage client utilities

#### 3. `backend/utils/azure_dlp_client.py`
- **+207 lines, -84 lines**
- Updates to Azure DLP (Data Loss Prevention) client utilities

#### 4. `backend/utils/metadata_extractor.py`
- **+55 lines, -33 lines**
- Updates to metadata extraction utilities

### Frontend Files (4 files)

#### 1. `frontend/src/pages/AssetsPage.jsx`
- **+1,597 lines, -202 lines**
- **Major Changes:**
  - ✅ **Filters converted to checkboxes (multi-select)**
    - Changed from single-select dropdowns to multi-select checkboxes
    - Type filter: `useState('')` → `useState([])` with checkbox menu
    - Catalog filter: `useState('')` → `useState([])` with checkbox menu
    - Approval status filter: `useState('')` → `useState([])` with checkbox menu
    - Filter logic changed from `===` to `.includes()` for array-based filtering
    - Added menu anchors for multi-select filter dropdowns
  - ✅ **Application name filter added**
    - New `applicationNameFilter` state variable
    - New `applicationMenuAnchor` for filter menu
    - Extracts unique application names from `asset.business_metadata?.application_name`
    - Filter UI with checkboxes in dropdown menu
    - Integrated into `fetchAssets()` filtering logic
    - Shows "All Applications" or "{count} Selected" in button
  - ✅ Added "Masking logic for Analytical User" column
  - ✅ Added "Masking logic for Operational User" column
  - ✅ Implemented PII-type-specific masking options (Email, PhoneNumber, SSN, etc.)
  - ✅ Conditional rendering: masking columns only appear for PII columns or when changing to PII
  - ✅ Added `columnMaskingLogic` state management
  - ✅ Added `originalPiiStatus` tracking for change detection
  - ✅ Added `unsavedMaskingChanges` and `savingMaskingLogic` state
  - ✅ Implemented `handleSaveMaskingLogic()` function for direct table saves
  - ✅ Updated `handleOpenPiiDialog()` to initialize masking logic
  - ✅ Updated `handleSavePii()` to save masking logic to backend
  - ✅ Added `MASKING_OPTIONS` object with PII-type-specific options
  - ✅ Added `getMaskingOptions()` helper function
  - ✅ **Department field added to business metadata**
    - New `department` state variable with dropdown selection
    - List of 15 departments (Data Engineering, Data Science, Business Intelligence, etc.)
    - Integrated into asset details dialog
  - ✅ **Governance rejection reasons system**
    - Structured rejection reasons with codes (001-011)
    - 11 predefined rejection reasons (Data Quality, Privacy Violation, Compliance Risk, etc.)
    - Custom rejection reason option ("Others")
    - Automatic tag generation from rejection reasons
    - `getRejectionTag()` function to convert reason codes to tags
  - ✅ **Business Glossary Tags integration**
    - Tag management system for assets
    - Fetch metadata tags from API (`/api/metadata-tags`)
    - Add/remove tags from asset descriptions
    - Tag dialog with searchable tag list
    - Tags stored in `business_metadata.tags`
  - ✅ **Rejection tag automation**
    - Automatically adds "REJECTED: [reason]" tag when rejecting assets
    - Filters out existing rejection tags and "torrocon" tag
    - Updates business metadata with rejection tag
  - ✅ Fixed syntax errors and code cleanup

#### 2. `frontend/src/pages/ConnectorsPage.jsx`
- **+51 lines, -41 lines**
- Updates to connectors page

#### 3. `frontend/src/pages/DataLineagePage.jsx`
- **+73 lines, -17 lines**
- Updates to data lineage page

#### 4. `frontend/src/components/Sidebar.jsx`
- **+20 lines, -6 lines**
- Updates to sidebar component

### Airflow Files (5 files)

#### 1. `airflow/dags/azure_blob_discovery_dag.py`
- **+67 lines, -21 lines**
- **Changes:**
  - ✅ Fixed syntax errors (incomplete SQL strings)
  - ✅ Fixed indentation issues
  - ✅ Code cleanup and error fixes

#### 2. `airflow/dags/sql_lineage_extraction_dag.py`
- **+51 lines, -1 line**
- **Changes:**
  - ✅ Fixed syntax errors (incomplete SQL strings)
  - ✅ Fixed indentation issues
  - ✅ Code cleanup and error fixes

#### 3. `airflow/utils/deduplication.py`
- **+11 lines, -6 lines**
- **Changes:**
  - ✅ Fixed syntax errors (incomplete SQL strings)
  - ✅ Code cleanup

#### 4. `airflow/utils/email_notifier.py`
- **+27 lines, -1 line**
- **Changes:**
  - ✅ Fixed syntax errors (incomplete HTML strings)
  - ✅ Code cleanup

#### 5. `airflow/config/azure_config.py`
- **+11 lines, -1 line**
- **Changes:**
  - ✅ Fixed syntax errors (incomplete SQL strings)
  - ✅ Code cleanup

### Log Files (1 file)
- **`airflow/logs/scheduler/latest`**
  - Log file update (not a code change, appears in both working directory and stash)

---

## 🎯 Key Features Added

### 1. Filters Converted to Checkboxes (Multi-Select)
**Location**: `frontend/src/pages/AssetsPage.jsx`

**Description**: 
- Converted all filters from single-select to multi-select checkboxes
- Users can now select multiple values for Type, Catalog, Approval Status, and Application Name filters
- Improved UX with checkbox-based selection in dropdown menus
- Filter buttons show "All [Category]" or "{count} Selected" based on selection

**Technical Implementation**:
- Changed filter state from strings to arrays: `useState('')` → `useState([])`
- Added menu anchors for each filter dropdown
- Updated filter logic from `===` to `.includes()` for array-based filtering
- Added `handleTypeFilterChange()`, `handleCatalogFilterChange()`, `handleApprovalStatusFilterChange()`, `handleApplicationNameFilterChange()` functions

### 2. Application Name Filter Added
**Location**: `frontend/src/pages/AssetsPage.jsx`

**Description**: 
- Added new filter for Application Name
- Extracts unique application names from `asset.business_metadata?.application_name`
- Integrated into the filter bar with checkbox-based multi-select
- Works seamlessly with other filters

**Technical Implementation**:
- New state: `applicationNameFilter` (array)
- New state: `applicationMenuAnchor` (for menu positioning)
- Extracts: `uniqueApplicationNames` from assets
- Filter logic: `applicationNameFilter.includes(appName)`
- UI: Checkbox menu similar to other filters

### 3. Masking Logic for PII Columns
**Location**: `frontend/src/pages/AssetsPage.jsx` and `backend/main.py`

**Description**: 
- Added two new columns for masking logic (Analytical User and Operational User)
- Masking options are PII-type-specific:
  - **Email**: Mask domain, Full mask, Partial mask, Show domain only, etc.
  - **PhoneNumber**: Show last 4 digits, Full mask, Partial mask, etc.
  - **SSN**: Show last 4 digits, Full mask, etc.
  - **CreditCard**: Show last 4 digits, Full mask, etc.
  - And many more PII types with specific masking options
- Columns only appear for PII columns or when a column is being changed to PII
- Masking logic is saved and persisted in the database
- Backend ensures masking logic fields are always present in API responses (normalized)

**Technical Implementation**:
- Frontend state management with `columnMaskingLogic`, `originalPiiStatus`, `unsavedMaskingChanges`, `savingMaskingLogic`
- Backend normalization functions ensure consistent schema
- Direct save functionality from table without opening dialog

### 4. Discovery ID Stability Fix
**Location**: `backend/main.py`

**Description**: 
- Fixed critical issue where `discovery_id` was changing after asset approval/rejection/publishing
- Modified `approve_asset`, `reject_asset`, and `publish_asset` endpoints to:
  - Only update existing discovery records (no new record creation)
  - Use consistent query logic: `order_by(DataDiscovery.id.desc()).first()`
  - Only include `discovery_id` in response if discovery record exists

### 6. Department Field Added
**Location**: `frontend/src/pages/AssetsPage.jsx`

**Description**: 
- Added Department field to business metadata
- Dropdown selection with 15 predefined departments
- Integrated into asset details dialog
- Departments include: Data Engineering, Data Science, Business Intelligence, IT Operations, Security & Compliance, Finance, Risk Management, Customer Analytics, Product Development, Marketing, Sales, Human Resources, Legal, Operations, Other

### 7. Governance Rejection Reasons System
**Location**: `frontend/src/pages/AssetsPage.jsx`

**Description**: 
- Structured rejection reasons with codes (001-011)
- 11 predefined rejection reasons:
  - 001: Data Quality Issues
  - 002: Data Privacy Violation
  - 003: Compliance Risk
  - 004: Data Classification Mismatch
  - 005: Archive / Duplicate
  - 006: Data Lineage Issues
  - 007: Metadata Incomplete
  - 008: Data Retention Policy Violation
  - 009: Access Control Issues
  - 010: Data Source Not Authorized
  - 011: Others (with custom reason input)
- Automatic tag generation from rejection reasons
- Rejection tags automatically added to asset when rejected

### 8. Business Glossary Tags Integration
**Location**: `frontend/src/pages/AssetsPage.jsx`

**Description**: 
- Tag management system for assets
- Fetches metadata tags from `/api/metadata-tags` endpoint
- Add/remove tags from asset descriptions
- Tag dialog with searchable tag list
- Tags stored in `business_metadata.tags` array
- Supports adding tags to asset descriptions

### 9. Data Quality Score Caching
**Location**: `backend/main.py`

**Description**: 
- Performance optimization for data quality score calculation
- Only calculates quality score if:
  - Not cached, OR
  - Schema changed (detected via `quality_columns_hash`), OR
  - Quality metrics missing
- Caches results in `operational_metadata`:
  - `data_quality_score`
  - `quality_metrics`
  - `quality_issues`
  - `quality_columns_hash`
- Prevents unnecessary recalculation on every API call
- Improves API response time for assets with cached scores

### 10. Code Quality Improvements
**Location**: Multiple Airflow DAGs and utility files

**Description**:
- Fixed all syntax errors (incomplete SQL/HTML strings)
- Fixed indentation issues
- Code cleanup and standardization
- All Python files now have valid syntax

---

## 📝 Technical Details

### Backend API Changes

#### Endpoint: `PUT /api/assets/<asset_id>/columns/<column_name>/pii`
**Changes**:
- Now accepts `masking_logic_analytical` and `masking_logic_operational` in request body
- Stores masking logic in column's JSON metadata
- Clears masking logic if `pii_detected` is set to `False`
- Returns normalized column schema with masking logic fields always present

#### Endpoints: `POST /api/assets/<asset_id>/approve`, `/reject`, `/publish`
**Changes**:
- Fixed to prevent creation of new `DataDiscovery` records
- Only updates existing discovery records
- Ensures `discovery_id` consistency across all operations
- Uses same query logic as GET endpoint for consistency

### Frontend State Management

**New State Variables**:
```javascript
const [columnMaskingLogic, setColumnMaskingLogic] = useState({});
const [originalPiiStatus, setOriginalPiiStatus] = useState({});
const [unsavedMaskingChanges, setUnsavedMaskingChanges] = useState({});
const [savingMaskingLogic, setSavingMaskingLogic] = useState({});
```

**New Functions**:
- `handleSaveMaskingLogic(columnName)` - Saves masking logic directly from table
- `getMaskingOptions(column, userType)` - Returns PII-type-specific masking options
- Updated `handleOpenPiiDialog()` - Initializes masking logic state
- Updated `handleSavePii()` - Saves masking logic to backend

### PII Type-Specific Masking Options

Implemented comprehensive masking options for:
- Email
- PhoneNumber
- SSN
- CreditCard
- PersonName
- Address
- DateOfBirth
- IPAddress
- AccountNumber
- CustomerID
- TransactionID
- UserID
- ID
- PassportNumber
- DriverLicense
- BankAccount
- MedicalRecord
- LicensePlate
- Password
- Gender
- Race
- Religion
- Biometric
- Default (for unknown PII types)

---

## 🧹 Files Cleaned Up (Not in Git Diff)

These files were removed manually but are not tracked in git:
- Temporary inspection report files (.txt files)
- Debug/inspection scripts (already removed)
- Python cache files (__pycache__ directories and .pyc files)

---

## 📌 Notes

1. **Branch Status**: The `main` and `publishing2` branches are at the same commit (18332b1), but there are uncommitted changes in the working directory.

2. **Stash Status**: There is a stash (`stash@{0}`) that contains deletion of service scripts, but those files still exist in the working directory, suggesting they were restored.

3. **All Changes Uncommitted**: All changes listed above are currently uncommitted in the working directory.

4. **Syntax Errors Fixed**: All syntax errors in Airflow DAGs and utility files have been fixed.

5. **Codebase Cleaned**: Unnecessary files (inspection scripts, temporary reports, cache files) have been removed.

---

## 🔍 File-by-File Line Changes

| File | Lines Added | Lines Removed | Net Change |
|------|------------|---------------|------------|
| `README.md` | 0 | 1,165 | -1,165 |
| `clear_all_data.py` | 0 | 98 | -98 |
| `backend/main.py` | 463 | 83 | +380 |
| `frontend/src/pages/AssetsPage.jsx` | 1,597 | 202 | +1,395 |
| `backend/utils/azure_dlp_client.py` | 207 | 84 | +123 |
| `backend/utils/azure_blob_client.py` | 142 | 10 | +132 |
| `frontend/src/pages/DataLineagePage.jsx` | 73 | 17 | +56 |
| `backend/utils/metadata_extractor.py` | 55 | 33 | +22 |
| `frontend/src/pages/ConnectorsPage.jsx` | 51 | 41 | +10 |
| `airflow/dags/sql_lineage_extraction_dag.py` | 51 | 1 | +50 |
| `airflow/dags/azure_blob_discovery_dag.py` | 67 | 21 | +46 |
| `airflow/utils/email_notifier.py` | 27 | 1 | +26 |
| `frontend/src/components/Sidebar.jsx` | 20 | 6 | +14 |
| `airflow/config/azure_config.py` | 11 | 1 | +10 |
| `airflow/utils/deduplication.py` | 11 | 6 | +5 |
| `airflow/logs/scheduler/latest` | 1 | 1 | 0 |

**Total**: 2,776 lines added, 1,770 lines removed, **Net: +1,006 lines**

---

*Generated: $(date)*
*Branch: publishing2*
*Base: main (commit 18332b1)*

