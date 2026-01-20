# Torro Data Discovery Platform

Enterprise-grade data discovery and lineage platform with Oracle database support, comprehensive lineage tracking, and Azure Blob Storage integration.

## 🚀 Features

### Core Features
- **Multi-Connector Support**: Azure Blob Storage, Oracle Database
- **Data Discovery**: Automated discovery of data assets with metadata extraction
- **Data Lineage**: Complete lineage tracking with SQL parsing, manual lineage, and visualization
- **Asset Management**: Comprehensive asset catalog with technical, operational, and business metadata
- **Nginx Reverse Proxy**: Production-ready reverse proxy with SSL/TLS support
- **Azure MySQL Integration**: Cloud-native database backend

### New Features (new-connector branch)
- ✅ **Oracle Database Connector**: Full support for Oracle DB with JDBC and standard connection methods
- ✅ **Advanced Data Lineage System**: Multi-level lineage tracking (dataset, process, column-level)
- ✅ **Modular Architecture**: Clean separation of routes, services, and utilities
- ✅ **Performance Optimizations**: Database indexes, connection pooling, query optimizations

---

## 📋 Table of Contents

1. [Database Schema](#database-schema)
2. [SQL Migrations](#sql-migrations)
3. [Code Architecture](#code-architecture)
4. [Setup Instructions](#setup-instructions)
5. [API Documentation](#api-documentation)
6. [Deployment](#deployment)

---

## 🗄️ Database Schema

### Core Tables

#### 1. Assets Table
Stores discovered data assets with metadata.

```sql
CREATE TABLE assets (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    catalog VARCHAR(255),
    connector_id VARCHAR(255),
    discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    technical_metadata JSON,
    operational_metadata JSON,
    business_metadata JSON,
    columns JSON
);
```

**Indexes:**
- `idx_catalog` - Catalog-based queries
- `idx_connector_id` - Connector-based queries
- `idx_type` - Type-based filtering

#### 2. Connections Table
Stores data source connection configurations.

```sql
CREATE TABLE connections (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    connector_type VARCHAR(255) NOT NULL,
    connection_type VARCHAR(255),
    config JSON,
    status VARCHAR(50) DEFAULT 'active',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes:**
- `idx_connector_type` - Filter by connector type
- `idx_status` - Filter by connection status

#### 3. Data Discovery Table
Tracks discovered files and their metadata.

**Key Features:**
- Generated columns for storage location parsing
- Full-text search on file names and paths
- Comprehensive indexing for performance
- Approval workflow support

**Indexes:**
- `idx_asset_id` - Link to assets
- `idx_storage_location` - Storage-based queries
- `idx_status` - Status filtering
- `idx_approval_status` - Approval workflow
- `idx_discovered_at` - Time-based queries
- `idx_common_query` - Composite index for common queries
- `FULLTEXT idx_fulltext_search` - Full-text search

### Lineage Tables

#### 4. Lineage Datasets Table
Stores lineage dataset information.

```sql
CREATE TABLE lineage_datasets (
    urn VARCHAR(512) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    catalog VARCHAR(255),
    schema_name VARCHAR(255),
    storage_type VARCHAR(50),
    storage_location JSON,
    table_lineage_enabled BOOLEAN DEFAULT FALSE,
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

#### 5. Lineage Processes Table
Stores process/job information for lineage tracking.

```sql
CREATE TABLE lineage_processes (
    urn VARCHAR(512) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    source_system VARCHAR(100),
    job_id VARCHAR(255),
    job_name VARCHAR(255),
    process_definition JSON,
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL
);
```

#### 6. Lineage Edges Table
Stores precomputed lineage relationships.

```sql
CREATE TABLE lineage_edges (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_urn VARCHAR(512) NOT NULL,
    process_urn VARCHAR(512) NOT NULL,
    target_urn VARCHAR(512) NOT NULL,
    relationship_type VARCHAR(50) DEFAULT 'transformation',
    edge_metadata JSON,
    valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME NULL,
    edge_hash VARCHAR(64) GENERATED ALWAYS AS (SHA2(...)) STORED,
    UNIQUE KEY unique_lineage_edge (edge_hash)
);
```

**Indexes:**
- `idx_edge_source` - Upstream queries
- `idx_edge_target` - Downstream queries
- `idx_edge_process` - Process-based queries
- `idx_edge_composite` - Composite queries

#### 7. Column Lineage Table
Stores column-level lineage mappings.

```sql
CREATE TABLE lineage_column_lineage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    edge_id INT NOT NULL,
    source_column VARCHAR(255) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    source_table VARCHAR(255),
    target_table VARCHAR(255),
    transformation_type VARCHAR(50),
    transformation_expression TEXT,
    FOREIGN KEY (edge_id) REFERENCES lineage_edges(id) ON DELETE CASCADE
);
```

#### 8. Lineage Audit Log Table
Tracks all lineage changes for compliance.

```sql
CREATE TABLE lineage_audit_log (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    action VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_urn VARCHAR(512) NOT NULL,
    old_data JSON,
    new_data JSON,
    user_id VARCHAR(255),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 9. Lineage Relationships Table
Stores actual data flow relationships.

```sql
CREATE TABLE lineage_relationships (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_asset_id VARCHAR(255) NOT NULL,
    target_asset_id VARCHAR(255) NOT NULL,
    relationship_type VARCHAR(50) DEFAULT 'transformation',
    column_lineage JSON,
    transformation_type VARCHAR(50),
    sql_query TEXT,
    source_system VARCHAR(100),
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    extraction_method VARCHAR(50),
    UNIQUE KEY unique_relationship (source_asset_id, target_asset_id, source_job_id),
    FOREIGN KEY (source_asset_id) REFERENCES assets(id) ON DELETE CASCADE,
    FOREIGN KEY (target_asset_id) REFERENCES assets(id) ON DELETE CASCADE
);
```

---

## 📝 SQL Migrations

### Migration Files

#### 1. `database/schema.sql`
Core database schema with assets, connections, and data discovery tables.

**Run:**
```bash
mysql -u username -p torroforairflow < database/schema.sql
```

#### 2. `database/lineage_schema.sql`
Lineage-specific tables (lineage_relationships, lineage_history, sql_queries).

**Run:**
```bash
mysql -u username -p torroforairflow < database/lineage_schema.sql
```

#### 3. `database/migrations/create_lineage_tables.sql`
Advanced lineage system with datasets, processes, edges, column lineage, and audit log.

**Run:**
```bash
mysql -u username -p torroforairflow < database/migrations/create_lineage_tables.sql
```

#### 4. `database/add_indexes.sql`
Performance optimization indexes for all tables.

**Run:**
```bash
mysql -u username -p torroforairflow < database/add_indexes.sql
```

**Indexes Added:**
- Assets: `idx_assets_connector_id`, `idx_assets_name`, `idx_assets_created_at`, `idx_assets_catalog_name`
- Connections: `idx_connections_status`, `idx_connections_connector_type`
- Data Discovery: `idx_discovery_connection_id`, `idx_discovery_status`, `idx_discovery_asset_id`
- Lineage: `idx_lineage_source_asset`, `idx_lineage_target_asset`, `idx_lineage_created_at`

#### 5. `database/migrations/apply_lineage_migration.sh`
Automated migration script that applies all lineage migrations.

**Run:**
```bash
chmod +x database/migrations/apply_lineage_migration.sh
./database/migrations/apply_lineage_migration.sh
```

---

## 🏗️ Code Architecture

### Backend Structure

```
backend/
├── config/
│   └── gunicorn_config.py          # Production WSGI server config
├── routes/                          # API route handlers (modular)
│   ├── assets.py                    # Asset management endpoints
│   ├── connections.py               # Connection management
│   ├── discovery.py                 # Discovery operations
│   ├── health.py                    # Health check endpoints
│   ├── lineage_routes.py            # Lineage API endpoints
│   ├── lineage_extraction.py        # Lineage extraction
│   ├── lineage_relationships.py     # Relationship management
│   ├── lineage_sql.py               # SQL lineage parsing
│   └── metadata.py                 # Metadata endpoints
├── services/                        # Business logic layer
│   ├── discovery_service.py        # Discovery orchestration
│   ├── lineage_ingestion.py        # Lineage data ingestion
│   ├── lineage_traversal.py         # Lineage graph traversal
│   ├── lineage_visualization.py     # Lineage diagram generation
│   ├── manual_lineage_service.py   # Manual lineage management
│   ├── sql_lineage_integration.py   # SQL parsing integration
│   ├── folder_based_lineage.py      # Folder hierarchy lineage
│   └── asset_lineage_integration.py # Asset-lineage integration
├── models_lineage/                  # Lineage data models
│   └── models_lineage.py            # SQLAlchemy models for lineage
├── scripts/                         # Standalone scripts
│   └── discovery_runner.py         # Discovery execution script
├── utils/                           # Utility functions
│   ├── oracle_db_client.py          # Oracle database client
│   ├── oracle_lineage_extractor.py  # Oracle-specific lineage
│   ├── azure_blob_lineage_extractor.py
│   ├── sql_lineage_extractor.py    # SQL parsing for lineage
│   ├── stored_procedure_parser.py   # Stored procedure parsing
│   └── helpers.py                   # Common utilities
├── config.py                        # Application configuration
├── database.py                      # Database connection setup
└── main.py                          # Flask application entry point
```

### Key Code Changes

#### 1. Oracle Database Connector

**File:** `backend/utils/oracle_db_client.py`

**Features:**
- JDBC URL support (multiple formats)
- Standard connection parameters (host, port, service_name)
- Schema filtering
- Comprehensive metadata extraction
- Support for tables, views, materialized views, procedures, functions, packages, triggers

**Connection Methods:**
```python
# JDBC URL
config = {
    'jdbc_url': 'jdbc:oracle:thin:@//host:port/service_name',
    'username': 'user',
    'password': 'pass'
}

# Standard Connection
config = {
    'host': 'db.example.com',
    'port': '1521',
    'service_name': 'ORCL',
    'username': 'user',
    'password': 'pass',
    'schema_filter': 'SCHEMA1,SCHEMA2'  # Optional
}
```

#### 2. Data Lineage System

**Files:**
- `backend/services/lineage_ingestion.py` - Ingests lineage data
- `backend/services/lineage_traversal.py` - Graph traversal algorithms
- `backend/services/lineage_visualization.py` - Diagram generation
- `backend/services/sql_lineage_integration.py` - SQL parsing integration
- `backend/utils/sql_lineage_extractor.py` - SQL query parsing

**Features:**
- Dataset-level lineage
- Process-level lineage
- Column-level lineage
- SQL query parsing
- Manual lineage creation
- Temporal lineage (valid_from/valid_to)
- Audit logging

#### 3. Modular Route Architecture

**Before:** All routes in `main.py` (4000+ lines)

**After:** Separated into logical modules:
- `routes/assets.py` - Asset CRUD operations
- `routes/connections.py` - Connection management
- `routes/discovery.py` - Discovery operations
- `routes/lineage_*.py` - Lineage endpoints

**Benefits:**
- Better code organization
- Easier maintenance
- Clear separation of concerns
- Improved testability

#### 4. Service Layer

Business logic extracted from routes into service classes:
- `discovery_service.py` - Orchestrates discovery operations
- `lineage_*` services - Handle lineage business logic
- Reusable across different entry points (API, scripts, DAGs)

---

## 🚀 Setup Instructions

### Prerequisites

- Python 3.8+
- Node.js 16+
- MySQL 8.0+ (or Azure MySQL)
- Oracle Client Libraries (for Oracle connector)

### 1. Database Setup

```bash
# Create database
mysql -u root -p
CREATE DATABASE torroforairflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# Run migrations
mysql -u root -p torroforairflow < database/schema.sql
mysql -u root -p torroforairflow < database/lineage_schema.sql
mysql -u root -p torroforairflow < database/migrations/create_lineage_tables.sql
mysql -u root -p torroforairflow < database/add_indexes.sql
```

### 2. Backend Setup

```bash
cd backend
python3.8 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Install Oracle driver (optional, for Oracle connector)
pip install oracledb  # or: pip install cx_Oracle

# Configure environment
cp .env.example .env
# Edit .env with your database credentials
```

**Backend Environment Variables:**
```bash
DB_HOST=your-mysql-host
DB_PORT=3306
DB_USER=your-username
DB_PASSWORD=your-password
DB_NAME=torroforairflow
FLASK_ENV=production
FLASK_HOST=0.0.0.0
FLASK_PORT=8099
```

### 3. Frontend Setup

```bash
cd frontend
npm install

# Configure API endpoint
echo "VITE_API_BASE_URL=http://localhost:8099" > .env
```

### 4. Airflow Setup

```bash
cd airflow
python3.8 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize Airflow
export AIRFLOW_HOME=$(pwd)
airflow db init

# Configure database connection in airflow.cfg
# Set sql_alchemy_conn to your MySQL connection string
```

### 5. Start Services

```bash
# Using startup script
./start_services.sh

# Or manually:
# Terminal 1: Backend
cd backend && source venv/bin/activate && python main.py

# Terminal 2: Frontend
cd frontend && npm run dev

# Terminal 3: Airflow Webserver
cd airflow && source venv/bin/activate && airflow webserver

# Terminal 4: Airflow Scheduler
cd airflow && source venv/bin/activate && airflow scheduler
```

### 6. Nginx Reverse Proxy (Production)

```bash
# Copy nginx configuration
sudo cp nginx/torro-reverse-proxy.conf /etc/nginx/nginx.conf

# Setup SSL certificates
sudo mkdir -p /etc/nginx/ssl
# Place your cert.pem and key.pem in /etc/nginx/ssl/

# Test and reload
sudo nginx -t
sudo systemctl reload nginx
```

---

## 📡 API Documentation

### Connection Management

#### Create Oracle Connection
```http
POST /api/connections
Content-Type: application/json

{
  "name": "Oracle Production",
  "connector_type": "oracle_db",
  "connection_type": "JDBC",
  "config": {
    "jdbc_url": "jdbc:oracle:thin:@//host:1521/SERVICE",
    "username": "user",
    "password": "pass",
    "schema_filter": "SCHEMA1,SCHEMA2"
  }
}
```

#### Test Connection
```http
POST /api/connections/test-config
Content-Type: application/json

{
  "config": {
    "jdbc_url": "jdbc:oracle:thin:@//host:1521/SERVICE",
    "username": "user",
    "password": "pass"
  }
}
```

### Lineage Endpoints

#### Ingest Process Lineage
```http
POST /api/lineage/process
Content-Type: application/json

{
  "process_urn": "urn:process:etl:job123",
  "process_name": "Daily ETL Job",
  "source_system": "airflow",
  "input_datasets": ["urn:dataset:azure:container/path/file.parquet"],
  "output_datasets": ["urn:dataset:azure:container/path/output.parquet"]
}
```

#### Get Lineage Graph
```http
GET /api/lineage/graph?urn=urn:dataset:azure:container/path/file.parquet&direction=downstream&depth=3
```

#### Create Manual Lineage
```http
POST /api/lineage/manual
Content-Type: application/json

{
  "source_asset_id": "asset123",
  "target_asset_id": "asset456",
  "relationship_type": "transformation",
  "column_lineage": [
    {"source_column": "col1", "target_column": "col1", "transformation": "pass_through"}
  ]
}
```

---

## 🐳 Docker Support

### Oracle Database Setup

```bash
# Start Oracle container
cd docker
./start-oracle.sh

# Or use docker-compose
docker-compose -f docker-compose.oracle.yml up -d
```

### Full Stack with Docker Compose

```bash
docker-compose up -d
```

---

## 🔧 Configuration

### Backend Configuration

**File:** `backend/config.py`

Key settings:
- Database connection pooling (pool_size=75, max_overflow=75)
- Connection recycling (pool_recycle=3600s)
- Discovery batch size (1500)
- Logging configuration

### Nginx Configuration

**File:** `nginx/torro-reverse-proxy.conf`

Features:
- HTTP to HTTPS redirect
- SSL/TLS configuration
- Reverse proxy for frontend, backend, and Airflow
- Security headers (HSTS, X-Frame-Options, etc.)

---

## 📊 Performance Optimizations

### Database Indexes
- Composite indexes for common query patterns
- Full-text search indexes
- Temporal indexes for time-based queries
- Foreign key indexes

### Connection Pooling
- SQLAlchemy connection pool (75 connections)
- Connection recycling to prevent stale connections
- Pool pre-ping for connection health checks

### Query Optimizations
- Batch operations for bulk inserts
- Pagination for large result sets
- Lazy loading for relationships
- Query result caching where appropriate

---

## 🔐 Security

### Authentication
- Oracle: Username/Password, JDBC URL support
- Azure: Service Principal, Connection String, Azure CLI fallback

### Security Headers (Nginx)
- Strict-Transport-Security
- X-Frame-Options
- X-Content-Type-Options
- X-XSS-Protection

### Data Protection
- Credentials stored in encrypted JSON config
- No credentials in frontend code
- Secure connection strings

---

## 📝 Changelog

### Version 2.0 (new-connector merge)

#### Added
- Oracle database connector with JDBC support
- Complete data lineage system
- Modular route architecture
- Service layer for business logic
- Advanced lineage tables (datasets, processes, edges, column lineage, audit log)
- Performance indexes
- Docker Oracle setup
- Nginx reverse proxy configuration

#### Changed
- Moved `discovery_runner.py` to `backend/scripts/`
- Moved `gunicorn_config.py` to `backend/config/`
- Refactored `main.py` into modular routes
- Enhanced frontend with expanded filter dropdowns

#### Fixed
- Database connection pooling
- Query performance with indexes
- Frontend API endpoint paths
- Filter dropdown widths

---

## 🤝 Contributing

1. Create a feature branch
2. Make your changes
3. Add tests if applicable
4. Update documentation
5. Submit a pull request

---

## 📄 License

[Your License Here]

---

## 🆘 Support

For issues and questions:
- Create an issue in the repository
- Check existing documentation
- Review SQL migration files in `database/` directory

---

## 🔗 Related Documentation

- [Database Schema Details](database/schema.sql)
- [Lineage Schema](database/migrations/create_lineage_tables.sql)
- [Performance Indexes](database/add_indexes.sql)
- [Nginx Configuration](nginx/torro-reverse-proxy.conf)

---

**Last Updated:** January 2026  
**Version:** 2.0 (new-connector merged)
