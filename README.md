# Torro Data Discovery Platform

A comprehensive data discovery and governance platform for Azure Storage services, featuring automated metadata extraction, PII detection, approval workflows, and data lineage tracking.

## Features

- **Complete Azure Storage Support**: Discover and catalog assets from:
  - Azure Blob Storage (containers and blobs)
  - Azure Data Lake Gen2 (ABFS paths)
  - Azure File Shares (file shares and files)
  - Azure Queues (queue storage)
  - Azure Tables (table storage)
- **Multiple Authentication Methods**: 
  - Connection String authentication
  - Service Principal authentication
- **Immediate & Scheduled Discovery**: 
  - Manual refresh button for immediate discovery
  - Airflow-powered scheduled discovery of new assets
- **Complete Metadata Extraction**: Extracts schema, columns, file properties, and PII detection for all file types
- **Approval Workflow**: Approve, reject, and publish discovered assets with full audit trail
- **Data Lineage**: Track relationships between data assets
- **High Performance**: Optimized for large-scale discovery (3000-5000+ assets)
- **Read-Only Operations**: All Azure operations are read-only (Storage Blob Data Reader role)

## Prerequisites

- Python 3.8+
- Node.js 16+
- MySQL 8.0+
- Apache Airflow 2.7.0
- Azure Storage Account (Blob Storage, File Shares, Queues, Tables)
- Azure Service Principal with **Storage Blob Data Reader** role (for Service Principal auth)
- OR Azure Storage Account connection string (for Connection String auth)

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Frontend   │──────│   Backend   │──────│   MySQL     │
│  (React)    │      │   (Flask)   │      │  Database   │
└─────────────┘      └─────────────┘      └─────────────┘
                            │
                            │
                     ┌─────────────┐
                     │   Airflow   │
                     │  (DAGs)     │
                     └─────────────┘
                            │
                            │
                     ┌─────────────┐
                     │   Azure     │
                     │ Data Lake   │
                     │   Gen2      │
                     └─────────────┘
```

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd torroforazure
```

### 2. Backend Setup

```bash
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create `backend/.env`:
```env
# Flask Configuration
FLASK_HOST=0.0.0.0
FLASK_PORT=8099
FLASK_ENV=development
FLASK_DEBUG=false

# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=torroforexcel

# Airflow Configuration
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow

# Logging
LOG_LEVEL=INFO
LOG_FILE=app.log
```

### 3. Airflow Setup

```bash
cd airflow
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize Airflow
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create --username airflow --password airflow --firstname Admin --lastname User --role Admin --email admin@example.com
```

Create `airflow/.env`:
```env
# Database Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=torroforexcel

# Airflow Configuration
AIRFLOW_DAG_SCHEDULE=0 * * * *
FRONTEND_URL=http://localhost:5162

# Optional: Azure AI Language (for PII detection)
AZURE_AI_LANGUAGE_ENDPOINT=
AZURE_AI_LANGUAGE_KEY=
```

### 4. Frontend Setup

```bash
cd frontend
npm install
```

Create `frontend/.env`:
```env
VITE_API_BASE_URL=http://localhost:8099
```

### 5. Database Setup

```bash
mysql -u root -p < database/schema.sql
mysql -u root -p < database/lineage_schema.sql
```

## Running the Application

### Option 1: Manual Start

**Backend:**
```bash
cd backend
python3 main.py
```

**Airflow:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
airflow webserver --port 8080
# In another terminal:
airflow scheduler
```

**Frontend:**
```bash
cd frontend
npm run dev
```

### Option 2: Docker (Recommended)

```bash
cd docker
docker-compose up -d
```

Access services:
- Frontend: http://localhost:5162
- Backend API: http://localhost:8099
- Airflow UI: http://localhost:8080
- MySQL: localhost:3307

## Azure Configuration

### Authentication Methods

The platform supports two authentication methods:

#### 1. Connection String Authentication

Use your Azure Storage Account connection string:
```
DefaultEndpointsProtocol=https;AccountName=<account-name>;AccountKey=<account-key>;EndpointSuffix=core.windows.net
```

#### 2. Service Principal Authentication

The platform can use **hardcoded Service Principal credentials** in the frontend for bank deployment. These are configured in `frontend/src/pages/ConnectorsPage.jsx`:

```javascript
const HARDCODED_AZURE_CREDENTIALS = {
  storage_account_name: 'hblazlakehousepreprdstg1',
  client_id: '6faaae79-2777-4cef-a3bf-b1a499c1a1ef',
  client_secret: 'YOUR_CLIENT_SECRET_HERE',
  tenant_id: '827fd022-05a6-4e57-be9c-cc069b6ae62d',
  datalake_paths: 'abfs://lh-enriched@hblazlakehousepreprdstg1.dfs.core.windows.net/visionplus/ATH3',
};
```

### Required Azure Permissions

For Service Principal authentication, the Service Principal must have:
- **Storage Blob Data Reader** role (read-only access)
- **Storage Queue Data Reader** role (for queue discovery)
- **Storage Table Data Reader** role (for table discovery)
- **Storage File Data SMB Share Reader** role (for file share discovery)
- Assigned at the Storage Account level
- For Data Lake Gen2, ensure ACLs grant Execute (X) on parent directories and Read (R) on files

For Connection String authentication, the connection string must have read permissions for all storage services.

### Supported Azure Storage Services

The platform discovers assets from:

1. **Azure Blob Storage**: Containers and blobs
2. **Azure Data Lake Gen2**: ABFS paths (e.g., `abfs://<container>@<account>.dfs.core.windows.net/<path>`)
3. **Azure File Shares**: File shares and files
4. **Azure Queues**: Queue storage
5. **Azure Tables**: Table storage

### Data Lake Gen2 Path Format

The platform supports ABFS paths:
```
abfs://<container>@<account>.dfs.core.windows.net/<path>
```

Example:
```
abfs://lh-enriched@hblazlakehousepreprdstg1.dfs.core.windows.net/visionplus/ATH3
```

## Performance

### Discovery Speed

- **3000-4000 files**: ~2-4 minutes (typical)
- **5000 files**: ~3-5 minutes (typical)
- **Best case**: ~1-2 minutes (fast network)
- **Worst case**: ~5-8 minutes (slow network, high latency)

### Optimization Features

- **Parallel Processing**: 50 workers per container, 10 containers in parallel (up to 500 files simultaneously)
- **Batch Commits**: 2000 files per database transaction
- **Smart Deduplication**: ETag-based quick hash checks to skip unchanged files
- **Complete Metadata**: All files get full metadata extraction (properties, samples, schema)

## API Endpoints

### Connections
- `GET /api/connections` - List all connections
- `POST /api/connections` - Create new connection
- `PUT /api/connections/<id>` - Update connection
- `DELETE /api/connections/<id>` - Delete connection
- `POST /api/connections/test-config` - Test connection without saving
- `GET /api/connections/<id>/containers` - List containers, file shares, queues, and tables
- `GET /api/connections/<id>/list-files` - List files in container/path or file share
- `POST /api/connections/<id>/discover` - Discover assets (immediate discovery)

### Assets
- `GET /api/assets` - List all assets
- `POST /api/assets` - Create assets
- `PUT /api/assets/<id>` - Update asset
- `POST /api/assets/<id>/approve` - Approve asset
- `POST /api/assets/<id>/reject` - Reject asset
- `POST /api/assets/<id>/publish` - Publish asset

### Discovery
- `GET /api/discovery` - List all discoveries
- `GET /api/discovery/<id>` - Get discovery by ID
- `PUT /api/discovery/<id>/approve` - Approve discovery
- `PUT /api/discovery/<id>/reject` - Reject discovery
- `POST /api/discovery/trigger` - Trigger Airflow DAG manually (background discovery)
- `GET /api/discovery/stats` - Get discovery statistics

### Health
- `GET /api/health` - Health check

## Bank VM Deployment

### Environment Variables

All configuration is done via environment variables. No hardcoded values (except Service Principal in frontend).

**Backend (.env):**
```env
FLASK_HOST=0.0.0.0
FLASK_PORT=8099
DB_HOST=<mysql-host>
DB_PORT=3306
DB_USER=<db-user>
DB_PASSWORD=<db-password>
DB_NAME=torroforexcel
AIRFLOW_BASE_URL=http://<airflow-host>:8080
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=<airflow-password>
```

**Airflow (.env):**
```env
MYSQL_HOST=<mysql-host>
MYSQL_PORT=3306
MYSQL_USER=<db-user>
MYSQL_PASSWORD=<db-password>
MYSQL_DATABASE=torroforexcel
AIRFLOW_DAG_SCHEDULE=0 * * * *
FRONTEND_URL=http://<frontend-host>:5162
```

**Frontend (.env):**
```env
VITE_API_BASE_URL=http://<backend-host>:8099
```

### Permissions

- **Azure**: Storage Blob Data Reader role (read-only)
- **Database**: Read/Write access to MySQL database
- **Network**: Access to Azure Storage Account, MySQL, and Airflow

### Deployment Checklist

- [ ] All environment variables configured
- [ ] Service Principal credentials hardcoded in frontend
- [ ] Database schema created
- [ ] Airflow DAGs loaded and active
- [ ] All services accessible on network
- [ ] Firewall rules configured
- [ ] Logs directory created and writable

## Docker Deployment

### Using Docker Compose

```bash
cd docker
docker-compose up -d
```

### Services

- **mysql**: MySQL 8.0 database (port 3307)
- **backend**: Flask backend API (port 8099)
- **airflow-webserver**: Airflow UI (port 8080)
- **airflow-scheduler**: Airflow scheduler
- **frontend**: React frontend (port 5162)

### Environment Variables

Set environment variables in `docker-compose.yml` or use `.env` files in respective directories.

## Troubleshooting

### Backend Not Starting

- Check `logs/backend.log` for errors
- Verify database connection in `backend/.env`
- Ensure port 8099 is available

### Airflow DAGs Not Running

- Check `logs/airflow-scheduler.log`
- Verify DAGs are not paused in Airflow UI
- Check `AIRFLOW_DAG_SCHEDULE` in `airflow/.env`
- Ensure `dags_are_paused_at_creation = False` in `airflow/airflow.cfg`

### Connection Test Fails

- Verify Service Principal credentials
- Check Azure RBAC role assignment (Storage Blob Data Reader)
- Verify ACLs for Data Lake Gen2 paths
- Check network connectivity to Azure

### Discovery Slow

- Check network latency to Azure
- Verify parallel processing is enabled (50 workers)
- Check database connection pool settings
- Monitor Airflow scheduler logs

### Refresh Button Not Working

- Verify backend API is accessible
- Check browser console for errors
- Ensure all connections are active
- Try manual discovery via API: `POST /api/connections/<id>/discover`

## File Structure

```
torroforazure/
├── backend/              # Flask backend
│   ├── main.py          # Main application
│   ├── utils/           # Utility modules
│   ├── models.py        # Database models
│   ├── config.py        # Configuration
│   └── .env             # Environment variables
├── airflow/             # Airflow DAGs
│   ├── dags/           # DAG definitions
│   ├── utils/          # Utility modules
│   ├── config/         # Configuration
│   └── .env            # Environment variables
├── frontend/           # React frontend
│   ├── src/           # Source code
│   └── .env           # Environment variables
├── database/          # Database schemas
├── docker/            # Docker configuration
└── logs/             # Application logs
```

## Security Notes

- Service Principal credentials can be hardcoded in frontend (bank requirement) OR use Connection String
- All Azure operations are read-only (Storage Blob Data Reader, Queue Data Reader, Table Data Reader, File Data Reader)
- Database credentials stored in environment variables
- Connection strings stored securely in database (encrypted at rest)
- All API calls use HTTPS for Azure operations

## Discovery Methods

### Immediate Discovery (Refresh Button)

Click the "Refresh" button in the UI to:
- Discover all new files immediately
- Update existing assets if schema changed
- Show results in real-time

### Scheduled Discovery (Airflow DAG)

The Airflow DAG runs automatically on schedule:
- Discovers new assets in the background
- Updates the database when complete
- Can be triggered manually via API

## Monitoring

### Logs

- Backend: `logs/backend.log`
- Airflow Webserver: `logs/airflow-webserver.log`
- Airflow Scheduler: `logs/airflow-scheduler.log`
- Frontend: `logs/frontend.log`

### Health Checks

- Backend: `GET http://localhost:8099/api/health`
- Airflow: `GET http://localhost:8080/health`

