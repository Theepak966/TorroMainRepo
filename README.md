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

### Step 1: System Prerequisites

#### Install Python 3.8+ (Linux/RHEL/CentOS)
```bash
sudo dnf install python38 python38-devel python38-pip gcc gcc-c++ make
# Or for Ubuntu/Debian:
# sudo apt-get update
# sudo apt-get install python3.8 python3.8-venv python3.8-dev build-essential
```

#### Install Node.js 16+ (Linux)
```bash
# Using NodeSource repository
curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
sudo dnf install -y nodejs
# Or for Ubuntu/Debian:
# curl -fsSL https://deb.nodesource.com/setup_20.x | sudo bash -
# sudo apt-get install -y nodejs

# Verify installation
node --version  # Should be 16+ or 20+
npm --version
```

#### Install MySQL 8.0+ (Linux)
```bash
sudo dnf install mysql mysql-server
# Or for Ubuntu/Debian:
# sudo apt-get install mysql-server

# Start MySQL service
sudo systemctl start mysqld
sudo systemctl enable mysqld

# Secure MySQL installation (set root password)
sudo mysql_secure_installation
```

### Step 2: Clone Repository

```bash
git clone <repository-url>
cd torrofinalv2release
```

### Step 3: Database Setup

```bash
# Create database
mysql -u root -p
CREATE DATABASE torroforexcel;
EXIT;

# Load schemas
mysql -u root -p torroforexcel < database/schema.sql
mysql -u root -p torroforexcel < database/lineage_schema.sql
```

### Step 4: Backend Setup

```bash
cd backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Install Gunicorn for production
pip install gunicorn
```

Create `backend/.env`:
```env
# Flask Configuration
FLASK_ENV=production
FLASK_DEBUG=false
SECRET_KEY=your-secret-key-here-change-in-production
ALLOWED_ORIGINS=*
LOG_LEVEL=INFO
LOG_FILE=app.log

# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=torroforexcel

# API Configuration
API_VERSION=v1

# Server Configuration
FLASK_HOST=0.0.0.0
FLASK_PORT=8099

# Airflow Configuration
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
```

**Backend Production Start (using Gunicorn):**
```bash
cd backend
source venv/bin/activate
gunicorn --bind 0.0.0.0:8099 --workers 4 --timeout 300 --access-logfile ../logs/backend-access.log --error-logfile ../logs/backend.log main:app
```

**Backend Development Start:**
```bash
cd backend
source venv/bin/activate
python main.py
```

### Step 5: Airflow Setup

```bash
cd airflow

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Set Airflow home
export AIRFLOW_HOME=$(pwd)

# Initialize Airflow database
airflow db init

# Create Airflow admin user
airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Configure Airflow (edit airflow.cfg)
# Set sql_alchemy_conn to MySQL:
# sql_alchemy_conn = mysql+pymysql://root:your_password@localhost:3306/torroforexcel
# Set base_url, web_server_port, secret_key, enable_proxy_fix
```

Create `airflow/.env`:
```env
# Database Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=torroforexcel

# Notification Configuration (Optional)
NOTIFICATION_EMAILS=
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=

# Airflow Configuration
AIRFLOW_DAG_SCHEDULE=0 * * * *
FRONTEND_URL=http://localhost:5162
```

**Airflow Configuration (airflow/airflow.cfg):**
```ini
[core]
sql_alchemy_conn = mysql+pymysql://root:your_password@localhost:3306/torroforexcel
dags_are_paused_at_creation = False

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080
secret_key = your-secret-key-here
enable_proxy_fix = True
```

**Start Airflow Webserver:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
airflow webserver --hostname 0.0.0.0 --port 8080
```

**Start Airflow Scheduler (in separate terminal):**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
airflow scheduler
```

### Step 6: Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Build for production (optional)
npm run build
```

Create `frontend/.env`:
```env
# Backend API Base URL
VITE_API_BASE_URL=http://localhost:8099

# Optional: Azure Service Principal Credentials (if hardcoding)
VITE_AZURE_STORAGE_ACCOUNT_NAME=
VITE_AZURE_CLIENT_ID=
VITE_AZURE_CLIENT_SECRET=
VITE_AZURE_TENANT_ID=
VITE_AZURE_DATALAKE_PATHS=
```

**Frontend Development Start:**
```bash
cd frontend
npm run dev
```

**Frontend Production Start:**
```bash
cd frontend
npm run build
# Serve with a web server (nginx, apache, or serve)
npx serve -s dist -l 5162
```

## Running the Application

### Option 1: Using Startup Script (Recommended)

The project includes a startup script that starts all services:

```bash
# Make script executable
chmod +x start_services.sh

# Start all services
./start_services.sh

# Check service status
ps aux | grep -E "gunicorn|airflow|node|vite"

# View logs
tail -f logs/backend.log
tail -f logs/airflow-webserver.log
tail -f logs/airflow-scheduler.log
tail -f logs/frontend.log
```

**Stop all services:**
```bash
# Kill all services
pkill -f "gunicorn.*main:app"
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
pkill -f "vite"
```

### Option 2: Manual Start (Development)

**Terminal 1 - Backend:**
```bash
cd backend
source venv/bin/activate
python main.py
# Or for production:
# gunicorn --bind 0.0.0.0:8099 --workers 4 main:app
```

**Terminal 2 - Airflow Webserver:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
airflow webserver --hostname 0.0.0.0 --port 8080
```

**Terminal 3 - Airflow Scheduler:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
airflow scheduler
```

**Terminal 4 - Frontend:**
```bash
cd frontend
npm run dev
```

### Option 3: Production Deployment with Nginx

#### Install and Configure Nginx

```bash
# Install Nginx
sudo dnf install nginx
# Or for Ubuntu/Debian:
# sudo apt-get install nginx

# Create Nginx configuration
sudo nano /etc/nginx/conf.d/torro-reverse-proxy.conf
```

**Nginx Configuration (`/etc/nginx/conf.d/torro-reverse-proxy.conf`):**
```nginx
# HTTP to HTTPS redirect
server {
    listen 80;
    server_name _;
    return 301 https://$host$request_uri;
}

# HTTPS server
server {
    listen 443 ssl http2;
    server_name _;

    # SSL certificates (self-signed for testing, use Let's Encrypt for production)
    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # Frontend
    location /airflow-fe/ {
        proxy_pass http://127.0.0.1:5162/airflow-fe/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Backend API
    location /airflow-be/api/ {
        rewrite ^/airflow-be/api(/.*)$ /api$1 break;
        proxy_pass http://127.0.0.1:8099;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Airflow UI
    location /airflow {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 3600s;
    }

    # Health check
    location /health {
        access_log off;
        return 200 "healthy\n";
        add_header Content-Type text/plain;
    }
}
```

**Generate SSL Certificates (Self-Signed):**
```bash
sudo mkdir -p /etc/nginx/ssl
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/ssl/key.pem \
    -out /etc/nginx/ssl/cert.pem
```

**Enable SELinux for Nginx (if using SELinux):**
```bash
sudo setsebool -P httpd_can_network_connect 1
```

**Start Nginx:**
```bash
sudo systemctl start nginx
sudo systemctl enable nginx
sudo nginx -t  # Test configuration
sudo systemctl reload nginx
```

**Configure Firewall:**
```bash
# Allow HTTP and HTTPS
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload

# Or for Ubuntu/Debian:
# sudo ufw allow 80/tcp
# sudo ufw allow 443/tcp
```

**Access Services via Nginx:**
- Frontend: `https://your-server-ip/airflow-fe/`
- Backend API: `https://your-server-ip/airflow-be/api/health`
- Airflow UI: `https://your-server-ip/airflow/`

### Option 4: Docker (Alternative)

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

### Complete Deployment Checklist

**System Setup:**
- [ ] Python 3.8+ installed
- [ ] Node.js 16+ installed
- [ ] MySQL 8.0+ installed and running
- [ ] All system dependencies installed (gcc, make, etc.)

**Database Setup:**
- [ ] MySQL service started
- [ ] Database `torroforexcel` created
- [ ] Schema loaded (`database/schema.sql`)
- [ ] Lineage schema loaded (`database/lineage_schema.sql`)
- [ ] Database user has proper permissions

**Backend Setup:**
- [ ] Virtual environment created
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Gunicorn installed for production
- [ ] `backend/.env` file created with correct values
- [ ] Backend can connect to MySQL
- [ ] Backend starts without errors

**Airflow Setup:**
- [ ] Virtual environment created
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Airflow database initialized (`airflow db init`)
- [ ] Airflow admin user created
- [ ] `airflow/.env` file created
- [ ] `airflow/airflow.cfg` configured (MySQL connection, base_url, secret_key)
- [ ] Airflow webserver starts
- [ ] Airflow scheduler starts
- [ ] DAGs are visible in Airflow UI and not paused

**Frontend Setup:**
- [ ] Node.js dependencies installed (`npm install`)
- [ ] `frontend/.env` file created with correct API URL
- [ ] Frontend builds without errors (`npm run build`)
- [ ] Frontend dev server starts (`npm run dev`)

**Azure Configuration:**
- [ ] Service Principal created (if using SP auth)
- [ ] RBAC roles assigned to Service Principal:
  - [ ] Storage Blob Data Reader
  - [ ] Storage Queue Data Reader
  - [ ] Storage Table Data Reader
  - [ ] Storage File Data SMB Share Reader
- [ ] OR Connection String obtained (if using connection string auth)
- [ ] Test connection successful

**Production Deployment (if using Nginx):**
- [ ] Nginx installed
- [ ] SSL certificates generated/obtained
- [ ] Nginx configuration created
- [ ] SELinux configured (if applicable)
- [ ] Firewall rules configured (ports 80, 443)
- [ ] All services accessible via Nginx

**Service Management:**
- [ ] `start_services.sh` script works
- [ ] All services start successfully
- [ ] Logs directory exists and is writable
- [ ] Services restart after reboot (if using systemd)

**Testing:**
- [ ] Frontend accessible
- [ ] Backend health check returns 200
- [ ] Airflow UI accessible
- [ ] Can create connection
- [ ] Can test connection
- [ ] Can discover assets
- [ ] Refresh button works
- [ ] Airflow DAGs run successfully

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

