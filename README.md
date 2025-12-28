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

- **Operating System**: Linux (RHEL/CentOS 8+ or Ubuntu 20.04+)
- **Python**: 3.8+ (Python 3.8 recommended)
- **Node.js**: 20.19+ (required for Vite)
- **MySQL**: 8.0+
- **Apache Airflow**: 2.7.0
- **Azure Storage Account**: Blob Storage, File Shares, Queues, Tables
- **Azure Service Principal** (optional): With required RBAC roles for Service Principal authentication
- **OR Azure Storage Account Connection String** (for Connection String authentication)
- **Disk Space**: Minimum 20GB free space (recommended 50GB+)
- **RAM**: Minimum 4GB (recommended 8GB+)

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
                     │   Storage   │
                     └─────────────┘
```

## Installation Guide

### Step 1: System Prerequisites

#### Install Python 3.8+ (RHEL/CentOS 8+)

```bash
sudo dnf install python38 python38-devel python38-pip gcc gcc-c++ make -y
```

#### Install Python 3.8+ (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install python3.8 python3.8-venv python3.8-dev build-essential -y
```

#### Install Node.js 20.19+ (RHEL/CentOS)

```bash
curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
sudo dnf install -y nodejs
```

#### Install Node.js 20.19+ (Ubuntu/Debian)

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo bash -
sudo apt-get install -y nodejs
```

**Verify installation:**
```bash
node --version  # Should be 20.19+
npm --version
```

#### Install MySQL 8.0+ (RHEL/CentOS)

```bash
sudo dnf install mysql mysql-server -y
sudo systemctl start mysqld
sudo systemctl enable mysqld
```

#### Install MySQL 8.0+ (Ubuntu/Debian)

```bash
sudo apt-get install mysql-server -y
sudo systemctl start mysql
sudo systemctl enable mysql
```

**Secure MySQL installation:**
```bash
sudo mysql_secure_installation
# Set root password when prompted
```

**Configure MySQL for production (increase max_connections):**
```bash
sudo nano /etc/my.cnf  # or /etc/mysql/mysql.conf.d/mysqld.cnf on Ubuntu
```

Add or update:
```ini
[mysqld]
max_connections = 250
```

Restart MySQL:
```bash
sudo systemctl restart mysqld  # or mysql on Ubuntu
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
```

In MySQL prompt:
```sql
CREATE DATABASE torroforexcel;
EXIT;
```

**Load database schemas:**
```bash
mysql -u root -p torroforexcel < database/schema.sql
mysql -u root -p torroforexcel < database/lineage_schema.sql
```

**Verify database:**
```bash
mysql -u root -p torroforexcel -e "SHOW TABLES;"
```

#### Database Storage Overview

All application data is stored in the `torroforexcel` database. This includes:

**Application Tables (6 tables):**
- `assets` - Discovered data assets (files, tables, queues, file shares, etc.)
- `connections` - Azure Storage connection configurations (connection strings, service principals)
- `data_discovery` - Discovery run records, metadata, and status tracking
- `lineage_relationships` - Data lineage connections between assets
- `lineage_history` - History of lineage changes and updates
- `sql_queries` - SQL query metadata for lineage extraction

**Airflow Metadata:**
- Airflow's internal metadata (DAGs, task instances, runs, logs) is also stored in `torroforexcel`
- Configured via `airflow.cfg`: `sql_alchemy_conn = mysql+pymysql://...torroforexcel`

**Note:** The database name `torroforexcel` is the original project name and is used throughout the codebase. All components (Backend, Airflow, Frontend) use this single database for centralized data storage.

### Step 4: Backend Setup

```bash
cd backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Install Gunicorn for production
pip install gunicorn
```

**Create `backend/.env` file:**
```bash
cd backend
nano .env
```

**Backend `.env` file content:**
```env
# Flask Configuration
FLASK_ENV=production
FLASK_DEBUG=false
SECRET_KEY=your-secret-key-here-change-in-production-generate-random-string
ALLOWED_ORIGINS=*
LOG_LEVEL=INFO
LOG_FILE=app.log

# Database Configuration
# IMPORTANT:
# - VM / non-Docker: use your MySQL hostname/IP (example: torrodb.mysql.database.azure.com)
# - Docker Compose: use the mysql service name: mysql
DB_HOST=your-mysql-host
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=torroforexcel

# Database Connection Pool Configuration (for 20-30 concurrent users)
DB_POOL_SIZE=75
DB_MAX_OVERFLOW=75
DB_POOL_RECYCLE=3600

# API Configuration
API_VERSION=v1

# Server Configuration
FLASK_HOST=0.0.0.0
FLASK_PORT=8099

# Airflow Configuration
# For production with Nginx, use: https://your-server-ip/airflow/
# For development, use: http://localhost:8080
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow

# Azure AI Language (PII Detection) configuration (optional)
AZURE_AI_LANGUAGE_ENDPOINT=
AZURE_AI_LANGUAGE_KEY=
```

**Generate a secure SECRET_KEY:**
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

**Test backend connection:**
```bash
cd backend
source venv/bin/activate
python -c "from database import engine; engine.connect(); print('Database connection successful!')"
```

### Step 5: Airflow Setup

```bash
cd airflow

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Set Airflow home directory
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
```

**Create `airflow/.env` file:**
```bash
cd airflow
nano .env
```

**Airflow `.env` file content:**
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
# For production with Nginx, use: https://your-server-ip/airflow-fe/
# For development, use: http://localhost:5162
FRONTEND_URL=http://localhost:5162
```

**Configure `airflow/airflow.cfg`:**
```bash
cd airflow
nano airflow.cfg
```

**Update these sections in `airflow.cfg`:**
```ini
[core]
; VM / non-Docker: use your MySQL hostname/IP (example: torrodb.mysql.database.azure.com)
; Docker Compose: use the mysql service name: mysql
sql_alchemy_conn = mysql+pymysql://root:your_mysql_password@your-mysql-host:3306/torroforexcel
dags_are_paused_at_creation = False

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080
secret_key = your-secret-key-here-generate-random-string
enable_proxy_fix = True
```

**Generate a secure secret_key for Airflow:**
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

**Verify Airflow configuration:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
airflow version
airflow db check
```

### Step 6: Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Build for production (optional, for production deployment)
npm run build
```

**Create `frontend/.env` file:**
```bash
cd frontend
nano .env
```

**Frontend `.env` file content:**
```env
# Backend API Base URL
# For production with Nginx, use: https://your-server-ip/airflow-be
# For development, use: http://localhost:8099
# Note: Do NOT include /api in the URL - it's added automatically in code
VITE_API_BASE_URL=http://localhost:8099

# Optional: Azure Service Principal Credentials (if hardcoding for bank deployment)
# Note: Vite requires VITE_ prefix for environment variables to be accessible in browser
VITE_AZURE_STORAGE_ACCOUNT_NAME=
VITE_AZURE_CLIENT_ID=
VITE_AZURE_CLIENT_SECRET=
VITE_AZURE_TENANT_ID=
VITE_AZURE_DATALAKE_PATHS=
```

**Important Notes:**
- Each component (backend, airflow, frontend) has its own `.env` file in its respective directory
- There is NO root `.env` file - all configuration is component-specific
- Replace `your_mysql_password` with your actual MySQL root password
- Replace `your-server-ip` with your actual server IP address for production
- Generate secure random strings for `SECRET_KEY` values

### Step 7: Create Logs Directory

```bash
cd /path/to/torrofinalv2release
mkdir -p logs
chmod 755 logs
```

## Running the Application

### Option 1: Using Startup Script (Recommended)

The project includes a `start_services.sh` script that starts all services:

```bash
# Make script executable
chmod +x start_services.sh

# Start all services
./start_services.sh

# Check service status
ps aux | grep -E "python.*main.py|airflow|node|vite"

# View logs
tail -f logs/backend.log
tail -f logs/airflow-webserver.log
tail -f logs/airflow-scheduler.log
tail -f logs/frontend.log
```

**Stop all services:**
```bash
# Kill all services
pkill -f "python.*main.py"
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
sudo dnf install nginx  # RHEL/CentOS
# OR
sudo apt-get install nginx  # Ubuntu/Debian

# Create Nginx configuration directory
sudo mkdir -p /etc/nginx/conf.d
```

**Copy Nginx configuration:**
```bash
# Copy the provided Nginx configuration
sudo cp nginx/torro-reverse-proxy.conf /etc/nginx/conf.d/torro-reverse-proxy.conf

# Or create manually:
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

    # SSL Security Settings
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # Frontend
    location /airflow-fe/ {
        proxy_pass http://127.0.0.1:5162/airflow-fe/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 3600s;
    }

    # Root redirect to frontend
    location = / {
        return 301 /airflow-fe/;
    }

    # Backend API
    location /airflow-be/api/ {
        rewrite ^/airflow-be/api(/.*)$ /api$1 break;
        proxy_pass http://127.0.0.1:8099;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 3600s;
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
        proxy_redirect http://127.0.0.1:8080/ /airflow/;
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

**Generate SSL Certificates (Self-Signed for Testing):**
```bash
sudo mkdir -p /etc/nginx/ssl
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/ssl/key.pem \
    -out /etc/nginx/ssl/cert.pem \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"
```

**For Production, use Let's Encrypt:**
```bash
sudo dnf install certbot python3-certbot-nginx  # RHEL/CentOS
# OR
sudo apt-get install certbot python3-certbot-nginx  # Ubuntu/Debian

sudo certbot --nginx -d your-domain.com
```

**Enable SELinux for Nginx (if using SELinux):**
```bash
sudo setsebool -P httpd_can_network_connect 1
```

**Test and Start Nginx:**
```bash
sudo nginx -t  # Test configuration
sudo systemctl start nginx
sudo systemctl enable nginx
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

**Update Environment Variables for Production:**

Update `backend/.env`:
```env
AIRFLOW_BASE_URL=https://your-server-ip/airflow/
```

Update `airflow/.env`:
```env
FRONTEND_URL=https://your-server-ip/airflow-fe/
```

Update `airflow/airflow.cfg`:
```ini
[webserver]
base_url = https://your-server-ip/airflow
```

Update `frontend/.env`:
```env
VITE_API_BASE_URL=https://your-server-ip/airflow-be
```

**Access Services via Nginx:**
- Frontend: `https://your-server-ip/airflow-fe/`
- Backend API: `https://your-server-ip/airflow-be/api/health`
- Airflow UI: `https://your-server-ip/airflow/`

## Azure Configuration

### Authentication Methods

The platform supports two authentication methods:

#### 1. Connection String Authentication

Use your Azure Storage Account connection string:
```
DefaultEndpointsProtocol=https;AccountName=<account-name>;AccountKey=<account-key>;EndpointSuffix=core.windows.net
```

**How to get connection string:**
1. Go to Azure Portal → Storage Account
2. Click "Access keys"
3. Copy "Connection string" from key1 or key2

#### 2. Service Principal Authentication

Create a Service Principal in Azure AD and assign required RBAC roles.

**Create Service Principal (Azure CLI):**
```bash
az ad sp create-for-rbac --name "torro-data-discovery" --role "Storage Blob Data Reader" --scopes /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account-name>
```

**Or via Azure Portal:**
1. Go to Azure Portal → Azure Active Directory → App registrations
2. Click "New registration"
3. Name: "torro-data-discovery"
4. Click "Register"
5. Note the Application (client) ID and Directory (tenant) ID
6. Go to "Certificates & secrets" → Create a new client secret
7. Note the client secret value

**Assign RBAC Roles:**
1. Go to Azure Portal → Storage Account
2. Click "Access Control (IAM)"
3. Click "Add role assignment"
4. Assign these roles to your Service Principal:
   - **Storage Blob Data Reader** (for Blob Storage and Data Lake Gen2)
   - **Storage File Data SMB Share Reader** (for File Shares)
   - **Storage Queue Data Reader** (for Queues)
   - **Storage Table Data Reader** (for Tables)

**Service Principal Credentials:**
- Client ID (Application ID)
- Client Secret
- Tenant ID (Directory ID)
- Storage Account Name

### Required Azure Permissions

For Service Principal authentication, the Service Principal must have these RBAC roles assigned at the Storage Account level:
- **Storage Blob Data Reader** (read-only access to blobs and Data Lake Gen2)
- **Storage Queue Data Reader** (for queue discovery)
- **Storage Table Data Reader** (for table discovery)
- **Storage File Data SMB Share Reader** (for file share discovery)

For Connection String authentication, the connection string must have read permissions for all storage services.

### Supported Azure Storage Services

The platform discovers assets from:

1. **Azure Blob Storage**: All containers and all blobs/files in containers
2. **Azure Data Lake Gen2**: All filesystems (containers) and all files/directories (supports ABFS paths)
3. **Azure File Shares**: All file shares and all files in shares
4. **Azure Queues**: All queues in storage account
5. **Azure Tables**: All tables in storage account

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
- **Smart Deduplication**: Path normalization and hash-based checks to skip unchanged files
- **Complete Metadata**: All files get full metadata extraction (properties, samples, schema)
- **Connection Pooling**: Database connection pool (75 base + 75 overflow) for 20-30 concurrent users

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

## Complete Deployment Checklist

### System Setup
- [ ] Python 3.8+ installed
- [ ] Node.js 20.19+ installed
- [ ] MySQL 8.0+ installed and running
- [ ] MySQL `max_connections` set to 250 in `/etc/my.cnf`
- [ ] All system dependencies installed (gcc, make, etc.)
- [ ] At least 20GB free disk space

### Database Setup
- [ ] MySQL service started
- [ ] Database `torroforexcel` created
- [ ] Schema loaded (`database/schema.sql`)
- [ ] Lineage schema loaded (`database/lineage_schema.sql`)
- [ ] Database user has proper permissions
- [ ] MySQL root password set and known

### Backend Setup
- [ ] Virtual environment created (`backend/venv`)
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Gunicorn installed for production
- [ ] `backend/.env` file created with correct values:
  - [ ] Database credentials (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
  - [ ] Database pool settings (DB_POOL_SIZE=75, DB_MAX_OVERFLOW=75)
  - [ ] SECRET_KEY generated and set
  - [ ] AIRFLOW_BASE_URL configured
- [ ] Backend can connect to MySQL (test connection)
- [ ] Backend starts without errors
- [ ] Logs directory exists and is writable

### Airflow Setup
- [ ] Virtual environment created (`airflow/venv`)
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Airflow database initialized (`airflow db init`)
- [ ] Airflow admin user created (username: airflow, password: airflow)
- [ ] `airflow/.env` file created with correct values:
  - [ ] MySQL credentials (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)
  - [ ] FRONTEND_URL configured
- [ ] `airflow/airflow.cfg` configured:
  - [ ] `sql_alchemy_conn` set to MySQL connection string
  - [ ] `base_url` configured
  - [ ] `secret_key` generated and set
  - [ ] `enable_proxy_fix = True` (if using Nginx)
  - [ ] `dags_are_paused_at_creation = False`
- [ ] Airflow webserver starts
- [ ] Airflow scheduler starts
- [ ] DAGs are visible in Airflow UI and not paused
- [ ] No DAG import errors

### Frontend Setup
- [ ] Node.js dependencies installed (`npm install`)
- [ ] `frontend/.env` file created with correct API URL:
  - [ ] `VITE_API_BASE_URL` configured (without /api suffix)
- [ ] Frontend builds without errors (`npm run build`)
- [ ] Frontend dev server starts (`npm run dev`)

### Azure Configuration
- [ ] Service Principal created (if using SP auth)
- [ ] RBAC roles assigned to Service Principal:
  - [ ] Storage Blob Data Reader
  - [ ] Storage Queue Data Reader
  - [ ] Storage Table Data Reader
  - [ ] Storage File Data SMB Share Reader
- [ ] OR Connection String obtained (if using connection string auth)
- [ ] Test connection successful via frontend or API

### Production Deployment (if using Nginx)
- [ ] Nginx installed
- [ ] SSL certificates generated/obtained
- [ ] Nginx configuration created (`/etc/nginx/conf.d/torro-reverse-proxy.conf`)
- [ ] Nginx configuration tested (`sudo nginx -t`)
- [ ] SELinux configured (if applicable): `sudo setsebool -P httpd_can_network_connect 1`
- [ ] Firewall rules configured (ports 80, 443)
- [ ] All environment variables updated with production URLs (HTTPS)
- [ ] All services accessible via Nginx
- [ ] HTTP redirects to HTTPS

### Service Management
- [ ] `start_services.sh` script works
- [ ] All services start successfully
- [ ] Logs directory exists and is writable (`logs/`)
- [ ] Services can be stopped and restarted
- [ ] Services restart after reboot (if using systemd)

### Testing
- [ ] Frontend accessible (direct or via Nginx)
- [ ] Backend health check returns 200 (`GET /api/health`)
- [ ] Airflow UI accessible (direct or via Nginx)
- [ ] Can create connection (Connection String or Service Principal)
- [ ] Can test connection (test button works)
- [ ] Can discover assets (immediate discovery)
- [ ] Refresh button works (discovers new files)
- [ ] Airflow DAGs run successfully (scheduled discovery)
- [ ] Assets appear in "Discovered Assets" page
- [ ] Data lineage page loads
- [ ] No duplicate assets on refresh

## Troubleshooting

### Backend Not Starting

**Check logs:**
```bash
tail -f logs/backend.log
```

**Common issues:**
- Database connection error: Verify `backend/.env` has correct DB credentials
- Port 8099 already in use: `lsof -i :8099` or `netstat -tulnp | grep 8099`
- Missing dependencies: `cd backend && source venv/bin/activate && pip install -r requirements.txt`

**Test database connection:**
```bash
cd backend
source venv/bin/activate
python -c "from database import engine; engine.connect(); print('OK')"
```

### Airflow DAGs Not Running

**Check logs:**
```bash
tail -f logs/airflow-scheduler.log
tail -f logs/airflow-webserver.log
```

**Common issues:**
- DAGs are paused: Check Airflow UI → DAGs → Unpause
- DAG import errors: Check Airflow UI → DAGs → Import Errors
- Missing dependencies: `cd airflow && source venv/bin/activate && pip install -r requirements.txt`
- Database connection error: Verify `airflow/airflow.cfg` has correct MySQL connection string

**Verify DAGs:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
airflow dags list
airflow dags list-import-errors
```

### Connection Test Fails

**Service Principal authentication:**
- Verify Service Principal credentials (Client ID, Client Secret, Tenant ID)
- Check Azure RBAC role assignment (Storage Blob Data Reader, etc.)
- Verify roles are assigned at Storage Account level (not subscription level)
- Wait 1-2 minutes after role assignment for permissions to propagate
- Check network connectivity to Azure: `ping <storage-account>.blob.core.windows.net`

**Connection String authentication:**
- Verify connection string format: `DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net`
- Check connection string is from correct storage account
- Verify storage account is accessible from VM

### Discovery Slow or Failing

**Check network:**
```bash
ping <storage-account>.blob.core.windows.net
curl -I https://<storage-account>.blob.core.windows.net
```

**Check database connection pool:**
- Verify `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` in `backend/.env`
- Check MySQL `max_connections` is set to 250+
- Monitor database connections: `mysql -u root -p -e "SHOW PROCESSLIST;"`

**Check logs:**
```bash
tail -f logs/backend.log | grep -i error
tail -f logs/airflow-scheduler.log | grep -i error
```

### Refresh Button Not Working

**Check browser console:**
- Open browser Developer Tools (F12)
- Check Console tab for JavaScript errors
- Check Network tab for failed API calls

**Verify backend API:**
```bash
curl http://localhost:8099/api/health
curl http://localhost:8099/api/connections
```

**Check backend logs:**
```bash
tail -f logs/backend.log
```

### Duplicate Assets on Refresh

**This should be fixed, but if it occurs:**
- Check deduplication logic is working
- Verify `normalize_path` function is being used
- Check database for duplicate asset IDs
- Clear database and re-discover: `python clear_db.py`

### Nginx 502 Bad Gateway

**Check SELinux:**
```bash
sudo setsebool -P httpd_can_network_connect 1
```

**Check Nginx error logs:**
```bash
sudo tail -f /var/log/nginx/error.log
```

**Verify services are running:**
```bash
ps aux | grep -E "python.*main.py|airflow|node"
netstat -tulnp | grep -E "8099|8080|5162"
```

**Test services directly:**
```bash
curl http://127.0.0.1:8099/api/health
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:5162
```

### Database Connection Pool Exhausted

**Symptoms:**
- `QueuePool limit of size 75 overflow 75 reached`
- 500 Internal Server Error on API calls

**Fix:**
1. Kill stuck MySQL connections:
```bash
mysql -u root -p -e "SHOW PROCESSLIST;"
mysql -u root -p -e "KILL <process-id>;"
```

2. Increase MySQL `max_connections`:
```bash
sudo nano /etc/my.cnf
# Add: max_connections = 250
sudo systemctl restart mysqld
```

3. Restart backend:
```bash
pkill -f "python.*main.py"
cd backend && source venv/bin/activate && python main.py &
```

## File Structure

```
torrofinalv2release/
├── backend/              # Flask backend
│   ├── main.py          # Main application
│   ├── models.py        # Database models
│   ├── database.py      # Database connection
│   ├── config.py        # Configuration
│   ├── utils/           # Utility modules
│   ├── requirements.txt # Python dependencies
│   ├── venv/           # Python virtual environment
│   └── .env            # Backend environment variables
├── airflow/             # Airflow DAGs
│   ├── dags/           # DAG definitions
│   │   ├── azure_blob_discovery_dag.py
│   │   └── sql_lineage_extraction_dag.py
│   ├── utils/          # Utility modules
│   ├── requirements.txt # Python dependencies
│   ├── airflow.cfg     # Airflow configuration
│   ├── venv/           # Python virtual environment
│   └── .env            # Airflow environment variables
├── frontend/           # React frontend
│   ├── src/           # Source code
│   ├── public/        # Public assets
│   ├── package.json   # Node.js dependencies
│   ├── vite.config.ts # Vite configuration
│   └── .env           # Frontend environment variables
├── database/          # Database schemas
│   ├── schema.sql
│   └── lineage_schema.sql
├── nginx/            # Nginx configuration
│   └── torro-reverse-proxy.conf
├── docker/           # Docker configuration (optional)
├── logs/            # Application logs
├── start_services.sh # Service startup script
├── clear_db.py      # Database clearing script
└── README.md        # This file
```

## Security Notes

- Service Principal credentials can be hardcoded in frontend (bank requirement) OR use Connection String
- All Azure operations are read-only (Storage Blob Data Reader, Queue Data Reader, Table Data Reader, File Data Reader)
- Database credentials stored in environment variables (`.env` files)
- Connection strings stored securely in database (encrypted at rest)
- All API calls use HTTPS for Azure operations
- SECRET_KEY values should be strong random strings (use `secrets.token_hex(32)`)
- `.env` files should NOT be committed to version control (add to `.gitignore`)

## Discovery Methods

### Immediate Discovery (Refresh Button)

Click the "Refresh" button in the UI to:
- Discover all new files immediately
- Update existing assets if schema changed
- Show results in real-time
- Uses synchronous API endpoint: `POST /api/connections/<id>/discover`

### Scheduled Discovery (Airflow DAG)

The Airflow DAG runs automatically on schedule:
- Discovers new assets in the background
- Updates the database when complete
- Can be triggered manually via API: `POST /api/discovery/trigger`
- Schedule configured in `airflow/.env`: `AIRFLOW_DAG_SCHEDULE=0 * * * *` (hourly)

## Monitoring

### Logs

- Backend: `logs/backend.log`
- Airflow Webserver: `logs/airflow-webserver.log`
- Airflow Scheduler: `logs/airflow-scheduler.log`
- Frontend: `logs/frontend.log`
- Nginx: `/var/log/nginx/access.log` and `/var/log/nginx/error.log`

### Health Checks

- Backend: `GET http://localhost:8099/api/health` or `GET https://your-server-ip/airflow-be/api/health`
- Airflow: `GET http://localhost:8080/health` or `GET https://your-server-ip/airflow/health`
- Nginx: `GET https://your-server-ip/health`

### Service Status

```bash
# Check all services
ps aux | grep -E "python.*main.py|airflow|node|vite"

# Check ports
netstat -tulnp | grep -E "8099|8080|5162|443|80"

# Check MySQL
sudo systemctl status mysqld

# Check Nginx
sudo systemctl status nginx
```

## Additional Resources

- **Azure Storage Documentation**: https://docs.microsoft.com/en-us/azure/storage/
- **Apache Airflow Documentation**: https://airflow.apache.org/docs/
- **Flask Documentation**: https://flask.palletsprojects.com/
- **React Documentation**: https://react.dev/

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review logs in `logs/` directory
3. Check Airflow UI for DAG import errors
4. Verify all environment variables are set correctly
5. Ensure all prerequisites are installed

---

**Last Updated**: December 2024
**Version**: 2.0
