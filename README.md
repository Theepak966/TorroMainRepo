# Torro Data Discovery Platform

A comprehensive data discovery and governance platform for Azure Storage services, featuring automated metadata extraction, PII detection, approval workflows, data lineage tracking, and advanced data governance capabilities.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [API Documentation](#api-documentation)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## 🎯 Overview

Torro Data Discovery Platform is an enterprise-grade solution for discovering, cataloging, and governing data assets across Azure Storage services. It provides automated discovery, PII detection, approval workflows, data quality scoring, and comprehensive metadata management.

### Supported Azure Services

- **Azure Blob Storage** (containers and blobs)
- **Azure Data Lake Gen2** (ABFS paths)
- **Azure File Shares** (file shares and files)
- **Azure Queues** (queue storage)
- **Azure Tables** (table storage)

### Authentication Methods

- Connection String authentication
- Service Principal authentication
- Azure CLI authentication (fallback)

---

## ✨ Features

### 1. **Multi-Select Filtering System**
- Checkbox-based multi-select filters for Type, Catalog, Approval Status, and Application Name
- Real-time filtering with instant results
- Filter state persistence across page navigation

### 2. **Application Name Filtering**
- Filter assets by application name from business metadata
- Extracts unique application names automatically
- Integrated with other filters for combined filtering

### 3. **PII Detection & Masking Logic**
- Automated PII detection using Azure DLP (Data Loss Prevention)
- PII-type-specific masking options:
  - **Email**: Mask domain, Full mask, Partial mask, Show domain only
  - **PhoneNumber**: Show last 4 digits, Full mask, Partial mask
  - **SSN**: Show last 4 digits, Full mask
  - **CreditCard**: Show last 4 digits, Full mask
  - And 20+ more PII types with specific masking options
- Separate masking logic for Analytical Users and Operational Users
- Conditional display: masking columns only appear for PII columns
- Direct save functionality from table without opening dialog

### 4. **Discovery ID Stability**
- Fixed discovery ID consistency across approval/rejection/publishing workflows
- Prevents discovery ID changes after asset operations
- Ensures data integrity and traceability

### 5. **Department Management**
- Department field in business metadata
- 15 predefined departments:
  - Data Engineering, Data Science, Business Intelligence
  - IT Operations, Security & Compliance, Finance
  - Risk Management, Customer Analytics, Product Development
  - Marketing, Sales, Human Resources, Legal, Operations, Other

### 6. **Governance Rejection Reasons**
- Structured rejection system with 11 predefined reasons:
  - Data Quality Issues
  - Data Privacy Violation
  - Compliance Risk
  - Data Classification Mismatch
  - Archive/Duplicate
  - Data Lineage Issues
  - Metadata Incomplete
  - Data Retention Policy Violation
  - Access Control Issues
  - Data Source Not Authorized
  - Others (with custom reason input)
- Automatic tag generation from rejection reasons
- Rejection tags automatically added to assets

### 7. **Business Glossary Tags**
- Tag management system for assets
- Fetch metadata tags from API
- Add/remove tags from asset descriptions
- Searchable tag dialog
- Tags stored in business metadata

### 8. **Data Quality Scoring**
- Automated data quality score calculation
- Performance-optimized with caching
- Only recalculates when schema changes
- Caches quality metrics, issues, and scores
- Improves API response time significantly

### 9. **Data Lineage Tracking**
- Visual data lineage graphs
- SQL query parsing and lineage extraction
- Upstream and downstream relationship tracking
- Interactive lineage visualization with React Flow

### 10. **Approval Workflows**
- Multi-stage approval process
- Approval/rejection with detailed reasons
- Publishing to marketplace
- Approval status tracking
- Workflow history

---

## 🏗️ Architecture

### Technology Stack

**Frontend:**
- React 19.1.1
- Material-UI (MUI) 7.3.4
- React Router 7.9.4
- React Flow 11.11.4 (for lineage visualization)
- Vite 7.1.7 (build tool)

**Backend:**
- Flask 2.2.5
- SQLAlchemy 2.0.23
- Gunicorn 21.2.0
- PyMySQL 1.1.0

**Database:**
- MySQL 8.0+

**Orchestration:**
- Apache Airflow (for scheduled discovery jobs)

**Azure Integration:**
- Azure Storage Blob SDK
- Azure Data Lake Gen2 SDK
- Azure File Share SDK
- Azure Queue SDK
- Azure Tables SDK
- Azure AI Text Analytics (for PII detection)

### Project Structure

```
torrofinalv2release/
├── backend/                 # Flask backend application
│   ├── main.py             # Main Flask app and API endpoints
│   ├── models.py           # SQLAlchemy models
│   ├── database.py         # Database configuration
│   ├── config.py          # Configuration management
│   ├── discovery_runner.py # Discovery orchestration
│   ├── utils/              # Utility modules
│   │   ├── azure_blob_client.py
│   │   ├── azure_dlp_client.py
│   │   ├── metadata_extractor.py
│   │   └── ...
│   ├── requirements.txt   # Python dependencies
│   └── .env               # Backend environment variables
│
├── frontend/               # React frontend application
│   ├── src/
│   │   ├── pages/         # Page components
│   │   │   ├── AssetsPage.jsx
│   │   │   ├── ConnectorsPage.jsx
│   │   │   ├── DataLineagePage.jsx
│   │   │   └── ...
│   │   ├── components/    # Reusable components
│   │   └── config/        # Configuration files
│   ├── package.json       # Node.js dependencies
│   └── .env               # Frontend environment variables
│
├── airflow/                # Apache Airflow DAGs
│   ├── dags/              # Airflow DAG definitions
│   │   ├── azure_blob_discovery_dag.py
│   │   └── sql_lineage_extraction_dag.py
│   ├── utils/             # Airflow utilities
│   └── config/            # Airflow configuration
│
├── database/               # Database schema files
│   └── schema.sql         # MySQL schema definitions
│
├── nginx/                  # Nginx configuration (if used)
├── logs/                   # Application logs
├── scripts/               # Utility scripts
│
├── start_services.sh       # Start all services
├── stop_services.sh        # Stop all services
└── restart_services.sh     # Restart services
```

---

## 📦 Prerequisites

### System Requirements

- **Operating System**: Linux (tested on RHEL 8.10)
- **Python**: 3.8 or higher
- **Node.js**: 18.x or higher
- **npm**: 9.x or higher
- **MySQL**: 8.0 or higher
- **Git**: For cloning the repository

### Azure Requirements

- Azure Storage Account(s) with appropriate access
- Service Principal credentials (optional, for Service Principal auth)
- Azure DLP service access (for PII detection)

### Required Permissions

- **Azure Storage**: Read access to containers/blobs
- **Azure DLP**: Access to Text Analytics API
- **Database**: CREATE, SELECT, INSERT, UPDATE, DELETE permissions

---

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd torrofinalv2release
```

### 2. Database Setup

#### Create MySQL Database

```bash
mysql -u root -p
```

```sql
CREATE DATABASE torroforexcel CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'torro_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON torroforexcel.* TO 'torro_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

#### Initialize Schema

```bash
mysql -u torro_user -p torroforexcel < database/schema.sql
```

### 3. Backend Setup

#### Create Virtual Environment

```bash
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### Configure Environment Variables

Create `backend/.env` file:

```bash
# Database Configuration
DB_HOST=your_mysql_host
DB_PORT=3306
DB_USER=torro_user
DB_PASSWORD=your_password
DB_NAME=torroforexcel

# Flask Configuration
FLASK_ENV=production
FLASK_DEBUG=False

# Azure Configuration (optional - can be set per connection)
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret

# Logging
SQL_ECHO=false
```

### 4. Frontend Setup

#### Install Dependencies

```bash
cd frontend
npm install
```

#### Configure Environment Variables

Create `frontend/.env` file:

```bash
VITE_API_BASE_URL=http://localhost:8099
# Or for production:
# VITE_API_BASE_URL=https://your-domain.com/api
```

### 5. Airflow Setup

#### Create Virtual Environment

```bash
cd airflow
python3 -m venv venv
source venv/bin/activate
```

#### Install Airflow

```bash
pip install apache-airflow==2.8.0
pip install apache-airflow-providers-microsoft-azure
```

#### Initialize Airflow

```bash
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow
```

---

## ⚙️ Configuration

### Backend Configuration

The backend uses a centralized configuration system. Key configuration files:

- `backend/.env` - Environment variables
- `backend/config.py` - Configuration classes
- `backend/database.py` - Database connection settings

### Frontend Configuration

- `frontend/.env` - API base URL and other frontend settings
- `frontend/src/config/api.js` - API endpoint configuration

### Airflow Configuration

- `airflow/airflow.cfg` - Airflow configuration
- `airflow/config/azure_config.py` - Azure connection configuration

### Service Scripts

The project includes service management scripts:

- `start_services.sh` - Start all services (Backend, Frontend, Airflow)
- `stop_services.sh` - Stop all services gracefully
- `restart_services.sh` - Restart Frontend and Backend (for code changes)

---

## 🏃 Running the Application

### Development Mode

#### Option 1: Using Service Scripts (Recommended)

```bash
# Start all services
./start_services.sh

# Stop all services
./stop_services.sh

# Restart services (after code changes)
./restart_services.sh
```

#### Option 2: Manual Start

**Backend:**
```bash
cd backend
source venv/bin/activate
python main.py
# Or with Gunicorn:
gunicorn -c gunicorn_config.py main:app
```

**Frontend:**
```bash
cd frontend
npm run dev
```

**Airflow:**
```bash
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate

# Terminal 1: Webserver
airflow webserver --hostname 0.0.0.0 --port 8080

# Terminal 2: Scheduler
airflow scheduler
```

### Accessing the Application

After starting services, access:

- **Frontend**: http://localhost:5162 (or your server IP)
- **Backend API**: http://localhost:8099
- **Airflow UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)

### Production Deployment

For production, use:

- **Backend**: Gunicorn with multiple workers
- **Frontend**: Build and serve static files via Nginx
- **Database**: Use connection pooling (configured in `database.py`)
- **Reverse Proxy**: Nginx for routing and SSL termination

---

## 📡 API Documentation

### Base URL

```
http://localhost:8099/api
```

### Key Endpoints

#### Assets

- `GET /api/assets` - List all assets (paginated)
- `GET /api/assets?discovery_id=<id>` - Get assets by discovery ID
- `GET /api/assets/<asset_id>` - Get asset details
- `PUT /api/assets/<asset_id>` - Update asset
- `POST /api/assets/<asset_id>/approve` - Approve asset
- `POST /api/assets/<asset_id>/reject` - Reject asset
- `POST /api/assets/<asset_id>/publish` - Publish asset

#### Column PII Management

- `PUT /api/assets/<asset_id>/columns/<column_name>/pii` - Update PII status and masking logic
  ```json
  {
    "pii_detected": true,
    "pii_types": ["Email", "PhoneNumber"],
    "masking_logic_analytical": "mask_domain",
    "masking_logic_operational": "show_full"
  }
  ```

#### Connections

- `GET /api/connections` - List all connections
- `POST /api/connections` - Create new connection
- `GET /api/connections/<connection_id>` - Get connection details
- `POST /api/connections/<connection_id>/discover` - Trigger discovery

#### Discovery

- `GET /api/discovery/<discovery_id>` - Get discovery details
- `POST /api/discovery/trigger` - Trigger discovery process

#### Metadata Tags

- `GET /api/metadata-tags` - Get all metadata tags

### Response Format

All API responses follow this format:

```json
{
  "success": true,
  "data": { ... },
  "message": "Operation successful"
}
```

Error responses:

```json
{
  "error": "Error message",
  "details": { ... }
}
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Database Connection Errors

**Problem**: Cannot connect to MySQL database

**Solutions**:
- Verify database credentials in `backend/.env`
- Check MySQL service is running: `sudo systemctl status mysqld`
- Verify network connectivity to database host
- Check firewall rules allow MySQL port (3306)

#### 2. Backend Not Starting

**Problem**: Backend fails to start or crashes

**Solutions**:
- Check virtual environment is activated: `source backend/venv/bin/activate`
- Verify all dependencies installed: `pip install -r backend/requirements.txt`
- Check logs: `tail -f logs/backend.log` or `tail -f logs/gunicorn.log`
- Verify `.env` file exists and has correct values

#### 3. Frontend Build Errors

**Problem**: Frontend fails to build or run

**Solutions**:
- Clear node_modules and reinstall: `rm -rf node_modules && npm install`
- Check Node.js version: `node --version` (should be 18.x or higher)
- Verify `.env` file exists in `frontend/` directory
- Check for port conflicts (default: 5162)

#### 4. Airflow DAGs Not Running

**Problem**: Discovery DAGs not executing

**Solutions**:
- Check Airflow scheduler is running: `ps aux | grep airflow`
- Verify DAGs are enabled in Airflow UI
- Check Airflow logs: `airflow/logs/scheduler/`
- Verify database connection in Airflow config

#### 5. Azure Authentication Failures

**Problem**: Cannot authenticate to Azure Storage

**Solutions**:
- Verify Service Principal credentials are correct
- Check Azure permissions (Storage Blob Data Reader role)
- Verify tenant ID, client ID, and client secret
- Test connection string authentication as fallback

#### 6. PII Detection Not Working

**Problem**: PII detection returns no results

**Solutions**:
- Verify Azure DLP/Text Analytics API access
- Check Azure credentials in backend configuration
- Verify API endpoint is accessible
- Check logs for DLP API errors

### Log Files

All logs are stored in the `logs/` directory:

- `logs/backend.log` - Backend application logs
- `logs/gunicorn.log` - Gunicorn server logs
- `logs/frontend.log` - Frontend development server logs
- `logs/airflow-webserver.log` - Airflow webserver logs
- `logs/airflow-scheduler.log` - Airflow scheduler logs

### Debug Mode

Enable debug logging:

**Backend:**
```bash
# In backend/.env
FLASK_DEBUG=True
SQL_ECHO=true
```

**Frontend:**
```bash
# In frontend/.env
VITE_LOG_LEVEL=debug
```

---

## 🤝 Contributing

### Development Workflow

1. Create a feature branch from `main`
2. Make your changes
3. Test thoroughly
4. Commit with descriptive messages
5. Push to remote and create pull request

### Code Style

- **Python**: Follow PEP 8 guidelines
- **JavaScript/React**: Use ESLint configuration
- **SQL**: Use consistent naming conventions

### Testing

Before submitting:

- Test all affected features
- Verify API endpoints work correctly
- Check for console errors in browser
- Verify database migrations (if any)

---

## 📝 License

[Add your license information here]

---

## 📞 Support

For issues, questions, or contributions:

- Create an issue in the repository
- Contact the development team
- Check the troubleshooting section above

---

## 🎉 Acknowledgments

Built with:
- React & Material-UI
- Flask & SQLAlchemy
- Apache Airflow
- Azure Storage SDKs
- And many other open-source libraries

---

**Last Updated**: January 2025
**Version**: 2.0

