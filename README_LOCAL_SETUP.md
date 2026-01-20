# Local Setup Guide

This project has been set up to run locally with MySQL database.

## Database Setup

- **Database Name**: `torroforairflow`
- **Username**: `root`
- **Password**: `Theepakk123`
- **Host**: `localhost`
- **Port**: `3306`

The database schema has been initialized with:
- `database/schema.sql` - Main application schema
- `database/lineage_schema.sql` - Lineage tracking schema

## Services

### Backend
- **Port**: 8099
- **Location**: `backend/`
- **Virtual Environment**: `backend/venv/`
- **Configuration**: `backend/.env`

### Frontend
- **Port**: 5173 (Vite default, check logs if different)
- **Location**: `frontend/`
- **Dependencies**: Installed via `npm install`

### Airflow
- **Status**: Has Python 3.13 compatibility issues
- **Note**: Airflow 2.7.0 is not fully compatible with Python 3.13
- **Workaround**: Use Python 3.11 or 3.12 for Airflow, or wait for Airflow updates

## Starting Services

Run the startup script:
```bash
./start_local.sh
```

This will start:
- Backend service on port 8099
- Frontend service (Vite dev server)

## Stopping Services

Run the stop script:
```bash
./stop_local.sh
```

## Manual Service Management

### Start Backend
```bash
cd backend
source venv/bin/activate
python main.py
```

### Start Frontend
```bash
cd frontend
npm run dev
```

## Logs

All service logs are in the `logs/` directory:
- `logs/backend.log` - Backend application logs
- `logs/frontend.log` - Frontend dev server logs
- `logs/*.pid` - Process ID files for service management

## Access URLs

- **Backend API**: http://localhost:8099
- **Frontend**: http://localhost:5173 (check logs for actual port)
- **Health Check**: http://localhost:8099/health

## Troubleshooting

### Backend not starting
1. Check MySQL is running: `brew services list | grep mysql`
2. Verify database connection: `mysql -u root -pTheepakk123 -e "USE torroforairflow; SHOW TABLES;"`
3. Check backend logs: `tail -f logs/backend.log`

### Frontend not starting
1. Check if node_modules exists: `ls frontend/node_modules`
2. Reinstall dependencies: `cd frontend && npm install`
3. Check frontend logs: `tail -f logs/frontend.log`

### Database Issues
1. Verify MySQL is running
2. Check database exists: `mysql -u root -pTheepakk123 -e "SHOW DATABASES;"`
3. Re-run schema if needed:
   ```bash
   mysql -u root -pTheepakk123 torroforairflow < database/schema.sql
   mysql -u root -pTheepakk123 torroforairflow < database/lineage_schema.sql
   ```
