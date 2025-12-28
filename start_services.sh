#!/bin/bash
# Startup script for Torro services
# Project is now located at /mnt/torro/torrofinalv2release

cd /mnt/torro/torrofinalv2release

# Start MySQL if not running
if ! systemctl is-active --quiet mysqld; then
    echo "Starting MySQL..."
    sudo systemctl start mysqld
    sleep 2
fi

# Start Backend
echo "Starting Backend..."
cd backend
source venv/bin/activate
nohup python main.py > ../logs/backend.log 2>&1 &
echo $! > ../logs/backend.pid
cd ..
sleep 2

# Start Airflow Webserver
echo "Starting Airflow Webserver..."
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
nohup airflow webserver --hostname 0.0.0.0 --port 8080 > ../logs/airflow-webserver.log 2>&1 &
echo $! > ../logs/airflow-webserver.pid
cd ..
sleep 3

# Start Airflow Scheduler
echo "Starting Airflow Scheduler..."
cd airflow
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
nohup airflow scheduler > ../logs/airflow-scheduler.log 2>&1 &
echo $! > ../logs/airflow-scheduler.pid
cd ..
sleep 2

# Start Frontend
if [ -d "frontend/node_modules" ]; then
    echo "Starting Frontend..."
    cd frontend
    nohup npm run dev > ../logs/frontend.log 2>&1 &
    echo $! > ../logs/frontend.pid
    cd ..
    sleep 3
fi

HOST_IP=$(hostname -I | awk '{print $1}')
echo ""
echo "=========================================="
echo "Services started. Check status:"
echo "=========================================="
echo "Backend:  http://${HOST_IP}:8099"
echo "Airflow:  http://${HOST_IP}:8080 (user: airflow, pass: airflow)"
echo "Frontend: http://${HOST_IP}:5162"
echo ""
echo "Logs are in: /mnt/torro/torrofinalv2release/logs/"
echo "PIDs are in: /mnt/torro/torrofinalv2release/logs/*.pid"
echo ""

