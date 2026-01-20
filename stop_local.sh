#!/bin/bash
# Stop local Torro services

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
cd "$PROJECT_ROOT"

echo "Stopping services..."

# Stop Backend
if [ -f "logs/backend.pid" ]; then
    PID=$(cat logs/backend.pid)
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        echo "Stopped Backend (PID: $PID)"
    fi
    rm -f logs/backend.pid
fi

# Stop Frontend
if [ -f "logs/frontend.pid" ]; then
    PID=$(cat logs/frontend.pid)
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        echo "Stopped Frontend (PID: $PID)"
    fi
    rm -f logs/frontend.pid
fi

# Stop Airflow services if running
if [ -f "logs/airflow-webserver.pid" ]; then
    PID=$(cat logs/airflow-webserver.pid)
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        echo "Stopped Airflow Webserver (PID: $PID)"
    fi
    rm -f logs/airflow-webserver.pid
fi

if [ -f "logs/airflow-scheduler.pid" ]; then
    PID=$(cat logs/airflow-scheduler.pid)
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        echo "Stopped Airflow Scheduler (PID: $PID)"
    fi
    rm -f logs/airflow-scheduler.pid
fi

echo "All services stopped."
