#!/bin/bash
# Restart script for Torro services (Frontend, Backend, Gunicorn only)
# Safely stops services using PID files, then starts them

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Restarting Frontend, Backend, and Gunicorn"
echo "=========================================="
echo ""

# Stop services first
bash "${PROJECT_ROOT}/stop_services.sh"

# Wait a moment for ports to be released
sleep 2

# Start services
echo ""
echo "Starting services..."
echo ""

# Start Backend with Gunicorn
echo "Starting Backend (Gunicorn)..."
cd "${PROJECT_ROOT}/backend"
source venv/bin/activate
nohup gunicorn -c gunicorn_config.py main:app > "${PROJECT_ROOT}/logs/gunicorn.log" 2>&1 &
GUNICORN_PID=$!
echo $GUNICORN_PID > "${PROJECT_ROOT}/logs/gunicorn.pid"
echo "Gunicorn started with PID: $GUNICORN_PID"
cd "${PROJECT_ROOT}"
sleep 3

# Start Frontend
if [ -d "${PROJECT_ROOT}/frontend/node_modules" ]; then
    echo "Starting Frontend..."
    cd "${PROJECT_ROOT}/frontend"
    nohup npm run dev > "${PROJECT_ROOT}/logs/frontend.log" 2>&1 &
    FRONTEND_PID=$!
    echo $FRONTEND_PID > "${PROJECT_ROOT}/logs/frontend.pid"
    echo "Frontend started with PID: $FRONTEND_PID"
    cd "${PROJECT_ROOT}"
    sleep 3
else
    echo "Frontend node_modules not found, skipping..."
fi

HOST_IP=$(hostname -I | awk '{print $1}')
echo ""
echo "=========================================="
echo "Services restarted successfully!"
echo "=========================================="
echo "Backend:  http://${HOST_IP}:8099"
echo "Frontend: http://${HOST_IP}:5162"
echo ""
echo "Logs:"
echo "  - Gunicorn: ${PROJECT_ROOT}/logs/gunicorn.log"
echo "  - Frontend: ${PROJECT_ROOT}/logs/frontend.log"
echo ""

