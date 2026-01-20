#!/bin/bash
# Local startup script for Torro services
# Uses local MySQL and runs services directly

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
cd "$PROJECT_ROOT"

# Create logs directory if it doesn't exist
mkdir -p logs

# Start Backend
echo "Starting Backend..."
cd backend
source venv/bin/activate
nohup python main.py > ../logs/backend.log 2>&1 &
echo $! > ../logs/backend.pid
cd ..
sleep 3

# Start Frontend
if [ -d "frontend/node_modules" ]; then
    echo "Starting Frontend..."
    cd frontend
    nohup npm run dev > ../logs/frontend.log 2>&1 &
    echo $! > ../logs/frontend.pid
    cd ..
    sleep 3
fi

# Note: Airflow has Python 3.13 compatibility issues
# You may need to use Python 3.11 or 3.12 for Airflow

HOST_IP=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "localhost")
echo ""
echo "=========================================="
echo "Services started. Check status:"
echo "=========================================="
echo "Backend:  http://localhost:8099"
echo "Frontend: http://localhost:5173 (or check logs/frontend.log for port)"
echo ""
echo "Logs are in: ${PROJECT_ROOT}/logs/"
echo "PIDs are in: ${PROJECT_ROOT}/logs/*.pid"
echo ""
echo "To stop services, run: ./stop_local.sh"
echo ""
