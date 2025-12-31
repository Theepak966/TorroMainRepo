#!/bin/bash
# Safe stop script for Torro services
# Uses PID files and graceful shutdown (SIGTERM) before SIGKILL

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
cd "$PROJECT_ROOT"

LOGS_DIR="${PROJECT_ROOT}/logs"
mkdir -p "$LOGS_DIR"

echo "Stopping services gracefully..."

# Stop Frontend (if PID file exists)
if [ -f "${LOGS_DIR}/frontend.pid" ]; then
    FRONTEND_PID=$(cat "${LOGS_DIR}/frontend.pid" 2>/dev/null)
    if [ ! -z "$FRONTEND_PID" ] && kill -0 "$FRONTEND_PID" 2>/dev/null; then
        echo "Stopping Frontend (PID: $FRONTEND_PID)..."
        kill -TERM "$FRONTEND_PID" 2>/dev/null
        sleep 2
        # Force kill if still running
        if kill -0 "$FRONTEND_PID" 2>/dev/null; then
            kill -9 "$FRONTEND_PID" 2>/dev/null
        fi
        rm -f "${LOGS_DIR}/frontend.pid"
    else
        echo "Frontend PID file exists but process not running, cleaning up..."
        rm -f "${LOGS_DIR}/frontend.pid"
    fi
fi

# Stop Gunicorn (if PID file exists)
if [ -f "${LOGS_DIR}/gunicorn.pid" ]; then
    GUNICORN_PID=$(cat "${LOGS_DIR}/gunicorn.pid" 2>/dev/null)
    if [ ! -z "$GUNICORN_PID" ] && kill -0 "$GUNICORN_PID" 2>/dev/null; then
        echo "Stopping Gunicorn (PID: $GUNICORN_PID)..."
        kill -TERM "$GUNICORN_PID" 2>/dev/null
        sleep 3
        # Force kill if still running
        if kill -0 "$GUNICORN_PID" 2>/dev/null; then
            kill -9 "$GUNICORN_PID" 2>/dev/null
        fi
        rm -f "${LOGS_DIR}/gunicorn.pid"
    else
        echo "Gunicorn PID file exists but process not running, cleaning up..."
        rm -f "${LOGS_DIR}/gunicorn.pid"
    fi
fi

# Stop Backend (if PID file exists)
if [ -f "${LOGS_DIR}/backend.pid" ]; then
    BACKEND_PID=$(cat "${LOGS_DIR}/backend.pid" 2>/dev/null)
    if [ ! -z "$BACKEND_PID" ] && kill -0 "$BACKEND_PID" 2>/dev/null; then
        echo "Stopping Backend (PID: $BACKEND_PID)..."
        kill -TERM "$BACKEND_PID" 2>/dev/null
        sleep 2
        # Force kill if still running
        if kill -0 "$BACKEND_PID" 2>/dev/null; then
            kill -9 "$BACKEND_PID" 2>/dev/null
        fi
        rm -f "${LOGS_DIR}/backend.pid"
    else
        echo "Backend PID file exists but process not running, cleaning up..."
        rm -f "${LOGS_DIR}/backend.pid"
    fi
fi

# Also kill any remaining processes by port (safer than killall)
echo "Checking for processes on service ports..."

# Frontend port 5162
FRONTEND_PORT_PID=$(lsof -ti:5162 2>/dev/null)
if [ ! -z "$FRONTEND_PORT_PID" ]; then
    echo "Killing process on port 5162 (PID: $FRONTEND_PORT_PID)..."
    kill -TERM "$FRONTEND_PORT_PID" 2>/dev/null
    sleep 1
    if kill -0 "$FRONTEND_PORT_PID" 2>/dev/null; then
        kill -9 "$FRONTEND_PORT_PID" 2>/dev/null
    fi
fi

# Backend port 8099
BACKEND_PORT_PID=$(lsof -ti:8099 2>/dev/null)
if [ ! -z "$BACKEND_PORT_PID" ]; then
    echo "Killing process on port 8099 (PID: $BACKEND_PORT_PID)..."
    kill -TERM "$BACKEND_PORT_PID" 2>/dev/null
    sleep 1
    if kill -0 "$BACKEND_PORT_PID" 2>/dev/null; then
        kill -9 "$BACKEND_PORT_PID" 2>/dev/null
    fi
fi

echo "Services stopped."
echo ""

