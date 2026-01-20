"""
Health check endpoints for monitoring and diagnostics.
Production-level health monitoring routes.
"""

from flask import Blueprint, jsonify
from database import engine, SessionLocal
from sqlalchemy import text
from utils.helpers import handle_error
import os
import logging

logger = logging.getLogger(__name__)

health_bp = Blueprint('health', __name__)

env = os.getenv("FLASK_ENV", "development")


@health_bp.route('/health', methods=['GET'])
def health_simple():
    """Simple health check endpoint"""
    return "healthy\n", 200


@health_bp.route('/api/health', methods=['GET'])
def health():
    """Detailed health check endpoint"""
    return jsonify({
        "status": "ok",
        "message": "Backend is running",
        "environment": env
    })


@health_bp.route('/api/health/db-pool', methods=['GET'])
@handle_error
def health_db_pool():
    """Monitor database connection pool status"""
    try:
        pool = engine.pool
        db = SessionLocal()
        try:
            db.execute(text("SELECT 1"))
            
            result = db.execute(text("SHOW STATUS LIKE 'Threads_connected'"))
            threads_connected = result.fetchone()[1] if result else "unknown"
            
            result = db.execute(text("SHOW VARIABLES LIKE 'max_connections'"))
            max_connections = result.fetchone()[1] if result else "unknown"
            
            result = db.execute(text("SHOW STATUS LIKE 'Max_used_connections'"))
            max_used = result.fetchone()[1] if result else "unknown"
            
            return jsonify({
                "status": "ok",
                "pool": {
                    "size": pool.size(),
                    "checked_in": pool.checkedin(),
                    "checked_out": pool.checkedout(),
                    "overflow": pool.overflow()
                },
                "mysql": {
                    "active_connections": int(threads_connected),
                    "max_connections": int(max_connections),
                    "max_used_ever": int(max_used) if max_used != "unknown" else 0,
                    "usage_percent": round((int(threads_connected) / int(max_connections)) * 100, 2) if max_connections != "unknown" else 0
                }
            }), 200
        finally:
            db.close()
    except Exception as e:
        logger.error(f'FN:health_db_pool error:{str(e)}')
        return jsonify({"status": "error", "error": str(e)}), 500

