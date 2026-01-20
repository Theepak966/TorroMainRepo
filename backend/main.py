"""
Minimal main.py - Flask app initialization and blueprint registration.
All route handlers have been moved to routes/ modules.
"""

import sys
import os

try:
    import flask
except ImportError:
    print("=" * 70)
    print("ERROR: Flask is not installed!")
    print("=" * 70)
    sys.exit(1)

from flask import Flask
from flask_cors import CORS
import logging
from logging.handlers import RotatingFileHandler

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import config

# Initialize Flask app
app = Flask(__name__)

# Load configuration
env = os.getenv("FLASK_ENV", "default")
app.config.from_object(config.get(env, config["default"]))

# CORS configuration
if app.config["ALLOWED_ORIGINS"] == ["*"]:
    CORS(app)
else:
    CORS(app, resources={
        r"/api/*": {
            "origins": app.config["ALLOWED_ORIGINS"],
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"]
        }
    })

# Configure logging
logging.basicConfig(
    level=getattr(logging, app.config["LOG_LEVEL"]),
    format='%(asctime)s %(name)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# File logging
if app.config.get("LOG_FILE"):
    file_handler = RotatingFileHandler(
        app.config["LOG_FILE"],
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    ))
    logger.addHandler(file_handler)

# Register blueprints
from routes.health import health_bp
from routes.connections import connections_bp
from routes.assets import assets_bp
from routes.discovery import discovery_bp
from routes.lineage_extraction import lineage_extraction_bp
from routes.lineage_sql import lineage_sql_bp
from routes.lineage_relationships import lineage_relationships_bp
from routes.lineage_routes import lineage_bp
from routes.metadata import metadata_bp

app.register_blueprint(health_bp)
app.register_blueprint(connections_bp)
app.register_blueprint(assets_bp)
app.register_blueprint(discovery_bp)
app.register_blueprint(lineage_extraction_bp)
app.register_blueprint(lineage_sql_bp)
app.register_blueprint(lineage_relationships_bp)
app.register_blueprint(lineage_bp)
app.register_blueprint(metadata_bp)

logger.info('FN:__init__ message:All blueprints registered successfully')

# Error handlers
@app.errorhandler(404)
def not_found(error):
    from flask import jsonify
    return jsonify({"error": "Resource not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error('FN:internal_error error:{}'.format(str(error)), exc_info=True)
    from flask import jsonify
    if app.config.get("DEBUG"):
        return jsonify({"error": str(error)}), 500
    else:
        return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == '__main__':
    host = app.config.get("FLASK_HOST", "0.0.0.0")
    port = app.config.get("FLASK_PORT", 8099)
    debug = app.config.get("DEBUG", False)
    logger.info('FN:__main__ port:{} environment:{} debug:{} message:Starting Flask app'.format(port, env, debug))
    app.run(host=host, port=port, debug=debug)

