"""
Metadata routes.
Production-level route handlers for metadata operations.
"""

import os
import sys
import logging
from flask import Blueprint, request, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from utils.helpers import handle_error
from flask import current_app

logger = logging.getLogger(__name__)

metadata_bp = Blueprint('metadata', __name__)

@metadata_bp.route('/api/metadata-tags', methods=['GET'])
@handle_error
def get_metadata_tags():
    """Get all metadata tags from torro_api.metadata_tags table"""
    # OPTIMIZATION: Use SQLAlchemy session from pool instead of raw pymysql connection
    db = SessionLocal()
    try:
        from sqlalchemy import text
        
        # Get workspace_id from query params if provided
        workspace_id = request.args.get('workspace_id', type=int)
        
        # OPTIMIZATION: Use SQLAlchemy text() for raw SQL with connection pooling
        if workspace_id:
            query = text("""
                SELECT id, workspace_id, tag_identifier, tag_name, description, platform, external_id, properties
                FROM metadata_tags
                WHERE is_deleted = 0 AND workspace_id = :workspace_id
                ORDER BY tag_name ASC
            """)
            result = db.execute(query, {"workspace_id": workspace_id})
        else:
            query = text("""
                SELECT id, workspace_id, tag_identifier, tag_name, description, platform, external_id, properties
                FROM metadata_tags
                WHERE is_deleted = 0
                ORDER BY tag_name ASC
            """)
            result = db.execute(query)
        
        tags = result.fetchall()
        
        # OPTIMIZATION: Use list comprehension instead of loop for better performance
        result_list = [{
            'id': tag[0],
            'workspace_id': tag[1],
            'tag_identifier': tag[2],
            'tag_name': tag[3],
            'description': tag[4],
            'platform': tag[5],
            'external_id': tag[6],
            'properties': tag[7]
        } for tag in tags]
        
        logger.info('FN:get_metadata_tags count:{} workspace_id:{}'.format(len(result_list), workspace_id))
        
        return jsonify({
            "tags": result_list,
            "count": len(result_list)
        }), 200
            
    except Exception as e:
        logger.error('FN:get_metadata_tags error:{}'.format(str(e)), exc_info=True)
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to fetch metadata tags"}), 400
    finally:
        db.close()


