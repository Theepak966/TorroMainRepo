"""
Lineage relationship routes.
Production-level route handlers for lineage relationships.
"""

import os
import sys
import logging
from datetime import datetime
from flask import Blueprint, request, jsonify

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import Asset, LineageRelationship
from utils.helpers import handle_error
from flask import current_app

logger = logging.getLogger(__name__)


def _source_label_from_connector(connector_id):
    """Derive display label for container/folder nodes (e.g. AWS S3, Azure Blob Storage)."""
    if not connector_id:
        return "Unknown"
    cid = (connector_id or "").strip().lower()
    if cid.startswith("aws_s3"):
        return "AWS S3"
    if cid.startswith("oracle_db"):
        return "Oracle DB"
    if cid.startswith("azure_blob") or cid.startswith("azure_"):
        return "Azure Blob Storage"
    parts = cid.split("_")
    return parts[0].capitalize() if parts else "Unknown"


lineage_relationships_bp = Blueprint('lineage_relationships', __name__)

@lineage_relationships_bp.route('/api/lineage/asset/<asset_id>/dataset-urn', methods=['GET'])
@handle_error
def get_asset_dataset_urn(asset_id):
    """Return the canonical dataset URN for an asset (must match backend lineage ingestion/traversal)."""
    try:
        from services.asset_lineage_integration import AssetLineageIntegration
        lineage_integration = AssetLineageIntegration()
        dataset_urn = lineage_integration.get_dataset_urn_for_asset(asset_id)
        if not dataset_urn:
            return jsonify({"error": "Dataset URN not found for asset"}), 404
        return jsonify({"asset_id": asset_id, "dataset_urn": dataset_urn}), 200
    except Exception as e:
        logger.error(f"Failed to get dataset URN for asset {asset_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 400

@lineage_relationships_bp.route('/api/lineage/relationships', methods=['GET'])
@handle_error
def get_all_lineage_relationships():
    """Get all lineage relationships from the database"""
    try:
        from models import LineageRelationship, Asset
        
        db = SessionLocal()
        try:
            # Get all relationships
            relationships = db.query(LineageRelationship).all()
            
            # Convert to JSON-serializable format
            relationships_data = []
            for rel in relationships:
                relationships_data.append({
                    'id': rel.id,
                    'source_asset_id': rel.source_asset_id,
                    'target_asset_id': rel.target_asset_id,
                    'relationship_type': rel.relationship_type,
                    'source_type': rel.source_type,
                    'target_type': rel.target_type,
                    'column_lineage': rel.column_lineage,
                    'transformation_type': rel.transformation_type,
                    'transformation_description': rel.transformation_description,
                    'sql_query': rel.sql_query,
                    'source_system': rel.source_system,
                    'source_job_id': rel.source_job_id,
                    'source_job_name': rel.source_job_name,
                    'confidence_score': float(rel.confidence_score) if rel.confidence_score else 0.5,
                    'extraction_method': rel.extraction_method,
                    'discovered_at': rel.discovered_at.isoformat() if rel.discovered_at else None,
                    'created_at': rel.created_at.isoformat() if rel.created_at else None
                })
            
            return jsonify({
                'relationships': relationships_data,
                'total': len(relationships_data)
            }), 200
        
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f'FN:get_all_lineage_relationships error:{str(e)}', exc_info=True)
        return jsonify({"error": str(e)}), 500



@lineage_relationships_bp.route('/api/lineage/asset/<asset_id>', methods=['GET'])
@handle_error
def get_asset_lineage(asset_id):
    """
    Get lineage for an asset using the new lineage system.
    Includes folder-based lineage relationships.
    """
    db = SessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            return jsonify({"error": "Asset not found"}), 404
        
        # Get folder-based lineage relationships
        from services.folder_based_lineage import FolderBasedLineageService
        folder_lineage_service = FolderBasedLineageService()
        folder_lineage = folder_lineage_service.get_folder_based_lineage(asset_id)
        
        # Try to get URN from new lineage system
        try:
            from services.asset_lineage_integration import AssetLineageIntegration
            lineage_integration = AssetLineageIntegration()
            dataset_urn = lineage_integration.get_dataset_urn_for_asset(asset_id)
            
            if dataset_urn:
                # Use new lineage system
                from services.lineage_traversal import LineageTraversalService
                # Disable cache to ensure fresh data (manual lineage might have been just created)
                traversal_service = LineageTraversalService(max_depth=3, cache_enabled=False)
                from models_lineage.models_lineage import ColumnLineage
                
                # Get upstream and downstream lineage
                upstream_data = traversal_service.get_upstream_lineage(dataset_urn, depth=3)
                downstream_data = traversal_service.get_downstream_lineage(dataset_urn, depth=3)

                # Preload column lineage for all traversed edges (avoid N+1 queries)
                edge_ids = set()
                for e in upstream_data.get('edges', []) or []:
                    if e.get('id') is not None:
                        edge_ids.add(e.get('id'))
                for e in downstream_data.get('edges', []) or []:
                    if e.get('id') is not None:
                        edge_ids.add(e.get('id'))

                column_lineage_by_edge_id = {}
                if edge_ids:
                    now = datetime.utcnow()
                    rows = db.query(ColumnLineage).filter(
                        ColumnLineage.edge_id.in_(list(edge_ids)),
                        (ColumnLineage.valid_to.is_(None) | (ColumnLineage.valid_to >= now))
                    ).all()
                    for r in rows:
                        column_lineage_by_edge_id.setdefault(r.edge_id, []).append({
                            "source_column": r.source_column,
                            "target_column": r.target_column,
                            "transformation_type": r.transformation_type,
                            "transformation_expression": r.transformation_expression,
                            "source_table": r.source_table,
                            "target_table": r.target_table,
                        })
                
                # Build nodes and edges from new lineage system
                nodes = [{
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog,
                    "is_selected": True
                }]
                edges = []
                node_ids = {asset.id}
                
                # Map dataset URNs back to asset IDs
                asset_urn_map = {}  # Map URN -> asset_id
                all_dataset_urns = set()
                
                # Collect all dataset URNs from upstream/downstream
                for dataset in upstream_data.get('datasets', []):
                    all_dataset_urns.add(dataset.get('urn'))
                for dataset in downstream_data.get('datasets', []):
                    all_dataset_urns.add(dataset.get('urn'))
                
                # Query assets by matching URNs (improved - match by catalog, schema, and name)
                if all_dataset_urns:
                    # Extract names from URNs and find matching assets
                    for urn in all_dataset_urns:
                        # URN format: urn:dataset:source_type:catalog.schema.name or urn:dataset:source_type:catalog.name
                        parts = urn.split(':')
                        if len(parts) >= 4:
                            qualified_name = parts[-1]  # catalog.schema.name or catalog.name
                            name_parts = qualified_name.split('.')
                            
                            # Handle different URN formats
                            if len(name_parts) >= 3:
                                # Format: catalog.schema.name
                                # NOTE: asset names (esp. files) can contain '.' (e.g. parquet), so join the remainder.
                                catalog_name = name_parts[0]
                                schema_name = name_parts[1]
                                asset_name = '.'.join(name_parts[2:])
                                
                                # Try to find by catalog, schema, and name
                                matching_asset = db.query(Asset).filter(
                                    Asset.name == asset_name,
                                    Asset.catalog == catalog_name
                                ).first()
                                
                                # If not found, try without schema (for Azure Blob Storage)
                                if not matching_asset:
                                    matching_asset = db.query(Asset).filter(
                                        Asset.name == asset_name,
                                        Asset.catalog == catalog_name
                                    ).first()
                            elif len(name_parts) == 2:
                                # Format: catalog.name (Azure Blob Storage, no schema)
                                # NOTE: asset names can contain '.' so join the remainder.
                                catalog_name = name_parts[0]
                                asset_name = '.'.join(name_parts[1:])
                                
                                matching_asset = db.query(Asset).filter(
                                    Asset.name == asset_name,
                                    Asset.catalog == catalog_name
                                ).first()
                            else:
                                # Fallback: just match by name
                                asset_name = name_parts[-1] if name_parts else None
                                matching_asset = db.query(Asset).filter(Asset.name == asset_name).first()
                            
                            if matching_asset:
                                asset_urn_map[urn] = matching_asset.id
                
                # Build nodes and edges from upstream
                for edge_data in upstream_data.get('edges', []):
                    source_urn = edge_data.get('source_urn')
                    target_urn = edge_data.get('target_urn')
                    edge_id = edge_data.get('id')
                    source_asset_id = asset_urn_map.get(source_urn)
                    target_asset_id = asset_urn_map.get(target_urn) or asset_id
                    
                    if source_asset_id and source_asset_id not in node_ids:
                        source_asset = db.query(Asset).filter(Asset.id == source_asset_id).first()
                        if source_asset:
                            nodes.append({
                                "id": source_asset.id,
                                "name": source_asset.name,
                                "type": source_asset.type,
                                "catalog": source_asset.catalog,
                                "is_selected": False
                            })
                            node_ids.add(source_asset.id)
                    
                    if source_asset_id and target_asset_id:
                        edges.append({
                            "id": f"{source_asset_id}-{target_asset_id}",
                            "source": source_asset_id,
                            "target": target_asset_id,
                            "type": edge_data.get('relationship_type', 'transformation'),
                            "column_lineage": column_lineage_by_edge_id.get(edge_id, []),
                            "confidence_score": 1.0
                        })
                
                # Build nodes and edges from downstream
                for edge_data in downstream_data.get('edges', []):
                    source_urn = edge_data.get('source_urn')
                    target_urn = edge_data.get('target_urn')
                    edge_id = edge_data.get('id')
                    source_asset_id = asset_urn_map.get(source_urn) or asset_id
                    target_asset_id = asset_urn_map.get(target_urn)
                    
                    if target_asset_id and target_asset_id not in node_ids:
                        target_asset = db.query(Asset).filter(Asset.id == target_asset_id).first()
                        if target_asset:
                            nodes.append({
                                "id": target_asset.id,
                                "name": target_asset.name,
                                "type": target_asset.type,
                                "catalog": target_asset.catalog,
                                "is_selected": False
                            })
                            node_ids.add(target_asset.id)
                    
                    if source_asset_id and target_asset_id:
                        edges.append({
                            "id": f"{source_asset_id}-{target_asset_id}",
                            "source": source_asset_id,
                            "target": target_asset_id,
                            "type": edge_data.get('relationship_type', 'transformation'),
                            "column_lineage": column_lineage_by_edge_id.get(edge_id, []),
                            "confidence_score": 1.0
                        })
                
                upstream_count = upstream_data.get('total_datasets', 0)
                downstream_count = downstream_data.get('total_datasets', 0)
                
                # Build hierarchical folder structure nodes (include source_system so UI shows S3 Bucket / Azure Container)
                hierarchy_nodes = []
                hierarchy_edges = []
                connector_id = getattr(asset, "connector_id", None) or ""
                source_system = "Unknown"
                if connector_id.startswith("aws_s3_"):
                    source_system = "AWS S3"
                elif connector_id.startswith("azure_blob_") or connector_id.startswith("azure_"):
                    source_system = "Azure Blob Storage"
                elif connector_id.startswith("oracle_db_"):
                    source_system = "Oracle DB"
                elif connector_id:
                    source_system = (connector_id.split("_")[0] or "").capitalize() or "Unknown"
                
                if folder_lineage and 'error' not in folder_lineage:
                    hierarchy = folder_lineage.get('hierarchy', {})
                    container_name = hierarchy.get('container', 'unknown')
                    path_parts = hierarchy.get('path_parts', [])
                    
                    # Create container node
                    container_node_id = f"container_{container_name}"
                    if container_name and container_name != 'unknown':
                        hierarchy_nodes.append({
                            "id": container_node_id,
                            "name": container_name,
                            "type": "container",
                            "catalog": None,
                            "is_selected": False,
                            "node_type": "container",
                            "source_system": source_system,
                            "connector_id": connector_id or None,
                        })
                        node_ids.add(container_node_id)
                    
                    # Create folder hierarchy nodes
                    current_path = ""
                    parent_node_id = container_node_id if container_name and container_name != 'unknown' else None
                    
                    for i, folder_part in enumerate(path_parts):
                        current_path = f"{current_path}/{folder_part}" if current_path else folder_part
                        folder_node_id = f"folder_{current_path.replace('/', '_')}"
                        
                        hierarchy_nodes.append({
                            "id": folder_node_id,
                            "name": folder_part,
                            "type": "folder",
                            "catalog": None,
                            "is_selected": False,
                            "node_type": "folder",
                            "full_path": current_path,
                            "source_system": source_system,
                            "connector_id": connector_id or None,
                        })
                        node_ids.add(folder_node_id)
                        
                        # Connect folder to parent (container or previous folder)
                        if parent_node_id:
                            hierarchy_edges.append({
                                "id": f"{parent_node_id}-{folder_node_id}",
                                "source": parent_node_id,
                                "target": folder_node_id,
                                "type": "contains",
                                "column_lineage": [],
                                "confidence_score": 1.0,
                                "folder_based": True
                            })
                        
                        parent_node_id = folder_node_id
                    
                    # Connect asset to its folder
                    if parent_node_id:
                        hierarchy_edges.append({
                            "id": f"{parent_node_id}-{asset_id}",
                            "source": parent_node_id,
                            "target": asset_id,
                            "type": "contains",
                            "column_lineage": [],
                            "confidence_score": 1.0,
                            "folder_based": True
                        })
                    
                    # NOTE: Co-location relationships (same folder, parent folder, child folder assets) 
                    # are NOT included by default. These are spatial relationships, not actual data lineage.
                    # We only show the hierarchy (container -> folders -> asset) and real lineage relationships.
                
                # Combine all nodes: hierarchy nodes first, then asset nodes
                all_nodes = hierarchy_nodes + nodes
                all_edges = hierarchy_edges + edges
                
                return jsonify({
                    "asset": {
                        "id": asset.id,
                        "name": asset.name,
                        "type": asset.type,
                        "catalog": asset.catalog
                    },
                    "lineage": {
                        "nodes": all_nodes,
                        "edges": all_edges,
                        "upstream_count": upstream_count,
                        "downstream_count": downstream_count,
                        "folder_structure": folder_lineage.get('folder_structure') if folder_lineage and 'error' not in folder_lineage else None,
                        "hierarchy": folder_lineage.get('hierarchy') if folder_lineage and 'error' not in folder_lineage else None
                    }
                }), 200

            # No dataset_urn: return at least the asset and folder hierarchy so hierarchical view always shows something
            fallback_connector_id = getattr(asset, 'connector_id', None)
            fallback_source_system = _source_label_from_connector(fallback_connector_id)
            if fallback_source_system == "Unknown" and folder_lineage and "error" not in folder_lineage:
                storage_type = folder_lineage.get("storage_type") or ""
                if storage_type == "aws_s3":
                    fallback_source_system = "AWS S3"
                elif storage_type in ("azure_blob", "azure"):
                    fallback_source_system = "Azure Blob Storage"
            nodes_fallback = [{
                "id": asset.id,
                "name": asset.name,
                "type": asset.type,
                "catalog": asset.catalog,
                "connector_id": fallback_connector_id,
                "is_selected": True
            }]
            edges_fallback = []
            if folder_lineage and 'error' not in folder_lineage:
                hierarchy = folder_lineage.get('hierarchy', {})
                container_name = hierarchy.get('container') or None
                path_parts = hierarchy.get('path_parts', []) or []
                if container_name and container_name != 'unknown':
                    nodes_fallback.insert(0, {
                        "id": f"container_{container_name}",
                        "name": container_name,
                        "type": "container",
                        "catalog": None,
                        "is_selected": False,
                        "node_type": "container",
                        "source_system": fallback_source_system,
                        "connector_id": fallback_connector_id,
                    })
                current_path = ""
                parent_id = f"container_{container_name}" if (container_name and container_name != 'unknown') else None
                for folder_part in path_parts:
                    current_path = f"{current_path}/{folder_part}" if current_path else folder_part
                    folder_node_id = f"folder_{current_path.replace('/', '_')}"
                    nodes_fallback.insert(-1, {
                        "id": folder_node_id,
                        "name": folder_part,
                        "type": "folder",
                        "catalog": None,
                        "is_selected": False,
                        "node_type": "folder",
                        "full_path": current_path,
                        "source_system": fallback_source_system,
                        "connector_id": fallback_connector_id,
                    })
                    if parent_id:
                        edges_fallback.append({
                            "id": f"{parent_id}-{folder_node_id}",
                            "source": parent_id,
                            "target": folder_node_id,
                            "type": "contains",
                            "column_lineage": [],
                            "confidence_score": 1.0,
                            "folder_based": True
                        })
                    parent_id = folder_node_id
                if parent_id:
                    edges_fallback.append({
                        "id": f"{parent_id}-{asset.id}",
                        "source": parent_id,
                        "target": asset.id,
                        "type": "contains",
                        "column_lineage": [],
                        "confidence_score": 1.0,
                        "folder_based": True
                    })
                elif container_name and container_name != 'unknown':
                    edges_fallback.append({
                        "id": f"container_{container_name}-{asset.id}",
                        "source": f"container_{container_name}",
                        "target": asset.id,
                        "type": "contains",
                        "column_lineage": [],
                        "confidence_score": 1.0,
                        "folder_based": True
                    })
            return jsonify({
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog
                },
                "lineage": {
                    "nodes": nodes_fallback,
                    "edges": edges_fallback,
                    "upstream_count": 0,
                    "downstream_count": 0,
                    "hierarchy": folder_lineage.get('hierarchy') if folder_lineage and 'error' not in folder_lineage else None
                }
            }), 200
        except Exception as lineage_error:
            logger.error(f"Failed to use new lineage system for asset {asset_id}: {lineage_error}")
            # Return at least the asset so hierarchical view always shows something
            return jsonify({
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.type,
                    "catalog": asset.catalog
                },
                "lineage": {
                    "nodes": [{
                        "id": asset.id,
                        "name": asset.name,
                        "type": asset.type,
                        "catalog": asset.catalog,
                        "is_selected": True
                    }],
                    "edges": [],
                    "upstream_count": 0,
                    "downstream_count": 0
                }
            }), 200
    except Exception as e:
        logger.error('FN:get_asset_lineage asset_id:{} error:{}'.format(asset_id, str(e)), exc_info=True)
        if current_app.config.get("DEBUG"):
            return jsonify({"error": str(e)}), 400
        else:
            return jsonify({"error": "Failed to get asset lineage"}), 400
    finally:
        db.close()



@lineage_relationships_bp.route('/api/lineage/impact/<asset_id>', methods=['GET'])
@handle_error
def get_impact_analysis(asset_id):
    """
    This endpoint has been removed. Use /api/lineage/dataset/{urn}?direction=downstream for impact analysis.
    """
    return jsonify({
        "error": "This endpoint has been removed. Use /api/lineage/dataset/{urn}?direction=downstream for impact analysis."
    }), 410
