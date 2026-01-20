"""
Folder-based lineage inference service.
Creates lineage relationships based on folder structure hierarchy.
"""

from typing import List, Dict, Optional, Set
from datetime import datetime
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models import Asset, DataDiscovery
from services.asset_lineage_integration import AssetLineageIntegration
from services.lineage_ingestion import LineageIngestionService
from services.manual_lineage_service import ManualLineageService

logger = logging.getLogger(__name__)


class FolderBasedLineageService:
    """Infers lineage relationships based on folder structure"""
    
    def __init__(self):
        self.asset_integration = AssetLineageIntegration()
        self.ingestion_service = LineageIngestionService()
        self.manual_service = ManualLineageService()
    
    def get_folder_based_lineage(self, asset_id: str) -> Dict:
        """
        Get hierarchical lineage structure for a given asset.
        
        Returns:
        {
            'hierarchy': {
                'container': str,
                'path_parts': [str],  # ['testDir', 'testDir2', 'gavin', ...]
                'full_path': str
            },
            'same_folder': [asset_ids],  # Assets in same folder
            'parent_folder': [asset_ids],  # Assets in parent folder
            'child_folders': [asset_ids],  # Assets in child folders
            'sibling_folders': [asset_ids]  # Assets in sibling folders (same parent)
        }
        """
        db = SessionLocal()
        try:
            # Get the asset and its discovery info
            asset = db.query(Asset).filter(Asset.id == asset_id).first()
            if not asset:
                return {'error': 'Asset not found'}
            
            discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
            if not discovery or not discovery.folder_path:
                return {
                    'same_folder': [],
                    'parent_folder': [],
                    'child_folders': [],
                    'sibling_folders': []
                }
            
            folder_path = discovery.folder_path.strip('/')
            folder_parts = folder_path.split('/') if folder_path else []
            
            # Get container from storage_location
            container_name = None
            if discovery.storage_location and isinstance(discovery.storage_location, dict):
                container_info = discovery.storage_location.get('container', {})
                if isinstance(container_info, dict):
                    container_name = container_info.get('name')
                elif isinstance(container_info, str):
                    container_name = container_info
            
            # Get connection info to filter by same connection
            connection_name = None
            if discovery.discovery_info and isinstance(discovery.discovery_info, dict):
                connection_name = discovery.discovery_info.get('connection_name')
            
            # Build folder path patterns
            same_folder_pattern = folder_path
            parent_folder_pattern = '/'.join(folder_parts[:-1]) if len(folder_parts) > 1 else ''
            child_folder_prefix = folder_path + '/'
            
            # Get parent folder path for siblings
            if len(folder_parts) > 1:
                parent_path = '/'.join(folder_parts[:-1])
                sibling_prefix = parent_path + '/'
            else:
                sibling_prefix = ''
            
            # Query assets in same container/connection
            from sqlalchemy import func
            query = db.query(Asset, DataDiscovery).join(
                DataDiscovery, Asset.id == DataDiscovery.asset_id
            ).filter(
                Asset.id != asset_id,
                DataDiscovery.folder_path.isnot(None)
            )
            
            # Filter by container if available (using JSON_EXTRACT for MySQL)
            if container_name:
                query = query.filter(
                    func.json_extract(DataDiscovery.storage_location, '$.container.name') == container_name
                )
            
            all_related = query.all()
            
            same_folder = []
            parent_folder = []
            child_folders = []
            sibling_folders = []
            
            for related_asset, related_discovery in all_related:
                if not related_discovery.folder_path:
                    continue
                
                related_path = related_discovery.folder_path.strip('/')
                
                # Same folder
                if related_path == folder_path:
                    same_folder.append({
                        'id': related_asset.id,
                        'name': related_asset.name,
                        'type': related_asset.type,
                        'catalog': related_asset.catalog,
                        'folder_path': related_path
                    })
                
                # Parent folder
                elif parent_folder_pattern and related_path == parent_folder_pattern:
                    parent_folder.append({
                        'id': related_asset.id,
                        'name': related_asset.name,
                        'type': related_asset.type,
                        'catalog': related_asset.catalog,
                        'folder_path': related_path
                    })
                
                # Child folders (assets in subfolders)
                elif related_path.startswith(child_folder_prefix):
                    child_folders.append({
                        'id': related_asset.id,
                        'name': related_asset.name,
                        'type': related_asset.type,
                        'catalog': related_asset.catalog,
                        'folder_path': related_path
                    })
                
                # Sibling folders (same parent, different folder)
                elif sibling_prefix and related_path.startswith(sibling_prefix) and not related_path.startswith(child_folder_prefix):
                    sibling_parts = related_path.split('/')
                    if len(sibling_parts) == len(folder_parts):  # Same depth
                        sibling_folders.append({
                            'id': related_asset.id,
                            'name': related_asset.name,
                            'type': related_asset.type,
                            'catalog': related_asset.catalog,
                            'folder_path': related_path
                        })
            
            # Build hierarchical path structure
            hierarchy = {
                'container': container_name or 'unknown',
                'path_parts': folder_parts,
                'full_path': folder_path,
                'asset_name': asset.name
            }
            
            return {
                'asset_id': asset_id,
                'folder_path': folder_path,
                'container': container_name,
                'hierarchy': hierarchy,
                'same_folder': same_folder,
                'parent_folder': parent_folder,
                'child_folders': child_folders,
                'sibling_folders': sibling_folders,
                'folder_structure': {
                    'current': folder_path,
                    'parent': parent_folder_pattern if parent_folder_pattern else None,
                    'depth': len(folder_parts)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting folder-based lineage: {e}", exc_info=True)
            return {'error': str(e)}
        finally:
            db.close()
    
    def create_folder_based_lineage_edges(self, asset_id: str, relationship_type: str = 'folder_hierarchy') -> Dict:
        """
        Create lineage edges in the new lineage system based on folder structure.
        Creates relationships for:
        - Same folder: 'co_location'
        - Parent folder: 'parent_folder'
        - Child folders: 'child_folder'
        """
        folder_lineage = self.get_folder_based_lineage(asset_id)
        
        if 'error' in folder_lineage:
            return folder_lineage
        
        # Get asset URN
        db = SessionLocal()
        try:
            asset = db.query(Asset).filter(Asset.id == asset_id).first()
            if not asset:
                return {'error': 'Asset not found'}
            
            discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
            asset_urn = self.asset_integration._generate_dataset_urn(asset, discovery)
            if not asset_urn:
                return {'error': 'Could not get dataset URN for asset'}
            
            edges_created = 0
            
            # Create process for folder-based relationships
            process_urn = f"urn:process:folder_structure:{folder_lineage.get('container', 'unknown')}"
            process_data = {
                'urn': process_urn,
                'name': f"Folder Structure: {folder_lineage.get('folder_path', '')}",
                'type': 'folder_structure',
                'source_system': 'folder_inference',
                'process_definition': {
                    'folder_path': folder_lineage.get('folder_path'),
                    'container': folder_lineage.get('container')
                }
            }
            
            # Get URNs for related assets
            related_asset_ids = (
                [a['id'] for a in folder_lineage.get('same_folder', [])] +
                [a['id'] for a in folder_lineage.get('parent_folder', [])] +
                [a['id'] for a in folder_lineage.get('child_folders', [])]
            )
            
            related_urns = []
            for related_id in related_asset_ids[:10]:  # Limit to 10 to avoid too many edges
                related_asset = db.query(Asset).filter(Asset.id == related_id).first()
                if related_asset:
                    related_discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == related_id).first()
                    related_urn = self.asset_integration._generate_dataset_urn(related_asset, related_discovery)
                    if related_urn:
                        related_urns.append(related_urn)
        finally:
            db.close()
        
        if related_urns:
            # Create lineage: related assets -> folder process -> current asset
            # This represents that assets in the same folder structure are related
            try:
                result = self.ingestion_service.ingest_process_lineage(
                    process_data=process_data,
                    input_datasets=related_urns[:5],  # Limit inputs
                    output_datasets=[asset_urn],
                    ingestion_id=f"folder_lineage_{asset_id}_{int(datetime.utcnow().timestamp())}",
                    user_id='folder_inference'
                )
                edges_created = result.get('edges_created', 0)
            except Exception as e:
                logger.error(f"Error creating folder-based lineage edges: {e}")
                return {'error': str(e)}
        
        return {
            'status': 'success',
            'edges_created': edges_created,
            'folder_lineage': folder_lineage
        }

