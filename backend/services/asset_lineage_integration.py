"""
Integration service to automatically register discovered assets in the lineage system.
This ensures that all discovered assets are available for lineage tracking.
"""

from typing import Dict, List, Optional
from datetime import datetime
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models_lineage.models_lineage import Dataset
from models import Asset, DataDiscovery

logger = logging.getLogger(__name__)


class AssetLineageIntegration:
    """Automatically registers discovered assets as datasets in the lineage system"""
    
    def register_discovered_asset(self, asset: Asset, discovery: Optional[DataDiscovery] = None) -> Optional[str]:
        """
        Register a discovered asset as a dataset in the lineage system.
        
        Args:
            asset: The Asset object from discovery
            discovery: Optional DataDiscovery object with additional metadata
            
        Returns:
            Dataset URN if successful, None otherwise
        """
        db = SessionLocal()
        try:
            # Generate URN for the asset
            dataset_urn = self._generate_dataset_urn(asset, discovery)
            
            # Check if dataset already exists
            existing_dataset = db.query(Dataset).filter(Dataset.urn == dataset_urn).first()
            
            if existing_dataset:
                # Update existing dataset with latest metadata
                existing_dataset.name = asset.name
                existing_dataset.type = asset.type or 'table'
                existing_dataset.catalog = asset.catalog
                existing_dataset.updated_at = datetime.utcnow()
                
                # Update storage location if discovery info is available
                if discovery:
                    existing_dataset.storage_type = discovery.data_source_type or 'azure_blob_storage'
                    existing_dataset.storage_location = {
                        'container': discovery.discovery_info.get('container') if discovery.discovery_info else None,
                        'folder_path': discovery.folder_path,
                        'storage_location': discovery.storage_location
                    }
                
                db.commit()
                logger.debug(f"Updated existing lineage dataset: {dataset_urn}")
                return dataset_urn
            
            # Create new dataset
            dataset = Dataset(
                urn=dataset_urn,
                name=asset.name,
                type=asset.type or 'table',
                catalog=asset.catalog,
                schema_name=self._extract_schema_from_asset(asset, discovery),
                storage_type=discovery.data_source_type if discovery else 'unknown',
                storage_location={
                    'container': discovery.discovery_info.get('container') if discovery and discovery.discovery_info else None,
                    'folder_path': discovery.folder_path if discovery else None,
                    'storage_location': discovery.storage_location if discovery else None
                } if discovery else None,
                created_by='discovery_system',
                created_at=datetime.utcnow()
            )
            
            db.add(dataset)
            db.commit()
            logger.debug(f"Registered new lineage dataset: {dataset_urn}")
            return dataset_urn
            
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to register asset in lineage system: {e}", exc_info=True)
            return None
        finally:
            db.close()
    
    def register_batch_assets(self, assets: List[Asset], discoveries: Optional[List[DataDiscovery]] = None) -> Dict[str, int]:
        """
        Bulk register multiple assets in the lineage system.
        
        Args:
            assets: List of Asset objects
            discoveries: Optional list of DataDiscovery objects (must match assets order)
            
        Returns:
            Dict with counts: {'registered': int, 'updated': int, 'failed': int}
        """
        db = SessionLocal()
        registered = 0
        updated = 0
        failed = 0
        
        try:
            discoveries_dict = {}
            if discoveries:
                # Create a mapping from asset_id to discovery
                for discovery in discoveries:
                    if discovery.asset_id:
                        discoveries_dict[discovery.asset_id] = discovery
            
            for asset in assets:
                try:
                    discovery = discoveries_dict.get(asset.id) if discoveries_dict else None
                    dataset_urn = self._generate_dataset_urn(asset, discovery)
                    
                    # Check if exists
                    existing = db.query(Dataset).filter(Dataset.urn == dataset_urn).first()
                    
                    if existing:
                        # Update
                        existing.name = asset.name
                        existing.type = asset.type or 'table'
                        existing.catalog = asset.catalog
                        existing.updated_at = datetime.utcnow()
                        if discovery:
                            existing.storage_type = discovery.data_source_type or 'azure_blob_storage'
                            existing.storage_location = {
                                'container': discovery.discovery_info.get('container') if discovery.discovery_info else None,
                                'folder_path': discovery.folder_path,
                                'storage_location': discovery.storage_location
                            }
                        updated += 1
                    else:
                        # Create
                        dataset = Dataset(
                            urn=dataset_urn,
                            name=asset.name,
                            type=asset.type or 'table',
                            catalog=asset.catalog,
                            schema_name=self._extract_schema_from_asset(asset, discovery),
                            storage_type=discovery.data_source_type if discovery else 'unknown',
                            storage_location={
                                'container': discovery.discovery_info.get('container') if discovery and discovery.discovery_info else None,
                                'folder_path': discovery.folder_path if discovery else None,
                                'storage_location': discovery.storage_location if discovery else None
                            } if discovery else None,
                            created_by='discovery_system',
                            created_at=datetime.utcnow()
                        )
                        db.add(dataset)
                        registered += 1
                    
                except Exception as e:
                    logger.error(f"Failed to register asset {asset.id} in lineage: {e}")
                    failed += 1
                    continue
            
            db.commit()
            logger.info(f"Bulk registered assets in lineage: {registered} new, {updated} updated, {failed} failed")
            
            return {
                'registered': registered,
                'updated': updated,
                'failed': failed
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"Bulk registration failed: {e}", exc_info=True)
            return {'registered': 0, 'updated': 0, 'failed': len(assets)}
        finally:
            db.close()
    
    def _generate_dataset_urn(self, asset: Asset, discovery: Optional[DataDiscovery] = None) -> str:
        """
        Generate a URN for the dataset based on asset and discovery metadata.
        
        Format: urn:dataset:{source_type}:{catalog}.{schema}.{name}
        """
        # Determine source type
        if discovery and discovery.data_source_type:
            source_type = discovery.data_source_type.replace('_', '-')
        elif asset.connector_id:
            source_type = asset.connector_id.replace('_', '-')
        else:
            source_type = 'unknown'
        
        # Build catalog.schema.name
        parts = []
        if asset.catalog:
            parts.append(asset.catalog)
        
        schema = self._extract_schema_from_asset(asset, discovery)
        if schema:
            parts.append(schema)
        
        parts.append(asset.name)
        
        qualified_name = '.'.join(parts)
        
        return f"urn:dataset:{source_type}:{qualified_name}"
    
    def _extract_schema_from_asset(self, asset: Asset, discovery: Optional[DataDiscovery] = None) -> Optional[str]:
        """Extract schema name from asset or discovery metadata"""
        # Try to get schema from discovery folder path
        if discovery and discovery.folder_path:
            # Extract schema from folder path (e.g., "schema/table" -> "schema")
            path_parts = discovery.folder_path.strip('/').split('/')
            if len(path_parts) > 1:
                return path_parts[0]  # First part as schema
        
        # Try to get from technical metadata
        if asset.technical_metadata and isinstance(asset.technical_metadata, dict):
            schema = asset.technical_metadata.get('schema') or asset.technical_metadata.get('schema_name')
            if schema:
                return schema
        
        return None
    
    def get_dataset_urn_for_asset(self, asset_id: str) -> Optional[str]:
        """Get the lineage dataset URN for a given asset ID"""
        db = SessionLocal()
        try:
            asset = db.query(Asset).filter(Asset.id == asset_id).first()
            if not asset:
                return None
            
            discovery = db.query(DataDiscovery).filter(DataDiscovery.asset_id == asset_id).first()
            return self._generate_dataset_urn(asset, discovery)
        finally:
            db.close()









