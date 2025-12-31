import logging
import sys
import os
from typing import Dict, Optional, Tuple
from sqlalchemy.orm import Session



Asset = None

def _get_asset_model():
    global Asset
    if Asset is None:
        try:

            import importlib.util
            backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            models_path = os.path.join(backend_path, 'models.py')
            if os.path.exists(models_path):
                spec = importlib.util.spec_from_file_location("models", models_path)
                models_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(models_module)
                Asset = models_module.Asset
            else:

                from models import Asset as AssetModel
                Asset = AssetModel
        except (ImportError, ValueError, Exception) as e:

            try:
                backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if backend_path not in sys.path:
                    sys.path.insert(0, backend_path)
                from models import Asset as AssetModel
                Asset = AssetModel
            except ImportError:
                logger.error(f'FN:_get_asset_model error:Could not import Asset model error:{str(e)}')
                raise
    return Asset

logger = logging.getLogger(__name__)


def normalize_path(path: str) -> str:
    if not path:
        return ""

    normalized = path.strip('/')

    return normalized.lower()


def check_asset_exists(
    db: Session,
    connector_id: str,
    storage_path: str
):
    try:
        Asset = _get_asset_model()

        normalized_search_path = normalize_path(storage_path)
        
        if not normalized_search_path:
            logger.warning(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} message:Empty normalized path')
            return None
        



        from sqlalchemy import text
        

        query = text("""
            SELECT id FROM assets 
            WHERE connector_id = :connector_id 
            AND (
                JSON_UNQUOTE(JSON_EXTRACT(technical_metadata, '$.location')) = :path
                OR JSON_UNQUOTE(JSON_EXTRACT(technical_metadata, '$.storage_path')) = :path
            )
            LIMIT 1
        """)
        
        result = db.execute(query, {
            'connector_id': connector_id,
            'path': normalized_search_path
        })
        
        row = result.fetchone()
        if row:
            asset_id = row[0]
            return db.query(Asset).filter(Asset.id == asset_id).first()
        
    except Exception as e:
        logger.error(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} error:{str(e)}')
        try:
            Asset = _get_asset_model()
            assets = db.query(Asset).filter(
                Asset.connector_id == connector_id
            ).all()
            
            normalized_search_path = normalize_path(storage_path)
            for asset in assets:
                tech_meta = asset.technical_metadata or {}
                stored_location = tech_meta.get('location') or tech_meta.get('storage_path') or ""
                normalized_stored = normalize_path(stored_location)
                
                if normalized_stored == normalized_search_path:
                    return asset
        except Exception:
            pass
        return None


def get_asset_hashes(asset) -> Tuple[Optional[str], Optional[str]]:
    tech_meta = asset.technical_metadata or {}
    
    file_hash = (
        tech_meta.get('file_hash') or
        tech_meta.get('hash', {}).get('value') or
        None
    )
    
    schema_hash = tech_meta.get('schema_hash') or None
    
    return file_hash, schema_hash


def compare_hashes(
    existing_file_hash: Optional[str],
    existing_schema_hash: Optional[str],
    new_file_hash: str,
    new_schema_hash: str
) -> Tuple[bool, bool]:
    file_changed = existing_file_hash != new_file_hash if existing_file_hash else True
    schema_changed = existing_schema_hash != new_schema_hash if existing_schema_hash else True
    
    return file_changed, schema_changed


def should_update_or_insert(
    existing_asset,
    new_file_hash: str,
    new_schema_hash: str
) -> Tuple[bool, bool]:
    if not existing_asset:
        return True, False
    
    existing_file_hash, existing_schema_hash = get_asset_hashes(existing_asset)
    file_changed, schema_changed = compare_hashes(
        existing_file_hash,
        existing_schema_hash,
        new_file_hash,
        new_schema_hash
    )
    
    if schema_changed:
        logger.info(f'FN:should_update_or_insert schema_changed:True existing_asset_id:{existing_asset.id}')
        return True, True
    
    if file_changed:
        logger.info(f'FN:should_update_or_insert file_changed:True schema_changed:False existing_asset_id:{existing_asset.id}')
        return False, False
    
    logger.info(f'FN:should_update_or_insert file_changed:False schema_changed:False existing_asset_id:{existing_asset.id}')
    return False, False