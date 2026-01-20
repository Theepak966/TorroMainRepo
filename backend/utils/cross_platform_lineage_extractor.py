"""
Cross-Platform Lineage Extraction Service
Finds relationships between assets across different platforms:
- Oracle DB <-> Azure Blob Storage
- Oracle DB <-> Other databases
- Azure Blob Storage <-> Other storage systems
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

try:
    from utils.ml_lineage_inference import infer_relationships_ml, fuzzy_column_match
    ML_INFERENCE_AVAILABLE = True
except ImportError:
    ML_INFERENCE_AVAILABLE = False
    logger.warning("ML inference not available")


class CrossPlatformLineageExtractor:
    """Extract lineage relationships across different platforms"""
    
    def __init__(self):
        pass
    
    def extract_cross_platform_lineage(
        self,
        all_assets: Dict,
        progress_callback: Optional[callable] = None
    ) -> List[Dict]:
        """
        Extract cross-platform lineage relationships.
        Returns list of lineage relationship dictionaries.
        """
        all_lineage = []
        
        # Group assets by platform
        oracle_assets = {}
        azure_blob_assets = {}
        other_assets = {}
        
        for asset_id, asset in all_assets.items():
            connector_id = asset.connector_id if hasattr(asset, 'connector_id') else ''
            
            if connector_id.startswith('oracle_db_'):
                oracle_assets[asset_id] = asset
            elif connector_id.startswith('azure_blob_'):
                azure_blob_assets[asset_id] = asset
            else:
                other_assets[asset_id] = asset
        
        # Method 1: Oracle DB <-> Azure Blob Storage
        if progress_callback:
            progress_callback("Finding Oracle <-> Azure Blob relationships...")
        oracle_azure_lineage = self._extract_oracle_azure_lineage(oracle_assets, azure_blob_assets)
        all_lineage.extend(oracle_azure_lineage)
        logger.info(f'FN:extract_cross_platform_lineage method:oracle_azure found:{len(oracle_azure_lineage)} relationships')
        
        # Method 2: Name-based matching across platforms
        if progress_callback:
            progress_callback("Matching assets by name across platforms...")
        name_lineage = self._extract_name_based_lineage(all_assets)
        all_lineage.extend(name_lineage)
        logger.info(f'FN:extract_cross_platform_lineage method:name_matching found:{len(name_lineage)} relationships')
        
        # Method 3: Column schema matching across platforms
        if progress_callback:
            progress_callback("Matching schemas across platforms...")
        schema_lineage = self._extract_schema_based_lineage(all_assets)
        all_lineage.extend(schema_lineage)
        logger.info(f'FN:extract_cross_platform_lineage method:schema_matching found:{len(schema_lineage)} relationships')
        
        # Method 4: SQL query references to cross-platform assets
        if progress_callback:
            progress_callback("Finding SQL references to cross-platform assets...")
        sql_lineage = self._extract_sql_cross_platform_lineage(all_assets)
        all_lineage.extend(sql_lineage)
        logger.info(f'FN:extract_cross_platform_lineage method:sql_references found:{len(sql_lineage)} relationships')
        
        # Method 5: ML-based inference across platforms
        if progress_callback:
            progress_callback("Inferring cross-platform relationships using ML...")
        ml_lineage = self._extract_ml_cross_platform_lineage(all_assets)
        all_lineage.extend(ml_lineage)
        logger.info(f'FN:extract_cross_platform_lineage method:ml_inference found:{len(ml_lineage)} relationships')
        
        # Deduplicate
        deduplicated = self._deduplicate_lineage(all_lineage)
        logger.info(f'FN:extract_cross_platform_lineage total:{len(all_lineage)} deduplicated:{len(deduplicated)}')
        
        return deduplicated
    
    def _extract_oracle_azure_lineage(
        self,
        oracle_assets: Dict,
        azure_blob_assets: Dict
    ) -> List[Dict]:
        """Extract relationships between Oracle DB and Azure Blob Storage"""
        lineage = []
        
        try:
            for oracle_id, oracle_asset in oracle_assets.items():
                oracle_name = oracle_asset.name if hasattr(oracle_asset, 'name') else oracle_id
                oracle_schema = oracle_asset.catalog if hasattr(oracle_asset, 'catalog') else ''
                oracle_cols = oracle_asset.columns if hasattr(oracle_asset, 'columns') else []
                
                # Normalize Oracle name (remove schema prefix if present)
                oracle_base_name = oracle_name.split('.')[-1] if '.' in oracle_name else oracle_name
                oracle_base_name_clean = re.sub(r'[_\-\s]', '', oracle_base_name.lower())
                
                for azure_id, azure_asset in azure_blob_assets.items():
                    azure_name = azure_asset.name if hasattr(azure_asset, 'name') else azure_id
                    azure_cols = azure_asset.columns if hasattr(azure_asset, 'columns') else []
                    
                    # Normalize Azure name (remove path, extension)
                    azure_base_name = azure_name.split('/')[-1] if '/' in azure_name else azure_name
                    azure_base_name = azure_base_name.split('.')[0] if '.' in azure_base_name else azure_base_name
                    azure_base_name_clean = re.sub(r'[_\-\s]', '', azure_base_name.lower())
                    
                    # Check name similarity
                    name_match = False
                    confidence = 0.0
                    
                    if oracle_base_name_clean == azure_base_name_clean:
                        name_match = True
                        confidence = 0.9
                    elif oracle_base_name_clean in azure_base_name_clean or azure_base_name_clean in oracle_base_name_clean:
                        name_match = True
                        confidence = 0.75
                    elif ML_INFERENCE_AVAILABLE:
                        is_match, similarity = fuzzy_column_match(oracle_base_name, azure_base_name, threshold=0.7)
                        if is_match:
                            name_match = True
                            confidence = similarity * 0.9
                    
                    # Check column schema match
                    column_lineage = None
                    if oracle_cols and azure_cols:
                        if ML_INFERENCE_AVAILABLE:
                            inferred_lineage, ml_confidence = infer_relationships_ml(
                                oracle_cols,
                                azure_cols,
                                min_matching_ratio=0.3
                            )
                            if inferred_lineage:
                                column_lineage = inferred_lineage
                                confidence = max(confidence, ml_confidence * 0.95)
                    
                    if name_match and confidence >= 0.7:
                        # Determine direction: Oracle -> Azure (export) or Azure -> Oracle (import)
                        # Heuristic: if Azure file has "export" or "dump" in path, Oracle is source
                        azure_path = azure_asset.operational_metadata.get('path', '') if hasattr(azure_asset, 'operational_metadata') and azure_asset.operational_metadata else ''
                        is_export = any(kw in azure_path.lower() or kw in azure_name.lower() for kw in ['export', 'dump', 'backup', 'extract'])
                        is_import = any(kw in azure_path.lower() or kw in azure_name.lower() for kw in ['import', 'load', 'ingest', 'input'])
                        
                        if is_export:
                            source_id, target_id = oracle_id, azure_id
                            direction = "export"
                        elif is_import:
                            source_id, target_id = azure_id, oracle_id
                            direction = "import"
                        else:
                            # Default: Oracle -> Azure (most common pattern)
                            source_id, target_id = oracle_id, azure_id
                            direction = "export"
                        
                        lineage.append({
                            "source_asset_id": source_id,
                            "target_asset_id": target_id,
                            "relationship_type": "cross_platform",
                            "source_type": "table" if source_id == oracle_id else "file",
                            "target_type": "file" if target_id == azure_id else "table",
                            "column_lineage": column_lineage,
                            "transformation_type": direction,
                            "transformation_description": f"Cross-platform relationship: {direction} (confidence: {confidence:.2f})",
                            "sql_query": None,
                            "source_system": "cross_platform",
                            "source_job_id": f"cross_platform_{oracle_id}_{azure_id}",
                            "source_job_name": f"Cross-Platform: {direction.title()}",
                            "confidence_score": min(confidence, 0.9),
                            "extraction_method": "cross_platform_matching",
                            "discovered_at": datetime.utcnow()
                        })
        
        except Exception as e:
            logger.error(f'FN:_extract_oracle_azure_lineage error:{str(e)}')
        
        return lineage
    
    def _extract_name_based_lineage(
        self,
        all_assets: Dict
    ) -> List[Dict]:
        """Extract relationships based on name matching across platforms"""
        lineage = []
        
        try:
            # Group assets by normalized name
            name_groups = defaultdict(list)
            
            for asset_id, asset in all_assets.items():
                asset_name = asset.name if hasattr(asset, 'name') else asset_id
                connector_id = asset.connector_id if hasattr(asset, 'connector_id') else ''
                
                # Normalize name
                normalized = re.sub(r'[_\-\s\.]', '', asset_name.lower())
                if normalized:
                    name_groups[normalized].append({
                        'asset_id': asset_id,
                        'asset': asset,
                        'connector_id': connector_id
                    })
            
            # Create relationships between assets with same normalized name but different platforms
            for normalized_name, assets in name_groups.items():
                if len(assets) < 2:
                    continue
                
                # Group by platform
                platforms = defaultdict(list)
                for asset_info in assets:
                    platform = asset_info['connector_id'].split('_')[0] if '_' in asset_info['connector_id'] else 'unknown'
                    platforms[platform].append(asset_info)
                
                # Create relationships between different platforms
                platform_list = list(platforms.items())
                for i, (platform1, assets1) in enumerate(platform_list):
                    for platform2, assets2 in platform_list[i+1:]:
                        if platform1 != platform2:
                            for asset1_info in assets1:
                                for asset2_info in assets2:
                                    lineage.append({
                                        "source_asset_id": asset1_info['asset_id'],
                                        "target_asset_id": asset2_info['asset_id'],
                                        "relationship_type": "cross_platform",
                                        "source_type": self._get_asset_type(asset1_info['asset']),
                                        "target_type": self._get_asset_type(asset2_info['asset']),
                                        "column_lineage": None,
                                        "transformation_type": "name_match",
                                        "transformation_description": f"Name match across platforms: {platform1} <-> {platform2}",
                                        "sql_query": None,
                                        "source_system": "cross_platform",
                                        "source_job_id": f"cross_name_{asset1_info['asset_id']}_{asset2_info['asset_id']}",
                                        "source_job_name": f"Cross-Platform Name Match",
                                        "confidence_score": 0.75,
                                        "extraction_method": "name_matching",
                                        "discovered_at": datetime.utcnow()
                                    })
        
        except Exception as e:
            logger.error(f'FN:_extract_name_based_lineage error:{str(e)}')
        
        return lineage
    
    def _extract_schema_based_lineage(
        self,
        all_assets: Dict
    ) -> List[Dict]:
        """Extract relationships based on column schema matching across platforms"""
        lineage = []
        
        if not ML_INFERENCE_AVAILABLE:
            return lineage
        
        try:
            # Get assets with columns
            assets_with_cols = []
            for asset_id, asset in all_assets.items():
                columns = asset.columns if hasattr(asset, 'columns') else []
                if columns and len(columns) > 0:
                    connector_id = asset.connector_id if hasattr(asset, 'connector_id') else ''
                    assets_with_cols.append({
                        'asset_id': asset_id,
                        'asset': asset,
                        'columns': columns,
                        'connector_id': connector_id
                    })
            
            # Compare across different platforms
            for i, asset1_info in enumerate(assets_with_cols):
                platform1 = asset1_info['connector_id'].split('_')[0] if '_' in asset1_info['connector_id'] else 'unknown'
                
                for asset2_info in assets_with_cols[i+1:]:
                    platform2 = asset2_info['connector_id'].split('_')[0] if '_' in asset2_info['connector_id'] else 'unknown'
                    
                    if platform1 == platform2:
                        continue  # Skip same platform (handled by platform-specific extractors)
                    
                    inferred_lineage, confidence = infer_relationships_ml(
                        asset1_info['columns'],
                        asset2_info['columns'],
                        min_matching_ratio=0.4
                    )
                    
                    if inferred_lineage and confidence >= 0.7:
                        lineage.append({
                            "source_asset_id": asset1_info['asset_id'],
                            "target_asset_id": asset2_info['asset_id'],
                            "relationship_type": "cross_platform",
                            "source_type": self._get_asset_type(asset1_info['asset']),
                            "target_type": self._get_asset_type(asset2_info['asset']),
                            "column_lineage": inferred_lineage,
                            "transformation_type": "schema_match",
                            "transformation_description": f"Schema match across platforms: {platform1} <-> {platform2} ({len(inferred_lineage)} columns, confidence: {confidence:.2f})",
                            "sql_query": None,
                            "source_system": "cross_platform",
                            "source_job_id": f"cross_schema_{asset1_info['asset_id']}_{asset2_info['asset_id']}",
                            "source_job_name": f"Cross-Platform Schema Match",
                            "confidence_score": min(confidence, 0.9),
                            "extraction_method": "schema_matching",
                            "discovered_at": datetime.utcnow()
                        })
        
        except Exception as e:
            logger.error(f'FN:_extract_schema_based_lineage error:{str(e)}')
        
        return lineage
    
    def _extract_sql_cross_platform_lineage(
        self,
        all_assets: Dict
    ) -> List[Dict]:
        """Extract relationships from SQL queries that reference cross-platform assets"""
        lineage = []
        
        try:
            # Build asset name map for lookup
            asset_name_map = {}
            for asset_id, asset in all_assets.items():
                asset_name = asset.name if hasattr(asset, 'name') else asset_id
                connector_id = asset.connector_id if hasattr(asset, 'connector_id') else ''
                
                # Store various name formats
                asset_name_map[asset_name.lower()] = asset_id
                asset_name_map[asset_name] = asset_id
                
                # Store schema.table format for Oracle
                if connector_id.startswith('oracle_db_'):
                    schema = asset.catalog if hasattr(asset, 'catalog') else ''
                    if schema:
                        asset_name_map[f"{schema}.{asset_name}".lower()] = asset_id
                        asset_name_map[f"{schema}.{asset_name}"] = asset_id
            
            # Look for SQL queries in assets
            for asset_id, asset in all_assets.items():
                sql_queries = []
                
                # Check technical_metadata for SQL
                if hasattr(asset, 'technical_metadata') and asset.technical_metadata:
                    if isinstance(asset.technical_metadata, dict):
                        for sql_field in ['sql_query', 'view_definition', 'create_statement', 'ddl']:
                            sql_text = asset.technical_metadata.get(sql_field)
                            if sql_text:
                                sql_queries.append(sql_text)
                
                # Parse SQL to find references to other assets
                for sql_query in sql_queries:
                    sql_lower = sql_query.lower()
                    
                    # Find referenced asset names in SQL
                    for ref_name, ref_asset_id in asset_name_map.items():
                        if ref_asset_id == asset_id:
                            continue
                        
                        # Check if reference appears in SQL
                        ref_name_lower = ref_name.lower()
                        if ref_name_lower in sql_lower:
                            # Check if it's a real reference (not just substring)
                            pattern = r'\b' + re.escape(ref_name_lower) + r'\b'
                            if re.search(pattern, sql_lower):
                                ref_asset = all_assets.get(ref_asset_id)
                                if ref_asset:
                                    ref_connector = ref_asset.connector_id if hasattr(ref_asset, 'connector_id') else ''
                                    asset_connector = asset.connector_id if hasattr(asset, 'connector_id') else ''
                                    
                                    # Only create cross-platform relationships
                                    ref_platform = ref_connector.split('_')[0] if '_' in ref_connector else 'unknown'
                                    asset_platform = asset_connector.split('_')[0] if '_' in asset_connector else 'unknown'
                                    
                                    if ref_platform != asset_platform:
                                        lineage.append({
                                            "source_asset_id": ref_asset_id,
                                            "target_asset_id": asset_id,
                                            "relationship_type": "cross_platform",
                                            "source_type": self._get_asset_type(ref_asset),
                                            "target_type": self._get_asset_type(asset),
                                            "column_lineage": None,
                                            "transformation_type": "sql_reference",
                                            "transformation_description": f"Referenced in SQL query ({asset_platform} -> {ref_platform})",
                                            "sql_query": sql_query[:500],
                                            "source_system": "cross_platform",
                                            "source_job_id": f"cross_sql_{ref_asset_id}_{asset_id}",
                                            "source_job_name": "Cross-Platform SQL Reference",
                                            "confidence_score": 0.85,
                                            "extraction_method": "sql_reference",
                                            "discovered_at": datetime.utcnow()
                                        })
        
        except Exception as e:
            logger.error(f'FN:_extract_sql_cross_platform_lineage error:{str(e)}')
        
        return lineage
    
    def _extract_ml_cross_platform_lineage(
        self,
        all_assets: Dict
    ) -> List[Dict]:
        """Extract relationships using ML-based inference across platforms"""
        lineage = []
        
        if not ML_INFERENCE_AVAILABLE:
            return lineage
        
        try:
            # Get assets with columns, grouped by platform
            assets_by_platform = defaultdict(list)
            
            for asset_id, asset in all_assets.items():
                columns = asset.columns if hasattr(asset, 'columns') else []
                if columns and len(columns) > 0:
                    connector_id = asset.connector_id if hasattr(asset, 'connector_id') else ''
                    platform = connector_id.split('_')[0] if '_' in connector_id else 'unknown'
                    assets_by_platform[platform].append({
                        'asset_id': asset_id,
                        'asset': asset,
                        'columns': columns
                    })
            
            # Compare across platforms
            platform_list = list(assets_by_platform.items())
            for i, (platform1, assets1) in enumerate(platform_list):
                for platform2, assets2 in platform_list[i+1:]:
                    if platform1 == platform2:
                        continue
                    
                    for asset1_info in assets1:
                        for asset2_info in assets2:
                            inferred_lineage, confidence = infer_relationships_ml(
                                asset1_info['columns'],
                                asset2_info['columns'],
                                min_matching_ratio=0.35
                            )
                            
                            if inferred_lineage and confidence >= 0.65:
                                lineage.append({
                                    "source_asset_id": asset1_info['asset_id'],
                                    "target_asset_id": asset2_info['asset_id'],
                                    "relationship_type": "cross_platform",
                                    "source_type": self._get_asset_type(asset1_info['asset']),
                                    "target_type": self._get_asset_type(asset2_info['asset']),
                                    "column_lineage": inferred_lineage,
                                    "transformation_type": "ml_inference",
                                    "transformation_description": f"ML-inferred cross-platform relationship: {platform1} <-> {platform2} (confidence: {confidence:.2f})",
                                    "sql_query": None,
                                    "source_system": "cross_platform",
                                    "source_job_id": f"cross_ml_{asset1_info['asset_id']}_{asset2_info['asset_id']}",
                                    "source_job_name": "Cross-Platform ML Inference",
                                    "confidence_score": min(confidence, 0.85),
                                    "extraction_method": "ml_inference",
                                    "discovered_at": datetime.utcnow()
                                })
        
        except Exception as e:
            logger.error(f'FN:_extract_ml_cross_platform_lineage error:{str(e)}')
        
        return lineage
    
    def _get_asset_type(self, asset) -> str:
        """Get asset type string"""
        if hasattr(asset, 'type') and asset.type:
            return asset.type
        return 'file'
    
    def _deduplicate_lineage(self, lineage: List[Dict]) -> List[Dict]:
        """Deduplicate lineage based on (source_asset_id, target_asset_id, source_job_id)"""
        seen = set()
        deduplicated = []
        
        for rel in lineage:
            key = (
                rel.get('source_asset_id'),
                rel.get('target_asset_id'),
                rel.get('source_job_id')
            )
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(rel)
            else:
                # If duplicate, keep the one with higher confidence
                existing = next((r for r in deduplicated if (
                    r.get('source_asset_id') == rel.get('source_asset_id') and
                    r.get('target_asset_id') == rel.get('target_asset_id') and
                    r.get('source_job_id') == rel.get('source_job_id')
                )), None)
                
                if existing and rel.get('confidence_score', 0) > existing.get('confidence_score', 0):
                    deduplicated.remove(existing)
                    deduplicated.append(rel)
        
        return deduplicated

