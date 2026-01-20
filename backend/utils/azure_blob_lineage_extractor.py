"""
Advanced Azure Blob Storage Lineage Extraction Service
Extracts actual data lineage using multiple advanced methods:
1. Folder hierarchy relationships
2. File naming pattern matching
3. Column schema matching (if available)
4. SQL query references to blob paths
5. ML-based inference for file relationships
6. ETL job pattern detection
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from collections import defaultdict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

try:
    from utils.ml_lineage_inference import infer_relationships_ml, fuzzy_column_match
    ML_INFERENCE_AVAILABLE = True
except ImportError:
    ML_INFERENCE_AVAILABLE = False
    logger.warning("ML inference not available")


class AzureBlobLineageExtractor:
    """Advanced Azure Blob Storage lineage extraction using multiple methods"""
    
    def __init__(self):
        pass
    
    def extract_comprehensive_lineage(
        self,
        connector_id: str,
        asset_map: Dict,
        progress_callback: Optional[callable] = None
    ) -> List[Dict]:
        """
        Extract comprehensive lineage for Azure Blob Storage assets.
        Returns list of lineage relationship dictionaries.
        """
        all_lineage = []
        
        # Method 1: Folder hierarchy relationships
        if progress_callback:
            progress_callback("Extracting folder hierarchy relationships...")
        folder_lineage = self._extract_folder_hierarchy_lineage(connector_id, asset_map)
        all_lineage.extend(folder_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:folder_hierarchy found:{len(folder_lineage)} relationships')
        
        # Method 2: File naming pattern matching
        if progress_callback:
            progress_callback("Matching files by naming patterns...")
        pattern_lineage = self._extract_naming_pattern_lineage(connector_id, asset_map)
        all_lineage.extend(pattern_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:naming_patterns found:{len(pattern_lineage)} relationships')
        
        # Method 3: Column schema matching
        if progress_callback:
            progress_callback("Matching files by column schemas...")
        schema_lineage = self._extract_schema_matching_lineage(connector_id, asset_map)
        all_lineage.extend(schema_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:schema_matching found:{len(schema_lineage)} relationships')
        
        # Method 4: SQL query references
        if progress_callback:
            progress_callback("Finding SQL references to blob paths...")
        sql_lineage = self._extract_sql_reference_lineage(connector_id, asset_map)
        all_lineage.extend(sql_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:sql_references found:{len(sql_lineage)} relationships')
        
        # Method 5: ML-based inference
        if progress_callback:
            progress_callback("Inferring relationships using ML...")
        ml_lineage = self._extract_ml_inferred_lineage(connector_id, asset_map)
        all_lineage.extend(ml_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:ml_inference found:{len(ml_lineage)} relationships')
        
        # Method 6: ETL job pattern detection
        if progress_callback:
            progress_callback("Detecting ETL job patterns...")
        etl_lineage = self._extract_etl_pattern_lineage(connector_id, asset_map)
        all_lineage.extend(etl_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:etl_patterns found:{len(etl_lineage)} relationships')
        
        # Deduplicate
        deduplicated = self._deduplicate_lineage(all_lineage)
        logger.info(f'FN:extract_comprehensive_lineage total:{len(all_lineage)} deduplicated:{len(deduplicated)}')
        
        return deduplicated
    
    def _extract_folder_hierarchy_lineage(
        self,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract folder hierarchy relationships (parent -> child)"""
        lineage = []
        
        try:
            # Group assets by container and folder path
            container_folders = defaultdict(lambda: defaultdict(list))
            
            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue
                
                # Extract container and path from asset metadata
                container = None
                folder_path = None
                
                if hasattr(asset, 'operational_metadata') and asset.operational_metadata:
                    if isinstance(asset.operational_metadata, dict):
                        container = asset.operational_metadata.get('container')
                        folder_path = asset.operational_metadata.get('folder_path') or asset.operational_metadata.get('path')
                
                if not container:
                    # Try to extract from name or catalog
                    if hasattr(asset, 'catalog') and asset.catalog:
                        container = asset.catalog
                    elif hasattr(asset, 'name') and '/' in asset.name:
                        parts = asset.name.split('/', 1)
                        container = parts[0]
                        folder_path = parts[1] if len(parts) > 1 else None
                
                if container:
                    # Extract folder from path
                    if folder_path:
                        folder_parts = folder_path.split('/')
                        if len(folder_parts) > 1:
                            folder = '/'.join(folder_parts[:-1])  # All but last part
                        else:
                            folder = ''
                    else:
                        folder = ''
                    
                    container_folders[container][folder].append({
                        'asset_id': asset_id,
                        'asset': asset,
                        'file_name': folder_parts[-1] if folder_path and '/' in folder_path else (asset.name if hasattr(asset, 'name') else '')
                    })
            
            # Create parent-child relationships within folders
            for container, folders in container_folders.items():
                # Sort folders by depth
                sorted_folders = sorted(folders.items(), key=lambda x: x[0].count('/'))
                
                for folder, assets in folders.items():
                    if not folder:  # Root level
                        continue
                    
                    # Find parent folder
                    parent_folder = '/'.join(folder.split('/')[:-1]) if '/' in folder else ''
                    
                    # Get parent folder assets
                    parent_assets = container_folders[container].get(parent_folder, [])
                    
                    # Create relationships: parent folder files -> child folder files
                    for parent_asset_info in parent_assets:
                        for child_asset_info in assets:
                            lineage.append({
                                "source_asset_id": parent_asset_info['asset_id'],
                                "target_asset_id": child_asset_info['asset_id'],
                                "relationship_type": "folder_hierarchy",
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": None,
                                "transformation_type": "folder_hierarchy",
                                "transformation_description": f"Folder hierarchy: {parent_folder} -> {folder}",
                                "sql_query": None,
                                "source_system": "azure_blob",
                                "source_job_id": f"azure_folder_{container}_{folder.replace('/', '_')}",
                                "source_job_name": "Azure Folder Hierarchy",
                                "confidence_score": 0.7,
                                "extraction_method": "folder_hierarchy",
                                "discovered_at": datetime.utcnow()
                            })
        
        except Exception as e:
            logger.error(f'FN:_extract_folder_hierarchy_lineage connector_id:{connector_id} error:{str(e)}')
        
        return lineage
    
    def _extract_naming_pattern_lineage(
        self,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract relationships based on file naming patterns"""
        lineage = []
        
        try:
            # Common ETL patterns
            patterns = [
                (r'(.+?)_(raw|staging|stg|temp|tmp)\.(.+)', r'\1_(processed|final|output)\.\3', 'etl_pipeline'),
                (r'(.+?)_(input|source)\.(.+)', r'\1_(output|target|result)\.\3', 'etl_pipeline'),
                (r'(.+?)_(\d{8})\.(.+)', r'\1_(\d{8})\.\3', 'date_partition'),  # Date-based partitioning
                (r'(.+?)_(backup|archive)\.(.+)', r'\1\.\3', 'backup_relationship'),
            ]
            
            # Group assets by base name pattern
            asset_groups = defaultdict(list)
            
            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue
                
                asset_name = asset.name if hasattr(asset, 'name') else asset_id
                
                # Try to match patterns
                for pattern, target_pattern, relationship_type in patterns:
                    match = re.match(pattern, asset_name, re.IGNORECASE)
                    if match:
                        base_name = match.group(1)
                        asset_groups[(base_name, relationship_type)].append({
                            'asset_id': asset_id,
                            'asset': asset,
                            'match': match
                        })
            
            # Create relationships between matching patterns
            for (base_name, rel_type), assets in asset_groups.items():
                if len(assets) < 2:
                    continue
                
                # Find source and target assets
                source_assets = [a for a in assets if any(kw in a['asset'].name.lower() for kw in ['raw', 'staging', 'stg', 'input', 'source', 'backup', 'archive'])]
                target_assets = [a for a in assets if any(kw in a['asset'].name.lower() for kw in ['processed', 'final', 'output', 'target', 'result'])]
                
                if not source_assets or not target_assets:
                    # If no clear source/target, create relationships between all pairs
                    for i, source_asset_info in enumerate(assets):
                        for target_asset_info in assets[i+1:]:
                            lineage.append({
                                "source_asset_id": source_asset_info['asset_id'],
                                "target_asset_id": target_asset_info['asset_id'],
                                "relationship_type": rel_type,
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": None,
                                "transformation_type": rel_type,
                                "transformation_description": f"Naming pattern match: {base_name}",
                                "sql_query": None,
                                "source_system": "azure_blob",
                                "source_job_id": f"azure_pattern_{base_name.replace('/', '_')}",
                                "source_job_name": "Azure Naming Pattern Analysis",
                                "confidence_score": 0.75,
                                "extraction_method": "naming_pattern",
                                "discovered_at": datetime.utcnow()
                            })
                else:
                    # Create source -> target relationships
                    for source_asset_info in source_assets:
                        for target_asset_info in target_assets:
                            lineage.append({
                                "source_asset_id": source_asset_info['asset_id'],
                                "target_asset_id": target_asset_info['asset_id'],
                                "relationship_type": rel_type,
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": None,
                                "transformation_type": rel_type,
                                "transformation_description": f"Naming pattern match: {base_name}",
                                "sql_query": None,
                                "source_system": "azure_blob",
                                "source_job_id": f"azure_pattern_{base_name.replace('/', '_')}",
                                "source_job_name": "Azure Naming Pattern Analysis",
                                "confidence_score": 0.8,
                                "extraction_method": "naming_pattern",
                                "discovered_at": datetime.utcnow()
                            })
        
        except Exception as e:
            logger.error(f'FN:_extract_naming_pattern_lineage connector_id:{connector_id} error:{str(e)}')
        
        return lineage
    
    def _extract_schema_matching_lineage(
        self,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract relationships based on matching column schemas"""
        lineage = []
        
        if not ML_INFERENCE_AVAILABLE:
            return lineage
        
        try:
            # Get assets with columns
            assets_with_cols = []
            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue
                
                columns = asset.columns if hasattr(asset, 'columns') else []
                if columns and len(columns) > 0:
                    assets_with_cols.append({
                        'asset_id': asset_id,
                        'asset': asset,
                        'columns': columns
                    })
            
            # Compare each pair
            for i, asset1_info in enumerate(assets_with_cols):
                for asset2_info in assets_with_cols[i+1:]:
                    inferred_lineage, confidence = infer_relationships_ml(
                        asset1_info['columns'],
                        asset2_info['columns'],
                        min_matching_ratio=0.4
                    )
                    
                    if inferred_lineage and confidence >= 0.65:
                        lineage.append({
                            "source_asset_id": asset1_info['asset_id'],
                            "target_asset_id": asset2_info['asset_id'],
                            "relationship_type": "schema_match",
                            "source_type": "file",
                            "target_type": "file",
                            "column_lineage": inferred_lineage,
                            "transformation_type": "schema_match",
                            "transformation_description": f"Schema match: {len(inferred_lineage)} matching columns (confidence: {confidence:.2f})",
                            "sql_query": None,
                            "source_system": "azure_blob",
                            "source_job_id": f"azure_schema_{asset1_info['asset_id']}_{asset2_info['asset_id']}",
                            "source_job_name": "Azure Schema Matching",
                            "confidence_score": min(confidence, 0.85),
                            "extraction_method": "schema_matching",
                            "discovered_at": datetime.utcnow()
                        })
        
        except Exception as e:
            logger.error(f'FN:_extract_schema_matching_lineage connector_id:{connector_id} error:{str(e)}')
        
        return lineage
    
    def _extract_sql_reference_lineage(
        self,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract relationships from SQL queries that reference blob paths"""
        lineage = []
        
        try:
            # Look for SQL queries in asset metadata
            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue
                
                # Check technical_metadata for SQL queries
                sql_queries = []
                if hasattr(asset, 'technical_metadata') and asset.technical_metadata:
                    if isinstance(asset.technical_metadata, dict):
                        # Check various SQL fields
                        for sql_field in ['sql_query', 'view_definition', 'create_statement', 'ddl']:
                            sql_text = asset.technical_metadata.get(sql_field)
                            if sql_text:
                                sql_queries.append(sql_text)
                
                # Parse SQL to find blob path references
                for sql_query in sql_queries:
                    # Pattern: blob paths in SQL (various formats)
                    blob_patterns = [
                        r'["\']([^"\']*\.blob\.core\.windows\.net[^"\']*)["\']',
                        r'["\'](abfs://[^"\']*)["\']',
                        r'["\'](abfss://[^"\']*)["\']',
                        r'["\']([^"\']*container[^"\']*[/\\][^"\']*)["\']',
                    ]
                    
                    referenced_paths = set()
                    for pattern in blob_patterns:
                        matches = re.finditer(pattern, sql_query, re.IGNORECASE)
                        for match in matches:
                            referenced_paths.add(match.group(1))
                    
                    # Find assets matching referenced paths
                    for ref_path in referenced_paths:
                        for other_asset_id, other_asset in asset_map.items():
                            if other_asset_id == asset_id:
                                continue
                            
                            # Check if path matches
                            other_name = other_asset.name if hasattr(other_asset, 'name') else ''
                            other_path = ''
                            if hasattr(other_asset, 'operational_metadata') and other_asset.operational_metadata:
                                if isinstance(other_asset.operational_metadata, dict):
                                    other_path = other_asset.operational_metadata.get('path') or other_asset.operational_metadata.get('folder_path') or ''
                            
                            if ref_path in other_name or ref_path in other_path or other_name in ref_path or other_path in ref_path:
                                lineage.append({
                                    "source_asset_id": other_asset_id,
                                    "target_asset_id": asset_id,
                                    "relationship_type": "sql_reference",
                                    "source_type": "file",
                                    "target_type": "file",
                                    "column_lineage": None,
                                    "transformation_type": "sql_reference",
                                    "transformation_description": f"Referenced in SQL query",
                                    "sql_query": sql_query[:500],  # Limit length
                                    "source_system": "azure_blob",
                                    "source_job_id": f"azure_sql_ref_{asset_id}",
                                    "source_job_name": "Azure SQL Reference",
                                    "confidence_score": 0.85,
                                    "extraction_method": "sql_reference",
                                    "discovered_at": datetime.utcnow()
                                })
        
        except Exception as e:
            logger.error(f'FN:_extract_sql_reference_lineage connector_id:{connector_id} error:{str(e)}')
        
        return lineage
    
    def _extract_ml_inferred_lineage(
        self,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract relationships using ML-based inference"""
        lineage = []
        
        if not ML_INFERENCE_AVAILABLE:
            return lineage
        
        try:
            # Get all assets
            assets_list = [(asset_id, asset) for asset_id, asset in asset_map.items() 
                          if asset_id.startswith(connector_id)]
            
            # Compare each pair
            for i, (asset1_id, asset1) in enumerate(assets_list):
                asset1_name = asset1.name if hasattr(asset1, 'name') else asset1_id
                asset1_cols = asset1.columns if hasattr(asset1, 'columns') else []
                
                for asset2_id, asset2 in assets_list[i+1:]:
                    asset2_name = asset2.name if hasattr(asset2, 'name') else asset2_id
                    asset2_cols = asset2.columns if hasattr(asset2, 'columns') else []
                    
                    # Use ML inference if columns available
                    if asset1_cols and asset2_cols:
                        inferred_lineage, confidence = infer_relationships_ml(
                            asset1_cols,
                            asset2_cols,
                            min_matching_ratio=0.3
                        )
                        
                        if inferred_lineage and confidence >= 0.6:
                            lineage.append({
                                "source_asset_id": asset1_id,
                                "target_asset_id": asset2_id,
                                "relationship_type": "inferred",
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": inferred_lineage,
                                "transformation_type": "ml_inference",
                                "transformation_description": f"ML-inferred relationship (confidence: {confidence:.2f})",
                                "sql_query": None,
                                "source_system": "azure_blob",
                                "source_job_id": f"azure_ml_{asset1_id}_{asset2_id}",
                                "source_job_name": "Azure ML Inference",
                                "confidence_score": min(confidence, 0.8),
                                "extraction_method": "ml_inference",
                                "discovered_at": datetime.utcnow()
                            })
                    else:
                        # Fallback to name similarity
                        is_match, _ = fuzzy_column_match(asset1_name, asset2_name, threshold=0.7)
                        if is_match:
                            lineage.append({
                                "source_asset_id": asset1_id,
                                "target_asset_id": asset2_id,
                                "relationship_type": "inferred",
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": None,
                                "transformation_type": "name_similarity",
                                "transformation_description": f"Name similarity match",
                                "sql_query": None,
                                "source_system": "azure_blob",
                                "source_job_id": f"azure_name_sim_{asset1_id}_{asset2_id}",
                                "source_job_name": "Azure Name Similarity",
                                "confidence_score": 0.65,
                                "extraction_method": "name_similarity",
                                "discovered_at": datetime.utcnow()
                            })
        
        except Exception as e:
            logger.error(f'FN:_extract_ml_inferred_lineage connector_id:{connector_id} error:{str(e)}')
        
        return lineage
    
    def _extract_etl_pattern_lineage(
        self,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract relationships based on ETL job patterns"""
        lineage = []
        
        try:
            # Common ETL patterns in metadata
            etl_keywords = ['etl', 'pipeline', 'transform', 'process', 'job', 'workflow']
            
            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue
                
                # Check if asset has ETL-related metadata
                has_etl_metadata = False
                etl_job_name = None
                
                if hasattr(asset, 'operational_metadata') and asset.operational_metadata:
                    if isinstance(asset.operational_metadata, dict):
                        for key, value in asset.operational_metadata.items():
                            if any(kw in str(key).lower() for kw in etl_keywords) or \
                               any(kw in str(value).lower() for kw in etl_keywords):
                                has_etl_metadata = True
                                etl_job_name = value if isinstance(value, str) else key
                                break
                
                if has_etl_metadata:
                    # Find other assets with same ETL job name
                    for other_asset_id, other_asset in asset_map.items():
                        if other_asset_id == asset_id or not other_asset_id.startswith(connector_id):
                            continue
                        
                        if hasattr(other_asset, 'operational_metadata') and other_asset.operational_metadata:
                            if isinstance(other_asset.operational_metadata, dict):
                                for key, value in other_asset.operational_metadata.items():
                                    if etl_job_name and etl_job_name in str(value):
                                        # Determine direction based on naming or metadata
                                        asset_name = asset.name if hasattr(asset, 'name') else ''
                                        other_name = other_asset.name if hasattr(other_asset, 'name') else ''
                                        
                                        # Heuristic: input/output based on name
                                        is_input = any(kw in asset_name.lower() for kw in ['input', 'source', 'raw', 'staging'])
                                        is_output = any(kw in other_name.lower() for kw in ['output', 'target', 'final', 'processed'])
                                        
                                        if is_input and is_output:
                                            lineage.append({
                                                "source_asset_id": asset_id,
                                                "target_asset_id": other_asset_id,
                                                "relationship_type": "etl_pipeline",
                                                "source_type": "file",
                                                "target_type": "file",
                                                "column_lineage": None,
                                                "transformation_type": "etl_pipeline",
                                                "transformation_description": f"ETL pipeline: {etl_job_name}",
                                                "sql_query": None,
                                                "source_system": "azure_blob",
                                                "source_job_id": f"azure_etl_{etl_job_name}",
                                                "source_job_name": f"ETL Pipeline: {etl_job_name}",
                                                "confidence_score": 0.8,
                                                "extraction_method": "etl_pattern",
                                                "discovered_at": datetime.utcnow()
                                            })
        
        except Exception as e:
            logger.error(f'FN:_extract_etl_pattern_lineage connector_id:{connector_id} error:{str(e)}')
        
        return lineage
    
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

