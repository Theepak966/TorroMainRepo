"""
AWS S3 Lineage Extraction Service

Extracts data lineage for S3-discovered assets using:
1. Folder hierarchy relationships (bucket + key prefix)
2. ML-based inference (column matching, name similarity)
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from utils.ml_lineage_inference import infer_relationships_ml, fuzzy_column_match
    ML_INFERENCE_AVAILABLE = True
except ImportError:
    ML_INFERENCE_AVAILABLE = False
    logger.warning("ML inference not available for S3 lineage")


class S3LineageExtractor:
    """Extract lineage for AWS S3 assets (folder hierarchy + ML inference)."""

    def __init__(self) -> None:
        pass

    def _extract_folder_hierarchy_lineage(
        self,
        connector_id: str,
        asset_map: Dict,
    ) -> List[Dict]:
        """Extract folder hierarchy relationships (parent folder -> child folder) from S3 keys."""
        lineage: List[Dict] = []

        try:
            bucket_folders: Dict[str, Dict[str, List[Dict]]] = defaultdict(lambda: defaultdict(list))

            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue

                bucket = None
                key = None
                tech = getattr(asset, "technical_metadata", None) or {}
                if isinstance(tech, dict):
                    bucket = tech.get("bucket")
                    key = tech.get("key")
                if not bucket or not key:
                    bus = getattr(asset, "business_metadata", None) or {}
                    if isinstance(bus, dict):
                        bucket = bucket or bus.get("bucket")
                        key = key or bus.get("key")

                if not bucket or not key:
                    continue

                folder = "/".join(key.split("/")[:-1]) if "/" in key else ""
                file_name = key.split("/")[-1] if "/" in key else key

                bucket_folders[bucket][folder].append({
                    "asset_id": asset_id,
                    "asset": asset,
                    "file_name": file_name,
                })

            for bucket, folders in bucket_folders.items():
                for folder, infos in folders.items():
                    if not folder:
                        continue
                    parent_folder = "/".join(folder.split("/")[:-1]) if "/" in folder else ""
                    parent_infos = bucket_folders[bucket].get(parent_folder, [])

                    for pa in parent_infos:
                        for ca in infos:
                            lineage.append({
                                "source_asset_id": pa["asset_id"],
                                "target_asset_id": ca["asset_id"],
                                "relationship_type": "folder_hierarchy",
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": None,
                                "transformation_type": "folder_hierarchy",
                                "transformation_description": f"S3 folder: {parent_folder} -> {folder}",
                                "sql_query": None,
                                "source_system": "aws_s3",
                                "source_job_id": f"s3_folder_{bucket}_{folder.replace('/', '_')}",
                                "source_job_name": "S3 Folder Hierarchy",
                                "confidence_score": 0.7,
                                "extraction_method": "folder_hierarchy",
                                "discovered_at": datetime.utcnow(),
                            })
        except Exception as e:
            logger.error("FN:_extract_folder_hierarchy_lineage connector_id:%s error:%s", connector_id, str(e))

        return lineage

    def _extract_ml_inferred_lineage(
        self,
        connector_id: str,
        asset_map: Dict,
    ) -> List[Dict]:
        """Extract relationships using ML-based inference (columns or name similarity)."""
        lineage: List[Dict] = []

        if not ML_INFERENCE_AVAILABLE:
            return lineage

        try:
            assets_list = [
                (aid, a)
                for aid, a in asset_map.items()
                if aid.startswith(connector_id)
            ]

            for i, (asset1_id, asset1) in enumerate(assets_list):
                asset1_name = getattr(asset1, "name", None) or asset1_id
                asset1_cols = getattr(asset1, "columns", None) or []

                for asset2_id, asset2 in assets_list[i + 1:]:
                    asset2_name = getattr(asset2, "name", None) or asset2_id
                    asset2_cols = getattr(asset2, "columns", None) or []

                    if asset1_cols and asset2_cols:
                        inferred, confidence = infer_relationships_ml(
                            asset1_cols,
                            asset2_cols,
                            min_matching_ratio=0.3,
                        )
                        if inferred and confidence >= 0.6:
                            lineage.append({
                                "source_asset_id": asset1_id,
                                "target_asset_id": asset2_id,
                                "relationship_type": "inferred",
                                "source_type": "file",
                                "target_type": "file",
                                "column_lineage": inferred,
                                "transformation_type": "ml_inference",
                                "transformation_description": f"ML-inferred (confidence: {confidence:.2f})",
                                "sql_query": None,
                                "source_system": "aws_s3",
                                "source_job_id": f"s3_ml_{asset1_id}_{asset2_id}",
                                "source_job_name": "S3 ML Inference",
                                "confidence_score": min(confidence, 0.8),
                                "extraction_method": "ml_inference",
                                "discovered_at": datetime.utcnow(),
                            })
                    else:
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
                                "transformation_description": "Name similarity match",
                                "sql_query": None,
                                "source_system": "aws_s3",
                                "source_job_id": f"s3_name_sim_{asset1_id}_{asset2_id}",
                                "source_job_name": "S3 Name Similarity",
                                "confidence_score": 0.65,
                                "extraction_method": "name_similarity",
                                "discovered_at": datetime.utcnow(),
                            })
        except Exception as e:
            logger.error("FN:_extract_ml_inferred_lineage connector_id:%s error:%s", connector_id, str(e))

        return lineage
