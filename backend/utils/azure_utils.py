"""
Azure utilities availability check.
Production-level module for Azure availability flag.
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)

# Check Azure availability
try:
    from utils.azure_blob_client import AzureBlobClient
    from utils.metadata_extractor import extract_file_metadata, generate_file_hash, generate_schema_hash
    from utils.asset_deduplication import check_asset_exists, should_update_or_insert
    AZURE_AVAILABLE = True
    logger.info('FN:azure_utils message:Azure utilities loaded successfully')
except ImportError as e:
    logger.warning('FN:azure_utils message:Azure utilities not available error:{}'.format(str(e)))
    AZURE_AVAILABLE = False
    
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("azure_blob_client", os.path.join(os.path.dirname(__file__), "azure_blob_client.py"))
        if spec and spec.loader:
            azure_blob_client = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(azure_blob_client)
            AzureBlobClient = azure_blob_client.AzureBlobClient
            AZURE_AVAILABLE = True
            logger.info('FN:azure_utils message:Azure utilities loaded via importlib')
    except Exception as e2:
        logger.warning('FN:azure_utils message:Could not load Azure utilities via importlib error:{}'.format(str(e2)))

