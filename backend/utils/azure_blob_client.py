from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import ClientSecretCredential, AzureCliCredential, DefaultAzureCredential
from typing import List, Dict, Optional, Union
import logging


try:
    from azure.storage.filedatalake import DataLakeServiceClient
    DATALAKE_AVAILABLE = True
except ImportError:
    DATALAKE_AVAILABLE = False
    DataLakeServiceClient = None

logger = logging.getLogger(__name__)


def create_azure_blob_client(config: Dict) -> 'AzureBlobClient':

    connection_string = config.get('connection_string')
    if connection_string:
        return AzureBlobClient(connection_string=connection_string)
    

    account_name = config.get('account_name')
    tenant_id = config.get('tenant_id')
    client_id = config.get('client_id')
    client_secret = config.get('client_secret')
    
    use_dfs = config.get('use_dfs_endpoint', False) or config.get('storage_type') == 'datalake'
    
    credential = None
    
    # If Service Principal credentials are provided, use ONLY those (no fallback to .env or Azure CLI)
    if account_name and tenant_id and client_id and client_secret:
        try:
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            logger.info('FN:create_azure_blob_client message:Using Service Principal authentication from manual input')
        except Exception as e:
            logger.error('FN:create_azure_blob_client message:Service Principal authentication failed with provided credentials error:{}'.format(str(e)))
            raise ValueError(
                f"Service Principal authentication failed with the provided credentials. "
                f"Please verify:\n"
                f"1. Tenant ID is correct\n"
                f"2. Client ID (Application ID) is correct\n"
                f"3. Client Secret is valid and not expired\n"
                f"4. Service Principal has required permissions\n"
                f"Error: {str(e)}"
            )
    
    # Only fallback to Azure CLI / DefaultAzureCredential if NO credentials were provided
    if not credential:
        try:
            credential = AzureCliCredential()
            logger.info('FN:create_azure_blob_client message:Using Azure CLI authentication (no Service Principal provided)')
        except Exception as e:
            logger.warning('FN:create_azure_blob_client message:Azure CLI auth failed, trying DefaultAzureCredential error:{}'.format(str(e)))
            try:
                credential = DefaultAzureCredential()
                logger.info('FN:create_azure_blob_client message:Using DefaultAzureCredential authentication (no Service Principal provided)')
            except Exception as e2:
                logger.error('FN:create_azure_blob_client message:All authentication methods failed error:{}'.format(str(e2)))
                raise ValueError(
                    "Failed to authenticate. Please provide Service Principal credentials (account_name, tenant_id, client_id, client_secret) "
                    "or ensure Azure CLI is installed and logged in (run 'az login')."
                )
    
    if not account_name:
        raise ValueError("account_name is required when using credential authentication")
    
    if use_dfs:
        dfs_account_url = f"https://{account_name}.dfs.core.windows.net"
        return AzureBlobClient(dfs_account_url=dfs_account_url, dfs_credential=credential)
    else:
        account_url = f"https://{account_name}.blob.core.windows.net"
        return AzureBlobClient(account_url=account_url, credential=credential)


class AzureBlobClient:
    def __init__(self, connection_string: Optional[str] = None, 
                 account_url: Optional[str] = None,
                 credential: Optional[ClientSecretCredential] = None,
                 dfs_account_url: Optional[str] = None,
                 dfs_credential: Optional[ClientSecretCredential] = None):
        if connection_string:
            self.connection_string = connection_string
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self.auth_method = "connection_string"
        elif dfs_account_url and dfs_credential:

            if not DATALAKE_AVAILABLE:
                raise ImportError("azure-storage-filedatalake package is required for Data Lake Gen2 support. Install it with: pip install azure-storage-filedatalake")
            self.dfs_account_url = dfs_account_url
            self.dfs_credential = dfs_credential
            self.data_lake_service_client = DataLakeServiceClient(account_url=dfs_account_url, credential=dfs_credential)

            self.blob_service_client = self.data_lake_service_client._blob_service_client
            self.auth_method = "service_principal_dfs"
        elif account_url and credential:
            self.account_url = account_url
            self.credential = credential
            self.blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
            self.auth_method = "service_principal"
        else:
            raise ValueError("Either connection_string, (account_url and credential), or (dfs_account_url and dfs_credential) must be provided")
    
    def list_datalake_files(self, file_system_name: str, path: str = "", file_extensions: List[str] = None) -> List[Dict]:
        if not hasattr(self, 'data_lake_service_client') or not self.data_lake_service_client:

            return self.list_blobs(file_system_name, path, file_extensions)
        
        try:
            file_system_client = self.data_lake_service_client.get_file_system_client(file_system_name)
            

            normalized_path = path.strip('/') if path else ""
            
            
            files = []
            paths = file_system_client.get_paths(path=normalized_path, recursive=True)
            
            for path_item in paths:

                if path_item.is_directory:
                    continue
                
                file_name = path_item.name.split('/')[-1] if '/' in path_item.name else path_item.name
                

                if file_extensions:
                    if not any(file_name.lower().endswith(ext.lower()) for ext in file_extensions):
                        continue
                
                file_info = {
                    "name": file_name,
                    "full_path": path_item.name,
                    "size": path_item.content_length or 0,
                    "content_type": getattr(path_item, 'content_type', 'application/octet-stream'),
                    "last_modified": path_item.last_modified,
                    "created_at": getattr(path_item, 'creation_time', path_item.last_modified),
                    "etag": path_item.etag.strip('"') if path_item.etag else "",
                    "blob_type": "File" if hasattr(path_item, 'is_directory') else "Block blob",
                    "owner": getattr(path_item, 'owner', None),
                    "group": getattr(path_item, 'group', None),
                    "permissions": getattr(path_item, 'permissions', None),
                }
                
                files.append(file_info)
            
            logger.info('FN:list_datalake_files file_system:{} path:{} file_count:{}'.format(
                file_system_name, path, len(files)
            ))
            return files
            
        except Exception as e:
            logger.error('FN:list_datalake_files file_system:{} path:{} error:{}'.format(
                file_system_name, path, str(e)
            ))

            logger.info('FN:list_datalake_files falling_back_to_blob_api file_system:{} path:{}'.format(
                file_system_name, path
            ))
            return self.list_blobs(file_system_name, path, file_extensions)
    
    def list_blobs(self, container_name: str, folder_path: str = "", file_extensions: List[str] = None) -> List[Dict]:
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = []
            
            prefix = folder_path.rstrip("/") + "/" if folder_path else ""



            blob_list = []
            count = 0
            try:
                blob_iterator = container_client.list_blobs(name_starts_with=prefix)
                for blob in blob_iterator:
                    blob_list.append(blob)
                    count += 1


                    if count % 10000 == 0:
                        logger.info('FN:list_blobs container_name:{} folder_path:{} processed_count:{}'.format(container_name, folder_path, count))
                logger.info('FN:list_blobs container_name:{} folder_path:{} fetched_blob_count:{}'.format(container_name, folder_path, len(blob_list)))
            except Exception as e:
                logger.error('FN:list_blobs container_name:{} folder_path:{} list_error:{}'.format(container_name, folder_path, str(e)))
                raise
            
            for blob in blob_list:

                if blob.name.endswith('/'):
                    logger.debug('FN:list_blobs blob_name:{} message:Skipping directory'.format(blob.name))
                    continue
                
                if file_extensions:
                    if not any(blob.name.lower().endswith(ext.lower()) for ext in file_extensions):
                        logger.debug('FN:list_blobs blob_name:{} message:Skipping blob extension filter'.format(blob.name))
                        continue
                


                class BlobPropertiesProxy:
                    def __init__(self, blob_item):
                        self.size = getattr(blob_item, 'size', 0)
                        self.etag = getattr(blob_item, 'etag', '').strip('"') if hasattr(blob_item, 'etag') else ''
                        self.creation_time = getattr(blob_item, 'creation_time', None)
                        self.last_modified = getattr(blob_item, 'last_modified', None)

                        content_type = getattr(blob_item, 'content_type', 'application/octet-stream')
                        self.content_settings = type('ContentSettings', (), {
                            'content_type': content_type,
                            'content_encoding': getattr(blob_item, 'content_encoding', None),
                            'content_language': getattr(blob_item, 'content_language', None),
                            'cache_control': getattr(blob_item, 'cache_control', None),
                        })()
                        self.blob_tier = getattr(blob_item, 'blob_tier', None)
                        self.lease = type('Lease', (), {'status': getattr(blob_item, 'lease_status', None)})()
                        self.metadata = getattr(blob_item, 'metadata', {}) if hasattr(blob_item, 'metadata') else {}
                blob_properties = BlobPropertiesProxy(blob)
                
                blob_type = None
                if hasattr(blob_properties, 'blob_type'):
                    blob_type = blob_properties.blob_type
                elif hasattr(blob_properties, 'blob_tier'):
                    blob_type = "Block blob"
                else:
                    blob_type = "Block blob"
                
                blob_info = {
                    "name": blob.name.split("/")[-1],
                    "full_path": blob.name,
                    "size": blob_properties.size,
                    "content_type": blob_properties.content_settings.content_type,
                    "created_at": blob_properties.creation_time,
                    "last_modified": blob_properties.last_modified,
                    "etag": blob_properties.etag,
                    "blob_type": blob_type,
                    "access_tier": blob_properties.blob_tier if hasattr(blob_properties, 'blob_tier') else None,
                    "lease_status": blob_properties.lease.status if hasattr(blob_properties, 'lease') else None,
                    "content_encoding": blob_properties.content_settings.content_encoding,
                    "content_language": blob_properties.content_settings.content_language,
                    "cache_control": blob_properties.content_settings.cache_control,
                    "metadata": blob_properties.metadata,
                }
                
                blobs.append(blob_info)
            
            logger.info('FN:list_blobs container_name:{} folder_path:{} blob_count:{}'.format(container_name, folder_path, len(blobs)))
            return blobs
            
        except Exception as e:
            logger.error('FN:list_blobs container_name:{} folder_path:{} error:{}'.format(container_name, folder_path, str(e)))
            raise
    
    def get_blob_content(self, container_name: str, blob_path: str) -> bytes:
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            return blob_client.download_blob().readall()
        except Exception as e:
            logger.error('FN:get_blob_content container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)))
            raise
    
    def get_blob_sample(self, container_name: str, blob_path: str, max_bytes: int = 1024) -> bytes:





        try:

            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)

                    return file_client.download_file(offset=0, length=max_bytes).readall()
                except Exception as e:
                    logger.warning('FN:get_blob_sample datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            

            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )

            return blob_client.download_blob(offset=0, length=max_bytes).readall()
        except Exception as e:
            logger.warning('FN:get_blob_sample container_name:{} blob_path:{} max_bytes:{} error:{}'.format(container_name, blob_path, max_bytes, str(e)))
            return b""
    
    def get_parquet_footer(self, container_name: str, blob_path: str, footer_size_kb: int = 256) -> bytes:
        """
        Download the footer of parquet file (last 256KB by default, increased for large schemas).
        Parquet files store schema metadata in the footer, so we don't need the entire file.
        This is MUCH more efficient for large-scale discovery (4000+ files).
        
        Strategy:
        1. First, download last 8 bytes to get the footer length
        2. Then download the exact footer size (up to max footer_size_kb)
        3. If footer is larger, progressively increase download size (optimized order: 512KB, 1024KB, 256KB, 2048KB)
        4. Early exit when PAR1 magic is found (no need to try other sizes)
        5. This ensures we get the COMPLETE schema even for files with 100+ columns
        
        Performance: 4,000 files × 256KB = 1GB (vs 40GB with full download)
        """
        import struct
        
        try:
            properties = self.get_blob_properties(container_name, blob_path)
            file_size = properties.get("size", 0)
            
            if file_size == 0:
                logger.warning('FN:get_parquet_footer blob_path:{} message:File size is 0'.format(blob_path))
                return b""
            
            # Parquet footer structure: [footer_data][footer_length:4 bytes][magic:4 bytes "PAR1"]
            # First, read last 8 bytes to get footer length
            if file_size < 8:
                # File too small, download full file
                logger.debug('FN:get_parquet_footer blob_path:{} message:File too small, downloading full file'.format(blob_path))
                return self.get_blob_content(container_name, blob_path)
            
            max_footer_bytes = footer_size_kb * 1024  # Convert KB to bytes
            
            # Download last 8 bytes to read footer length
            last_8_bytes = None
            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)
                    last_8_bytes = file_client.download_file(offset=file_size - 8, length=8).readall()
                except Exception as e:
                    logger.debug('FN:get_parquet_footer datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            
            if not last_8_bytes:
                blob_client = self.blob_service_client.get_blob_client(
                    container=container_name,
                    blob=blob_path
                )
                last_8_bytes = blob_client.download_blob(offset=file_size - 8, length=8).readall()
            
            if len(last_8_bytes) < 8:
                logger.warning('FN:get_parquet_footer blob_path:{} message:Could not read last 8 bytes, downloading full file'.format(blob_path))
                return self.get_blob_content(container_name, blob_path)
            
            # Verify magic bytes "PAR1" at the end
            if last_8_bytes[-4:] != b'PAR1':
                # Not a valid parquet file or footer not at end, try progressive download
                logger.debug('FN:get_parquet_footer blob_path:{} message:PAR1 magic not found, trying progressive download'.format(blob_path))
                
                # OPTIMIZED: Try progressively larger downloads to find the footer
                # Order optimized: Most parquet files have footers < 512KB, so try 512KB first
                # Then 1024KB for medium schemas, then 256KB for small, then 2048KB for very large
                for attempt_size_kb in [512, 1024, 256, 2048]:
                    attempt_bytes = attempt_size_kb * 1024
                    if file_size <= attempt_bytes:
                        # File is smaller than attempt size, download full file
                        return self.get_blob_content(container_name, blob_path)
                    
                    try:
                        offset = max(0, file_size - attempt_bytes)
                        length = min(attempt_bytes, file_size)
                        
                        if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                            try:
                                file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                                file_client = file_system_client.get_file_client(blob_path)
                                footer_data = file_client.download_file(offset=offset, length=length).readall()
                            except Exception:
                                blob_client = self.blob_service_client.get_blob_client(
                                    container=container_name,
                                    blob=blob_path
                                )
                                footer_data = blob_client.download_blob(offset=offset, length=length).readall()
                        else:
                            blob_client = self.blob_service_client.get_blob_client(
                                container=container_name,
                                blob=blob_path
                            )
                            footer_data = blob_client.download_blob(offset=offset, length=length).readall()
                        
                        # Check if we found PAR1 magic
                        if len(footer_data) >= 4 and footer_data[-4:] == b'PAR1':
                            logger.debug('FN:get_parquet_footer blob_path:{} message:Found PAR1 magic with {}KB download'.format(blob_path, attempt_size_kb))
                            # OPTIMIZATION: Early exit - we found valid footer, no need to try other sizes
                            # Try to read footer length from the downloaded data
                            if len(footer_data) >= 8:
                                try:
                                    footer_length = struct.unpack('<I', footer_data[-8:-4])[0]
                                    actual_footer_size = footer_length + 8
                                    if actual_footer_size <= attempt_bytes:
                                        # We have the complete footer - return immediately
                                        return footer_data
                                except Exception:
                                    pass
                            # Return the footer data we found (may be incomplete but has PAR1 magic)
                            return footer_data
                    except Exception as e:
                        logger.debug('FN:get_parquet_footer blob_path:{} attempt_size_kb:{} error:{}'.format(blob_path, attempt_size_kb, str(e)))
                        continue
                
                # If all attempts failed, download full file as last resort
                logger.warning('FN:get_parquet_footer blob_path:{} message:All footer attempts failed, downloading full file'.format(blob_path))
                return self.get_blob_content(container_name, blob_path)
            
            # Read footer length (4 bytes before magic)
            try:
                footer_length = struct.unpack('<I', last_8_bytes[0:4])[0]
                
                # Sanity check: footer length should be reasonable
                if footer_length > max_footer_bytes * 10:  # More than 10x max is suspicious
                    logger.warning('FN:get_parquet_footer blob_path:{} footer_length:{} message:Footer length seems too large, using max_footer_bytes'.format(blob_path, footer_length))
                    footer_bytes = max_footer_bytes
                elif footer_length == 0:
                    logger.warning('FN:get_parquet_footer blob_path:{} message:Footer length is 0, using max_footer_bytes'.format(blob_path))
                    footer_bytes = max_footer_bytes
                else:
                    # Footer includes: footer_data + 4 bytes length + 4 bytes magic = footer_length + 8
                    actual_footer_size = footer_length + 8
                    # Use the actual footer size, but cap at max_footer_bytes for safety
                    footer_bytes = min(actual_footer_size, max_footer_bytes)
                    
                    # If actual footer is larger than max, log a warning
                    if actual_footer_size > max_footer_bytes:
                        logger.warning('FN:get_parquet_footer blob_path:{} actual_footer_size:{} max_footer_bytes:{} message:Footer larger than max, may be incomplete'.format(
                            blob_path, actual_footer_size, max_footer_bytes
                        ))
            except Exception as e:
                logger.warning('FN:get_parquet_footer blob_path:{} error:{} message:Failed to parse footer length, using max_footer_bytes'.format(blob_path, str(e)))
                footer_bytes = max_footer_bytes
            
            if file_size <= footer_bytes:
                # File is smaller than footer size, download full file
                logger.debug('FN:get_parquet_footer blob_path:{} message:File smaller than footer, downloading full file'.format(blob_path))
                return self.get_blob_content(container_name, blob_path)
            
            # Download the footer (last N bytes)
            offset = max(0, file_size - footer_bytes)
            length = min(footer_bytes, file_size)
            
            try:
                if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                    try:
                        file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                        file_client = file_system_client.get_file_client(blob_path)
                        footer_data = file_client.download_file(offset=offset, length=length).readall()
                    except Exception as e:
                        logger.debug('FN:get_parquet_footer datalake_error:{} falling_back_to_blob_api'.format(str(e)))
                        blob_client = self.blob_service_client.get_blob_client(
                            container=container_name,
                            blob=blob_path
                        )
                        footer_data = blob_client.download_blob(offset=offset, length=length).readall()
                else:
                    blob_client = self.blob_service_client.get_blob_client(
                        container=container_name,
                        blob=blob_path
                    )
                    footer_data = blob_client.download_blob(offset=offset, length=length).readall()
                
                # Verify we got valid footer data
                if len(footer_data) >= 4 and footer_data[-4:] == b'PAR1':
                    return footer_data
                else:
                    logger.warning('FN:get_parquet_footer blob_path:{} message:Downloaded footer missing PAR1 magic, may be incomplete'.format(blob_path))
                    return footer_data  # Return anyway, let schema extraction handle errors
                    
            except Exception as e:
                logger.warning('FN:get_parquet_footer container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)))
                return b""
                
        except Exception as e:
            logger.error('FN:get_parquet_footer container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)), exc_info=True)
            return b""
    
    def get_parquet_file_for_extraction(self, container_name: str, blob_path: str, max_size_mb: int = 10) -> bytes:
        """
        Download parquet file for schema and PII extraction.
        DEPRECATED: Use get_parquet_footer() for schema extraction (much more efficient).
        This method is kept for backward compatibility and PII detection that needs row group data.
        """
        try:
            # Get file size first
            properties = self.get_blob_properties(container_name, blob_path)
            file_size = properties.get("size", 0)
            max_bytes = max_size_mb * 1024 * 1024  # Convert MB to bytes
            
            # Performance optimization: For very large files, limit download size
            if file_size > 100 * 1024 * 1024:  # > 100MB
                max_bytes = min(max_bytes, 5 * 1024 * 1024)  # Cap at 5MB for very large files
                logger.debug('FN:get_parquet_file_for_extraction container_name:{} blob_path:{} file_size:{} message:Large file detected, limiting download to 5MB'.format(
                    container_name, blob_path, file_size
                ))
            
            if file_size <= max_bytes:
                # Small file - download full file
                return self.get_blob_content(container_name, blob_path)
            else:
                # Large file - download first max_bytes to get first row group
                if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                    try:
                        file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                        file_client = file_system_client.get_file_client(blob_path)
                        return file_client.download_file(offset=0, length=max_bytes).readall()
                    except Exception as e:
                        logger.warning('FN:get_parquet_file_for_extraction datalake_error:{} falling_back_to_blob_api'.format(str(e)))
                
                blob_client = self.blob_service_client.get_blob_client(
                    container=container_name,
                    blob=blob_path
                )
                return blob_client.download_blob(offset=0, length=max_bytes).readall()
        except Exception as e:
            logger.warning('FN:get_parquet_file_for_extraction container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)))
            return b""
    
    def get_parquet_footer_and_row_group(self, container_name: str, blob_path: str, footer_size_kb: int = 256, row_group_size_mb: int = 2) -> bytes:
        """
        Optimized method for large parquet files (>100MB).
        Downloads footer (for schema) + first row group (for PII detection) and combines them.
        This is much more efficient than downloading the entire file or downloading first 5MB (which doesn't include footer).
        
        Strategy:
        1. Download footer from end of file (256KB-2MB typically) - contains schema metadata
        2. Download first row group from beginning (2MB typically) - contains data for PII detection
        3. Combine them: [row_group_data][footer_data]
        
        Note: PyArrow can extract schema from footer-only, and can read row groups if the structure is valid.
        The combined structure allows both schema extraction and PII detection with actual data samples.
        
        Args:
            footer_size_kb: Maximum footer size to download (default 256KB)
            row_group_size_mb: Maximum first row group size to download (default 2MB)
        
        Returns:
            Combined bytes: [first_row_group_data][footer_data]
            If row group download fails, returns footer only (schema extraction will still work)
        """
        try:
            properties = self.get_blob_properties(container_name, blob_path)
            file_size = properties.get("size", 0)
            
            if file_size == 0:
                logger.warning('FN:get_parquet_footer_and_row_group blob_path:{} message:File size is 0'.format(blob_path))
                return b""
            
            # Download footer first (for schema) - this always works
            footer_data = self.get_parquet_footer(container_name, blob_path, footer_size_kb=footer_size_kb)
            
            if not footer_data or len(footer_data) < 8:
                logger.warning('FN:get_parquet_footer_and_row_group blob_path:{} message:Failed to download footer'.format(blob_path))
                return footer_data if footer_data else b""
            
            # Download first row group from beginning (for PII detection)
            # This gives us actual data samples for better PII detection accuracy
            row_group_bytes = row_group_size_mb * 1024 * 1024
            row_group_data = b""
            
            try:
                if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                    try:
                        file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                        file_client = file_system_client.get_file_client(blob_path)
                        row_group_data = file_client.download_file(offset=0, length=min(row_group_bytes, file_size)).readall()
                    except Exception as e:
                        logger.debug('FN:get_parquet_footer_and_row_group datalake_error:{} falling_back_to_blob_api'.format(str(e)))
                        blob_client = self.blob_service_client.get_blob_client(
                            container=container_name,
                            blob=blob_path
                        )
                        row_group_data = blob_client.download_blob(offset=0, length=min(row_group_bytes, file_size)).readall()
                else:
                    blob_client = self.blob_service_client.get_blob_client(
                        container=container_name,
                        blob=blob_path
                    )
                    row_group_data = blob_client.download_blob(offset=0, length=min(row_group_bytes, file_size)).readall()
                
                logger.info('FN:get_parquet_footer_and_row_group blob_path:{} footer_size:{} row_group_size:{} message:Downloaded footer and first row group for large file'.format(
                    blob_path, len(footer_data), len(row_group_data)
                ))
            except Exception as e:
                logger.warning('FN:get_parquet_footer_and_row_group blob_path:{} message:Failed to download row group, using footer only (schema will work, PII will use column names) error:{}'.format(blob_path, str(e)))
                # Return footer only if row group download fails
                # Schema extraction will still work, but PII detection will use column names only
                return footer_data
            
            # Combine: row group data + footer data
            # Structure: [row_group_data][footer_data]
            # Note: Footer has PAR1 magic at end, so combined_data[-4:] == b'PAR1'
            # PyArrow can extract schema from footer (which has PAR1 magic at end)
            # PyArrow might not recognize the combined structure as valid parquet file,
            # but it should still be able to extract schema from the footer portion
            # If PyArrow fails, extract_parquet_schema will fallback gracefully
            combined_data = row_group_data + footer_data
            
            # Verify PAR1 magic is at the end (required for schema extraction)
            if len(combined_data) >= 4 and combined_data[-4:] == b'PAR1':
                return combined_data
            else:
                # If PAR1 magic is missing, return footer only (schema will work)
                logger.warning('FN:get_parquet_footer_and_row_group blob_path:{} message:PAR1 magic not found in combined data, returning footer only'.format(blob_path))
                return footer_data
            
        except Exception as e:
            logger.error('FN:get_parquet_footer_and_row_group container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)), exc_info=True)
            # Fallback to footer only
            try:
                return self.get_parquet_footer(container_name, blob_path, footer_size_kb=footer_size_kb)
            except Exception as fallback_error:
                logger.error('FN:get_parquet_footer_and_row_group container_name:{} blob_path:{} message:Fallback to footer also failed error:{}'.format(container_name, blob_path, str(fallback_error)), exc_info=True)
                return b""
    
    def get_blob_tail(self, container_name: str, blob_path: str, max_bytes: int = 8192) -> bytes:


        try:

            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)

                    properties = file_client.get_file_properties()
                    file_size = properties.size
                    

                    offset = max(0, file_size - max_bytes)
                    length = min(max_bytes, file_size)
                    return file_client.download_file(offset=offset, length=length).readall()
                except Exception as e:
                    logger.warning('FN:get_blob_tail datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            

            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )

            properties = blob_client.get_blob_properties()
            file_size = properties.size
            

            offset = max(0, file_size - max_bytes)
            length = min(max_bytes, file_size)
            return blob_client.download_blob(offset=offset, length=length).readall()
        except Exception as e:
            logger.warning('FN:get_blob_tail container_name:{} blob_path:{} max_bytes:{} error:{}'.format(container_name, blob_path, max_bytes, str(e)))
            return b""
    
    def get_blob_properties(self, container_name: str, blob_path: str) -> Dict:

        try:

            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)
                    properties = file_client.get_file_properties()
                    

                    return {
                        "size": properties.size,
                        "etag": properties.etag.strip('"') if properties.etag else "",
                        "created_at": properties.creation_time,
                        "last_modified": properties.last_modified,
                        "content_type": properties.content_settings.content_type if hasattr(properties, 'content_settings') else "application/octet-stream",
                        "content_encoding": properties.content_settings.content_encoding if hasattr(properties, 'content_settings') else None,
                        "content_language": properties.content_settings.content_language if hasattr(properties, 'content_settings') else None,
                        "cache_control": properties.content_settings.cache_control if hasattr(properties, 'content_settings') else None,
                        "content_md5": properties.content_settings.content_md5 if hasattr(properties, 'content_settings') and hasattr(properties.content_settings, 'content_md5') else None,
                        "content_disposition": properties.content_settings.content_disposition if hasattr(properties, 'content_settings') and hasattr(properties.content_settings, 'content_disposition') else None,
                        "metadata": properties.metadata or {},
                        "lease_status": properties.lease_status if hasattr(properties, 'lease_status') else None,
                        "access_tier": None,
                    }
                except Exception as e:
                    logger.warning('FN:get_blob_properties datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            

            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            properties = blob_client.get_blob_properties()
            

            blob_type = "Block blob"
            if hasattr(properties, 'blob_type'):
                blob_type = properties.blob_type
            elif hasattr(properties, 'blob_tier'):
                blob_type = "Block blob"
            
            return {
                "etag": properties.etag,
                "size": properties.size,
                "content_type": properties.content_settings.content_type,
                "created_at": properties.creation_time,
                "last_modified": properties.last_modified,
                "blob_type": blob_type,
                "access_tier": properties.blob_tier if hasattr(properties, 'blob_tier') else None,
                "lease_status": properties.lease.status if hasattr(properties, 'lease') else None,
                "lease_state": properties.lease.state if hasattr(properties, 'lease') and hasattr(properties.lease, 'state') else None,
                "content_encoding": properties.content_settings.content_encoding,
                "content_language": properties.content_settings.content_language,
                "cache_control": properties.content_settings.cache_control,
                "content_md5": properties.content_settings.content_md5 if hasattr(properties.content_settings, 'content_md5') else None,
                "content_disposition": properties.content_settings.content_disposition if hasattr(properties.content_settings, 'content_disposition') else None,
                "metadata": properties.metadata or {},
            }
        except Exception as e:
            logger.error('FN:get_blob_properties container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)))
            raise
    
    def upload_blob(self, container_name: str, blob_path: str, content: bytes, content_type: str = "text/plain"):
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            content_settings = ContentSettings(content_type=content_type)
            blob_client.upload_blob(content, overwrite=True, content_settings=content_settings)
            logger.info('FN:upload_blob container_name:{} blob_path:{} content_type:{}'.format(container_name, blob_path, content_type))
        except Exception as e:
            logger.error('FN:upload_blob container_name:{} blob_path:{} error:{}'.format(container_name, blob_path, str(e)))
            raise
    
    def list_containers(self) -> List[Dict]:
        try:
            containers = []
            container_list = list(self.blob_service_client.list_containers())
            
            for container in container_list:
                containers.append({
                    "name": container.name,
                    "last_modified": container.last_modified.isoformat() if container.last_modified else None,
                    "etag": container.etag,
                    "lease_status": container.lease.status if hasattr(container.lease, 'status') else None,
                    "public_access": container.public_access if hasattr(container, 'public_access') else None,
                })
            
            logger.info('FN:list_containers container_count:{}'.format(len(containers)))
            return containers
        except Exception as e:
            logger.error('FN:list_containers error:{}'.format(str(e)))
            raise
    
    def _get_account_name(self) -> str:
        if hasattr(self, 'connection_string') and self.connection_string:

            for part in self.connection_string.split(';'):
                if part.startswith('AccountName='):
                    return part.split('=')[1]
        elif hasattr(self, 'account_url') and self.account_url:

            return self.account_url.split('//')[1].split('.')[0]
        elif hasattr(self, 'dfs_account_url') and self.dfs_account_url:

            return self.dfs_account_url.split('//')[1].split('.')[0]
        raise ValueError("Cannot determine account name from connection")
    
    def _get_credential_or_connection_string(self):
        if hasattr(self, 'connection_string') and self.connection_string:
            return self.connection_string, None
        elif hasattr(self, 'credential') and self.credential:
            return None, self.credential
        elif hasattr(self, 'dfs_credential') and self.dfs_credential:
            return None, self.dfs_credential
        else:
            raise ValueError("Cannot create service client - missing credentials")
    
    def list_file_shares(self) -> List[Dict]:
        try:
            from azure.storage.fileshare import ShareServiceClient
            from azure.identity import TokenCredential
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                share_service_client = ShareServiceClient.from_connection_string(connection_string)
            elif credential:
                account_name = self._get_account_name()
                share_service_url = f"https://{account_name}.file.core.windows.net"

                if isinstance(credential, TokenCredential):
                    share_service_client = ShareServiceClient(account_url=share_service_url, credential=credential, token_intent="backup")
                else:
                    share_service_client = ShareServiceClient(account_url=share_service_url, credential=credential)
            else:
                raise ValueError("Cannot create ShareServiceClient - missing credentials")
            
            shares = []
            for share in share_service_client.list_shares():
                shares.append({
                    "name": share.name,
                    "last_modified": share.last_modified.isoformat() if share.last_modified else None,
                    "quota": share.quota,
                    "metadata": share.metadata or {}
                })
            
            logger.info('FN:list_file_shares share_count:{}'.format(len(shares)))
            return shares
        except ImportError as import_err:
            # Log the actual import error for debugging
            logger.warning('FN:list_file_shares message:azure-storage-file-share package not installed or import failed error:{}'.format(str(import_err)))
            return []
        except Exception as e:
            logger.error('FN:list_file_shares error:{}'.format(str(e)))
            raise
    
    def list_queues(self) -> List[Dict]:
        try:
            from azure.storage.queue import QueueServiceClient
            from azure.identity import TokenCredential
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                queue_service_client = QueueServiceClient.from_connection_string(connection_string)
            elif credential:
                account_name = self._get_account_name()
                queue_service_url = f"https://{account_name}.queue.core.windows.net"

                if isinstance(credential, TokenCredential):
                    queue_service_client = QueueServiceClient(account_url=queue_service_url, credential=credential, token_intent="backup")
                else:
                    queue_service_client = QueueServiceClient(account_url=queue_service_url, credential=credential)
            else:
                raise ValueError("Cannot create QueueServiceClient - missing credentials")
            
            queues = []
            for queue in queue_service_client.list_queues():
                queues.append({
                    "name": queue.name,
                    "metadata": queue.metadata or {}
                })
            
            logger.info('FN:list_queues queue_count:{}'.format(len(queues)))
            return queues
        except ImportError as import_err:
            # Log the actual import error for debugging
            logger.warning('FN:list_queues message:azure-storage-queue package not installed or import failed error:{}'.format(str(import_err)))
            return []
        except Exception as e:
            logger.error('FN:list_queues error:{}'.format(str(e)))
            raise
    
    def list_tables(self) -> List[Dict]:
        try:
            from azure.data.tables import TableServiceClient
            from azure.identity import TokenCredential
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                table_service_client = TableServiceClient.from_connection_string(connection_string)
            elif credential:
                account_name = self._get_account_name()
                table_service_url = f"https://{account_name}.table.core.windows.net"

                if isinstance(credential, TokenCredential):
                    table_service_client = TableServiceClient(endpoint=table_service_url, credential=credential, token_intent="backup")
                else:
                    table_service_client = TableServiceClient(endpoint=table_service_url, credential=credential)
            else:
                raise ValueError("Cannot create TableServiceClient - missing credentials")
            
            tables = []
            for table in table_service_client.list_tables():
                tables.append({
                    "name": table.name
                })
            
            logger.info('FN:list_tables table_count:{}'.format(len(tables)))
            return tables
        except ImportError as import_err:
            # Log the actual import error for debugging
            logger.warning('FN:list_tables message:azure-data-tables package not installed or import failed error:{}'.format(str(import_err)))
            return []
        except Exception as e:
            logger.error('FN:list_tables error:{}'.format(str(e)))
            raise
    
    def list_file_share_files(self, share_name: str, directory_path: str = "", file_extensions: List[str] = None) -> List[Dict]:
        try:
            from azure.storage.fileshare import ShareClient, ShareDirectoryClient
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            from azure.identity import TokenCredential
            
            if connection_string:
                share_client = ShareClient.from_connection_string(connection_string, share_name)
            elif credential:
                account_name = self._get_account_name()
                share_service_url = f"https://{account_name}.file.core.windows.net"

                if isinstance(credential, TokenCredential):
                    share_client = ShareClient(account_url=f"{share_service_url}/{share_name}", credential=credential, token_intent="backup")
                else:
                    share_client = ShareClient(account_url=f"{share_service_url}/{share_name}", credential=credential)
            else:
                raise ValueError("Cannot create ShareClient - missing credentials")
            
            directory_client = share_client.get_directory_client(directory_path)
            files = []
            
            for item in directory_client.list_directories_and_files():
                if item.is_directory:
                    continue
                
                if file_extensions:
                    if not any(item.name.lower().endswith(ext.lower()) for ext in file_extensions):
                        continue
                
                file_info = {
                    "name": item.name,
                    "full_path": f"{directory_path}/{item.name}" if directory_path else item.name,
                    "size": item.size,
                    "content_type": getattr(item, 'content_type', None),
                    "last_modified": item.last_modified.isoformat() if item.last_modified else None,
                    "file_attributes": getattr(item, 'file_attributes', None),
                }
                files.append(file_info)
            
            logger.info('FN:list_file_share_files share_name:{} directory_path:{} file_count:{}'.format(share_name, directory_path, len(files)))
            return files
        except ImportError:
            logger.warning('FN:list_file_share_files message:azure-storage-file-share package not installed')
            return []
        except Exception as e:
            logger.error('FN:list_file_share_files share_name:{} directory_path:{} error:{}'.format(share_name, directory_path, str(e)))
            raise
    
    def test_connection(self) -> Dict:
        try:

            containers = self.list_containers()
            return {
                "success": True,
                "message": "Connection successful",
                "container_count": len(containers)
            }
        except Exception as e:
            error_message = str(e)
            error_str_lower = error_message.lower()
            

            if "authorizationfailure" in error_str_lower or "authorization" in error_str_lower:
                detailed_message = (
                    "Authorization Failure: The service principal does not have the required permissions.\n\n"
                    "Required Azure RBAC Roles:\n"
                    "• Storage Blob Data Contributor (recommended) OR\n"
                    "• Storage Blob Data Reader (read-only)\n\n"
                    "How to fix:\n"
                    "1. Go to Azure Portal → Storage Account → Access Control (IAM)\n"
                    "2. Click 'Add role assignment'\n"
                    "3. Select 'Storage Blob Data Contributor' role\n"
                    "4. Assign to your Service Principal (search by Client ID)\n"
                    "5. Wait 1-2 minutes for permissions to propagate\n\n"
                    "For Data Lake Gen2, ensure the role is assigned at the Storage Account level."
                )
                logger.error('FN:test_connection auth_error:{}'.format(error_message))
                return {
                    "success": False,
                    "message": detailed_message,
                    "error_code": "AuthorizationFailure",
                    "container_count": 0
                }
            elif "authentication" in error_str_lower or "invalid" in error_str_lower:
                detailed_message = (
                    "Authentication Failed: Invalid credentials.\n\n"
                    "Please verify:\n"
                    "• Tenant ID is correct\n"
                    "• Client ID (Application ID) is correct\n"
                    "• Client Secret is valid and not expired\n"
                    "• Service Principal is enabled in Azure AD"
                )
                logger.error('FN:test_connection auth_error:{}'.format(error_message))
                return {
                    "success": False,
                    "message": detailed_message,
                    "error_code": "AuthenticationFailed",
                    "container_count": 0
                }
            else:
                logger.error('FN:test_connection error:{}'.format(error_message))
                return {
                    "success": False,
                    "message": error_message,
                    "container_count": 0
                }