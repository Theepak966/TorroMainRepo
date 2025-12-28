from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import ClientSecretCredential
from typing import List, Dict, Optional, Union
import logging

# Data Lake Gen2 support
try:
    from azure.storage.filedatalake import DataLakeServiceClient
    DATALAKE_AVAILABLE = True
except ImportError:
    DATALAKE_AVAILABLE = False
    DataLakeServiceClient = None

logger = logging.getLogger(__name__)


def create_azure_blob_client(config: Dict) -> 'AzureBlobClient':
    """
    Create an AzureBlobClient from a connection config dictionary.
    
    Supports both connection string and service principal authentication.
    
    Args:
        config: Dictionary containing either:
            - connection_string: For connection string auth
            - OR service principal fields:
                - account_name: Storage account name
                - tenant_id: Azure AD tenant ID
                - client_id: Service principal client ID (application ID)
                - client_secret: Service principal client secret
    
    Returns:
        AzureBlobClient instance
    """
    # Try connection string first (backward compatibility)
    connection_string = config.get('connection_string')
    if connection_string:
        return AzureBlobClient(connection_string=connection_string)
    
    # Try service principal authentication
    account_name = config.get('account_name')
    tenant_id = config.get('tenant_id')
    client_id = config.get('client_id')
    client_secret = config.get('client_secret')
    
    if account_name and tenant_id and client_id and client_secret:
        # Check if we should use DFS endpoint for Data Lake Gen2
        use_dfs = config.get('use_dfs_endpoint', False) or config.get('storage_type') == 'datalake'
        
        # Create credential
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        
        if use_dfs:
            # For Data Lake Gen2, use DFS endpoint
            dfs_account_url = f"https://{account_name}.dfs.core.windows.net"
            return AzureBlobClient(dfs_account_url=dfs_account_url, dfs_credential=credential)
        else:
            # Use blob endpoint (works for both Blob Storage and Data Lake Gen2)
            account_url = f"https://{account_name}.blob.core.windows.net"
            return AzureBlobClient(account_url=account_url, credential=credential)
    
    raise ValueError(
        "Config must contain either 'connection_string' OR "
        "('account_name', 'tenant_id', 'client_id', 'client_secret') for service principal authentication"
    )


class AzureBlobClient:
    def __init__(self, connection_string: Optional[str] = None, 
                 account_url: Optional[str] = None,
                 credential: Optional[ClientSecretCredential] = None,
                 dfs_account_url: Optional[str] = None,
                 dfs_credential: Optional[ClientSecretCredential] = None):
        """
        Initialize Azure Blob Client with either connection string or service principal credentials.
        
        Args:
            connection_string: Azure Storage connection string (for connection string auth)
            account_url: Azure Storage account URL (https://<account_name>.blob.core.windows.net) (for service principal)
            credential: ClientSecretCredential object (for service principal auth)
            dfs_account_url: Azure Data Lake Gen2 DFS endpoint (https://<account_name>.dfs.core.windows.net)
            dfs_credential: ClientSecretCredential for DFS endpoint
        """
        if connection_string:
            self.connection_string = connection_string
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self.auth_method = "connection_string"
        elif dfs_account_url and dfs_credential:
            # Use Data Lake Gen2 DFS endpoint
            if not DATALAKE_AVAILABLE:
                raise ImportError("azure-storage-filedatalake package is required for Data Lake Gen2 support. Install it with: pip install azure-storage-filedatalake")
            self.dfs_account_url = dfs_account_url
            self.dfs_credential = dfs_credential
            self.data_lake_service_client = DataLakeServiceClient(account_url=dfs_account_url, credential=dfs_credential)
            # Access underlying blob client from DataLakeServiceClient
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
        """
        List files in Azure Data Lake Gen2 using the Data Lake API (matches 'az storage fs file list').
        
        This method uses the Data Lake Gen2 API which is more appropriate for hierarchical file systems
        compared to the blob API.
        
        Args:
            file_system_name: Name of the file system (container)
            path: Path within the file system (e.g., 'visionplus/ATH3')
            file_extensions: Optional list of file extensions to filter (e.g., ['.csv', '.parquet'])
        
        Returns:
            List of file dictionaries with metadata
        """
        if not hasattr(self, 'data_lake_service_client') or not self.data_lake_service_client:
            # Fallback to blob API if Data Lake client not available
            return self.list_blobs(file_system_name, path, file_extensions)
        
        try:
            file_system_client = self.data_lake_service_client.get_file_system_client(file_system_name)
            
            # Normalize path (remove leading/trailing slashes)
            normalized_path = path.strip('/') if path else ""
            
            # Get directory client
            if normalized_path:
                directory_client = file_system_client.get_directory_client(normalized_path)
            else:
                directory_client = file_system_client.get_directory_client("")
            
            files = []
            paths = directory_client.list_paths(recursive=False)
            
            for path_item in paths:
                # Skip directories (they have is_directory=True)
                if path_item.is_directory:
                    continue
                
                file_name = path_item.name.split('/')[-1] if '/' in path_item.name else path_item.name
                
                # Filter by extension if specified
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
            # Fallback to blob API if Data Lake API fails
            logger.info('FN:list_datalake_files falling_back_to_blob_api file_system:{} path:{}'.format(
                file_system_name, path
            ))
            return self.list_blobs(file_system_name, path, file_extensions)
    
    def list_blobs(self, container_name: str, folder_path: str = "", file_extensions: List[str] = None) -> List[Dict]:
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = []
            
            prefix = folder_path.rstrip("/") + "/" if folder_path else ""
            # CRITICAL FIX: Convert lazy iterator to list immediately to avoid hanging
            # The iterator can hang on pagination if there are many blobs
            # Convert to list immediately - this will fetch all pages in one go
            blob_list = []
            count = 0
            try:
                blob_iterator = container_client.list_blobs(name_starts_with=prefix)
                for blob in blob_iterator:
                    blob_list.append(blob)
                    count += 1
                    # Note: Removed hard limit to discover all assets
                    # For very large containers (>100k blobs), consider pagination or async processing
                    if count % 10000 == 0:
                        logger.info('FN:list_blobs container_name:{} folder_path:{} processed_count:{}'.format(container_name, folder_path, count))
                logger.info('FN:list_blobs container_name:{} folder_path:{} fetched_blob_count:{}'.format(container_name, folder_path, len(blob_list)))
            except Exception as e:
                logger.error('FN:list_blobs container_name:{} folder_path:{} list_error:{}'.format(container_name, folder_path, str(e)))
                raise
            
            for blob in blob_list:
                # Skip directories (blobs ending with /)
                if blob.name.endswith('/'):
                    logger.debug('FN:list_blobs blob_name:{} message:Skipping directory'.format(blob.name))
                    continue
                
                if file_extensions:
                    if not any(blob.name.lower().endswith(ext.lower()) for ext in file_extensions):
                        logger.debug('FN:list_blobs blob_name:{} message:Skipping blob extension filter'.format(blob.name))
                        continue
                
                # CRITICAL FIX: Use properties directly from list_blobs() - NO extra API calls!
                # This was causing tasks to hang/timeout. list_blobs() already has all properties we need.
                class BlobPropertiesProxy:
                    def __init__(self, blob_item):
                        self.size = getattr(blob_item, 'size', 0)
                        self.etag = getattr(blob_item, 'etag', '').strip('"') if hasattr(blob_item, 'etag') else ''
                        self.creation_time = getattr(blob_item, 'creation_time', None)
                        self.last_modified = getattr(blob_item, 'last_modified', None)
                        # Content settings from blob properties
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
        # Get only headers/column names (first N bytes) - NO data rows
        # For banking/financial compliance: We only extract column names, never actual data
        # CSV: First line only (headers)
        # JSON: First object keys only
        # Supports both Blob Storage and Data Lake Gen2
        try:
            # Check if we have Data Lake client and use it for Data Lake Gen2
            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)
                    # Download only first max_bytes (just enough for headers/keys - NO data)
                    return file_client.download_file(offset=0, length=max_bytes).readall()
                except Exception as e:
                    logger.warning('FN:get_blob_sample datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            
            # Use regular blob API
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            # Download only first max_bytes (just enough for headers/keys - NO data)
            return blob_client.download_blob(offset=0, length=max_bytes).readall()
        except Exception as e:
            logger.warning('FN:get_blob_sample container_name:{} blob_path:{} max_bytes:{} error:{}'.format(container_name, blob_path, max_bytes, str(e)))
            return b""
    
    def get_blob_tail(self, container_name: str, blob_path: str, max_bytes: int = 8192) -> bytes:
        # Get the tail (last N bytes) of a blob. Useful for Parquet files where metadata is at the end
        # Supports both Blob Storage and Data Lake Gen2
        try:
            # Check if we have Data Lake client and use it for Data Lake Gen2
            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)
                    # Get file size first
                    properties = file_client.get_file_properties()
                    file_size = properties.size
                    
                    # Read from the end
                    offset = max(0, file_size - max_bytes)
                    length = min(max_bytes, file_size)
                    return file_client.download_file(offset=offset, length=length).readall()
                except Exception as e:
                    logger.warning('FN:get_blob_tail datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            
            # Use regular blob API
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            # Get file size first
            properties = blob_client.get_blob_properties()
            file_size = properties.size
            
            # Read from the end
            offset = max(0, file_size - max_bytes)
            length = min(max_bytes, file_size)
            return blob_client.download_blob(offset=offset, length=length).readall()
        except Exception as e:
            logger.warning('FN:get_blob_tail container_name:{} blob_path:{} max_bytes:{} error:{}'.format(container_name, blob_path, max_bytes, str(e)))
            return b""
    
    def get_blob_properties(self, container_name: str, blob_path: str) -> Dict:
        # Get blob/file properties - supports both Blob Storage and Data Lake Gen2
        try:
            # Check if we have Data Lake client and use it for Data Lake Gen2
            if hasattr(self, 'data_lake_service_client') and self.data_lake_service_client:
                try:
                    file_system_client = self.data_lake_service_client.get_file_system_client(container_name)
                    file_client = file_system_client.get_file_client(blob_path)
                    properties = file_client.get_file_properties()
                    
                    # Convert Data Lake file properties to blob-like format
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
                        "access_tier": None,  # Data Lake Gen2 doesn't have access tier
                    }
                except Exception as e:
                    logger.warning('FN:get_blob_properties datalake_error:{} falling_back_to_blob_api'.format(str(e)))
            
            # Use regular blob API
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            properties = blob_client.get_blob_properties()
            
            # Get blob type
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
                "metadata": properties.metadata or {},  # Ensure metadata is always a dict
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
        """List all containers in the storage account"""
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
        """Extract account name from connection string or account URL"""
        if hasattr(self, 'connection_string') and self.connection_string:
            # Extract from connection string: AccountName=xxx;AccountKey=yyy
            for part in self.connection_string.split(';'):
                if part.startswith('AccountName='):
                    return part.split('=')[1]
        elif hasattr(self, 'account_url') and self.account_url:
            # Extract from URL: https://accountname.blob.core.windows.net
            return self.account_url.split('//')[1].split('.')[0]
        elif hasattr(self, 'dfs_account_url') and self.dfs_account_url:
            # Extract from DFS URL: https://accountname.dfs.core.windows.net
            return self.dfs_account_url.split('//')[1].split('.')[0]
        raise ValueError("Cannot determine account name from connection")
    
    def _get_credential_or_connection_string(self):
        """Get credential or connection string for other Azure Storage services"""
        if hasattr(self, 'connection_string') and self.connection_string:
            return self.connection_string, None
        elif hasattr(self, 'credential') and self.credential:
            return None, self.credential
        elif hasattr(self, 'dfs_credential') and self.dfs_credential:
            return None, self.dfs_credential
        else:
            raise ValueError("Cannot create service client - missing credentials")
    
    def list_file_shares(self) -> List[Dict]:
        """List all file shares in the storage account"""
        try:
            from azure.storage.fileshare import ShareServiceClient
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                share_service_client = ShareServiceClient.from_connection_string(connection_string)
            elif credential:
                account_name = self._get_account_name()
                share_service_url = f"https://{account_name}.file.core.windows.net"
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
        except ImportError:
            logger.warning('FN:list_file_shares message:azure-storage-file-share package not installed')
            return []
        except Exception as e:
            logger.error('FN:list_file_shares error:{}'.format(str(e)))
            raise
    
    def list_queues(self) -> List[Dict]:
        """List all queues in the storage account"""
        try:
            from azure.storage.queue import QueueServiceClient
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                queue_service_client = QueueServiceClient.from_connection_string(connection_string)
            elif credential:
                account_name = self._get_account_name()
                queue_service_url = f"https://{account_name}.queue.core.windows.net"
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
        except ImportError:
            logger.warning('FN:list_queues message:azure-storage-queue package not installed')
            return []
        except Exception as e:
            logger.error('FN:list_queues error:{}'.format(str(e)))
            raise
    
    def list_tables(self) -> List[Dict]:
        """List all tables in the storage account"""
        try:
            from azure.data.tables import TableServiceClient
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                table_service_client = TableServiceClient.from_connection_string(connection_string)
            elif credential:
                account_name = self._get_account_name()
                table_service_url = f"https://{account_name}.table.core.windows.net"
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
        except ImportError:
            logger.warning('FN:list_tables message:azure-data-tables package not installed')
            return []
        except Exception as e:
            logger.error('FN:list_tables error:{}'.format(str(e)))
            raise
    
    def list_file_share_files(self, share_name: str, directory_path: str = "", file_extensions: List[str] = None) -> List[Dict]:
        """List files in an Azure File Share"""
        try:
            from azure.storage.fileshare import ShareClient, ShareDirectoryClient
            
            connection_string, credential = self._get_credential_or_connection_string()
            
            if connection_string:
                share_client = ShareClient.from_connection_string(connection_string, share_name)
            elif credential:
                account_name = self._get_account_name()
                share_service_url = f"https://{account_name}.file.core.windows.net"
                share_client = ShareClient(account_url=f"{share_service_url}/{share_name}", credential=credential)
            else:
                raise ValueError("Cannot create ShareClient - missing credentials")
            
            directory_client = share_client.get_directory_client(directory_path)
            files = []
            
            for item in directory_client.list_directories_and_files():
                if item.is_directory:
                    continue  # Skip directories for now, or handle recursively
                
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
        """Test the connection to Azure Blob Storage"""
        try:
            # Try to list containers as a connection test
            containers = self.list_containers()
            return {
                "success": True,
                "message": "Connection successful",
                "container_count": len(containers)
            }
        except Exception as e:
            error_message = str(e)
            error_str_lower = error_message.lower()
            
            # Provide helpful error messages for common issues
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