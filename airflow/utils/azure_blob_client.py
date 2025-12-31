from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import ClientSecretCredential
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
    
    if account_name and tenant_id and client_id and client_secret:

        use_dfs = config.get('use_dfs_endpoint', False) or config.get('storage_type') == 'datalake'
        

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        
        if use_dfs:

            dfs_account_url = f"https://{account_name}.dfs.core.windows.net"
            return AzureBlobClient(dfs_account_url=dfs_account_url, dfs_credential=credential)
        else:

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
            raise ValueError("Either connection_string or (account_url and credential) or (dfs_account_url and dfs_credential) must be provided")
    
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

                    if count > 10000:
                        logger.warning('FN:list_blobs container_name:{} folder_path:{} hit_safety_limit:10000'.format(container_name, folder_path))
                        break
                logger.info('FN:list_blobs container_name:{} folder_path:{} fetched_blob_count:{}'.format(container_name, folder_path, len(blob_list)))
            except Exception as e:
                logger.error('FN:list_blobs container_name:{} folder_path:{} list_error:{}'.format(container_name, folder_path, str(e)))
                raise
            
            for blob in blob_list:
                if file_extensions:
                    if not any(blob.name.lower().endswith(ext.lower()) for ext in file_extensions):
                        continue

                if blob.name.endswith('/'):
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
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )

            return blob_client.download_blob(offset=0, length=max_bytes).readall()
        except Exception as e:
            logger.warning('FN:get_blob_sample container_name:{} blob_path:{} max_bytes:{} error:{}'.format(container_name, blob_path, max_bytes, str(e)))
            return b""
    
    def get_blob_tail(self, container_name: str, blob_path: str, max_bytes: int = 8192) -> bytes:

        try:
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
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            properties = blob_client.get_blob_properties()
            
            return {
                "etag": properties.etag,
                "size": properties.size,
                "content_type": properties.content_settings.content_type,
                "created_at": properties.creation_time,
                "last_modified": properties.last_modified,
                "access_tier": properties.blob_tier if hasattr(properties, 'blob_tier') else None,
                "lease_status": properties.lease.status if hasattr(properties, 'lease') else None,
                "content_encoding": properties.content_settings.content_encoding,
                "content_language": properties.content_settings.content_language,
                "cache_control": properties.content_settings.cache_control,
                "metadata": properties.metadata,
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
    
    def test_connection(self) -> Dict:
        try:
            containers = self.list_containers()
            return {
                "success": True,
                "message": "Connection successful",
                "container_count": len(containers),
                "containers": [c["name"] for c in containers[:10]]
            }
        except Exception as e:
            error_msg = str(e)
            if "AuthorizationFailure" in error_msg:
                return {
                    "success": False,
                    "message": f"Authorization Failure: The service principal does not have the required permissions.\n\nRequired Azure RBAC Roles:\n• Storage Blob Data Contributor (recommended) OR\n• Storage Blob Data Reader (read-only)\n\nHow to fix:\n1. Go to Azure Portal → Storage Account → Access Control (IAM)\n2. Click 'Add role assignment'\n3. Select 'Storage Blob Data Contributor' role\n4. Assign to your Service Principal (search by Client ID)\n5. Wait 1-2 minutes for permissions to propagate\n\nFor Data Lake Gen2, ensure the role is assigned at the Storage Account level.",
                    "container_count": 0
                }
            return {
                "success": False,
                "message": f"Connection failed: {error_msg}",
                "container_count": 0
            }