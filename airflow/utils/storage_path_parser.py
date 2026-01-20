from abc import ABC, abstractmethod
from typing import Dict, Optional
import re
import logging

logger = logging.getLogger(__name__)


class StoragePathParser(ABC):
    
    @abstractmethod
    def can_parse(self, path: str) -> bool:
        pass
    
    @abstractmethod
    def parse(self, path: str) -> Dict:
        pass


class ABFSParser(StoragePathParser):
    


    ABFS_PATTERN = re.compile(
        r'^(abfs|abfss)://([^@]+)@([^.]+)\.dfs\.core\.windows\.net(.*)$',
        re.IGNORECASE
    )
    
    def can_parse(self, path: str) -> bool:
        if not path:
            return False
        return bool(self.ABFS_PATTERN.match(path.strip()))
    
    def parse(self, path: str) -> Dict:
        path = path.strip()
        match = self.ABFS_PATTERN.match(path)
        
        if not match:
            raise ValueError(f"Invalid ABFS URL format: {path}")
        
        protocol = match.group(1).lower()
        container = match.group(2)
        account_name = match.group(3)
        file_path = match.group(4).lstrip('/')
        
        return {
            "type": "azure_datalake",
            "account_name": account_name,
            "container": container,
            "path": file_path,
            "protocol": protocol,
            "full_url": path,
            "connection": {
                "method": "service_principal",
                "account_name": account_name,
                "endpoint": f"https://{account_name}.dfs.core.windows.net"
            },
            "container_info": {
                "name": container,
                "type": "filesystem"
            },
            "metadata": {
                "storage_type": "azure_datalake_gen2",
                "protocol": protocol
            }
        }


class AzureBlobParser(StoragePathParser):
    

    BLOB_URL_PATTERN = re.compile(
        r'^https://([^.]+)\.blob\.core\.windows\.net/([^/]+)(.*)$',
        re.IGNORECASE
    )
    
    def can_parse(self, path: str) -> bool:
        if not path:
            return False
        

        if self.BLOB_URL_PATTERN.match(path.strip()):
            return True
        


        if '/' in path and not path.startswith(('http://', 'https://', 'abfs://', 'abfss://', 's3://', 'gs://')):
            return True
        
        return False
    
    def parse(self, path: str, account_name: Optional[str] = None, 
              container: Optional[str] = None) -> Dict:
        path = path.strip()
        

        match = self.BLOB_URL_PATTERN.match(path)
        if match:
            account_name = match.group(1)
            container = match.group(2)
            file_path = match.group(3).lstrip('/')
        else:

            if account_name and container:

                file_path = path
            elif not account_name and not container:


                parts = path.split('/', 1)
                if len(parts) == 2:
                    container = parts[0]
                    file_path = parts[1]
                else:

                    file_path = path
                    container = None
            else:

                file_path = path
        
        return {
            "type": "azure_blob",
            "account_name": account_name or "unknown",
            "container": container or "unknown",
            "path": file_path,
            "protocol": "https",
            "full_url": f"https://{account_name}.blob.core.windows.net/{container}/{file_path}" if account_name and container else path,
            "connection": {
                "method": "connection_string",
                "account_name": account_name or "unknown"
            },
            "container_info": {
                "name": container or "unknown",
                "type": "blob_container"
            },
            "metadata": {
                "storage_type": "azure_blob_storage"
            }
        }


class PathParserRegistry:
    
    def __init__(self):
        self.parsers: list[StoragePathParser] = []
        self._register_default_parsers()
    
    def _register_default_parsers(self):

        self.register(ABFSParser())

        self.register(AzureBlobParser())
    
    def register(self, parser: StoragePathParser):
        if parser not in self.parsers:
            self.parsers.append(parser)
            logger.info(f'FN:PathParserRegistry.register parser:{parser.__class__.__name__}')
    
    def parse(self, path: str, account_name: Optional[str] = None, 
              container: Optional[str] = None) -> Dict:
        if not path:
            raise ValueError("Path cannot be empty")
        

        for parser in self.parsers:
            if parser.can_parse(path):
                try:

                    if isinstance(parser, AzureBlobParser):
                        result = parser.parse(path, account_name, container)
                    else:
                        result = parser.parse(path)
                    
                    logger.info(f'FN:PathParserRegistry.parse path:{path} parser:{parser.__class__.__name__} type:{result.get("type")}')
                    return result
                except Exception as e:
                    logger.warning(f'FN:PathParserRegistry.parse path:{path} parser:{parser.__class__.__name__} error:{str(e)}')
                    continue
        

        if account_name and container:
            try:
                parser = AzureBlobParser()
                result = parser.parse(path, account_name, container)
                logger.info(f'FN:PathParserRegistry.parse path:{path} parser:AzureBlobParser(fallback) type:{result.get("type")}')
                return result
            except Exception as e:
                logger.warning(f'FN:PathParserRegistry.parse path:{path} parser:AzureBlobParser(fallback) error:{str(e)}')
        
        raise ValueError(f"No parser found for path: {path}")



_default_registry = PathParserRegistry()


def parse_storage_path(path: str, account_name: Optional[str] = None, 
                      container: Optional[str] = None) -> Dict:
    return _default_registry.parse(path, account_name, container)

