import os
import logging
from typing import Dict, List, Optional
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient
from dotenv import load_dotenv
from pathlib import Path


backend_dir = Path(__file__).parent.parent
env_path = backend_dir / '.env'
load_dotenv(env_path)

logger = logging.getLogger(__name__)


AZURE_AI_LANGUAGE_ENDPOINT = os.getenv("AZURE_AI_LANGUAGE_ENDPOINT", "")
AZURE_AI_LANGUAGE_KEY = os.getenv("AZURE_AI_LANGUAGE_KEY", "")


class AzureDLPClient:
    
    def __init__(self, endpoint: Optional[str] = None, key: Optional[str] = None):
        self.endpoint = endpoint or AZURE_AI_LANGUAGE_ENDPOINT
        self.key = key or AZURE_AI_LANGUAGE_KEY
        
        if not self.endpoint or not self.key:
            logger.warning('FN:AzureDLPClient.__init__ endpoint:{} key_configured:{}'.format(self.endpoint, bool(self.key)))
            self.client = None
        else:
            try:

                endpoint_clean = self.endpoint.rstrip('/')
                credential = AzureKeyCredential(self.key)
                self.client = TextAnalyticsClient(endpoint=endpoint_clean, credential=credential)
                logger.info('FN:AzureDLPClient.__init__ endpoint:{}'.format(endpoint_clean))
            except Exception as e:
                logger.error('FN:AzureDLPClient.__init__ endpoint:{} error:{}'.format(self.endpoint, str(e)))
                self.client = None
    
    def detect_pii_in_text(self, text: str, language: str = "en") -> Dict:
        if not self.client or not text:
            return {
                "pii_detected": False,
                "pii_types": [],
                "entities": []
            }
        
        try:


            text_to_analyze = text[:5120] if len(text) > 5120 else text
            

            result = self.client.recognize_pii_entities([text_to_analyze], language=language)
            
            if not result or len(result) == 0:
                return {
                    "pii_detected": False,
                    "pii_types": [],
                    "entities": []
                }
            
            document_result = result[0]
            

            if document_result.is_error:
                logger.warning('FN:detect_pii_in_text text_length:{} language:{} error:{}'.format(len(text_to_analyze), language, document_result.error))
                return {
                    "pii_detected": False,
                    "pii_types": [],
                    "entities": []
                }
            

            entities = []
            pii_types = []
            
            for entity in document_result.entities:
                entities.append({
                    "text": entity.text,
                    "category": entity.category,
                    "subcategory": entity.subcategory,
                    "confidence_score": entity.confidence_score,
                    "offset": entity.offset,
                    "length": entity.length
                })
                

                category = entity.subcategory or entity.category
                if category and category not in pii_types:
                    pii_types.append(category)
            
            return {
                "pii_detected": len(entities) > 0,
                "pii_types": pii_types,
                "entities": entities
            }
            
        except Exception as e:
            logger.error('FN:detect_pii_in_text text_length:{} language:{} error:{}'.format(len(text) if text else 0, language, str(e)))
            return {
                "pii_detected": False,
                "pii_types": [],
                "entities": []
            }
    
    def detect_pii_in_column_name(self, column_name: str, sample_data: Optional[List[str]] = None) -> Dict:
        if not column_name:
            return {
                "pii_detected": False,
                "pii_types": []
            }
        

        if not self.client:
            logger.warning('FN:detect_pii_in_column_name column_name:{} message:Azure DLP client not configured'.format(column_name))
            return {
                "pii_detected": False,
                "pii_types": []
            }
        
        all_pii_types = []
        pii_detected = False
        

        name_result = self.detect_pii_in_text(column_name)
        if name_result.get("pii_detected"):
            pii_detected = True
            all_pii_types.extend(name_result.get("pii_types", []))
        

        if sample_data:


            sample_text = " ".join(str(val) for val in sample_data[:10])
            sample_text = sample_text[:5120]
            
            if sample_text.strip():
                sample_result = self.detect_pii_in_text(sample_text)
                if sample_result.get("pii_detected"):
                    pii_detected = True

                    for pii_type in sample_result.get("pii_types", []):
                        if pii_type not in all_pii_types:
                            all_pii_types.append(pii_type)
        
        return {
            "pii_detected": pii_detected,
            "pii_types": list(set(all_pii_types))
        }



_dlp_client = None


def get_dlp_client() -> Optional[AzureDLPClient]:
    global _dlp_client
    if _dlp_client is None:
        _dlp_client = AzureDLPClient()
    return _dlp_client


def detect_pii_in_column(column_name: str, sample_data: Optional[List[str]] = None) -> Dict:
    client = get_dlp_client()
    

    if client and client.client:
        return client.detect_pii_in_column_name(column_name, sample_data)
    

    return _detect_pii_pattern_based(column_name, sample_data)


def _detect_pii_pattern_based(column_name: str, sample_data: Optional[List[str]] = None) -> Dict:
    import re
    
    if not column_name:
        return {"pii_detected": False, "pii_types": []}
    
    pii_patterns = {
        'Email': [
            re.compile(r'email', re.IGNORECASE),
            re.compile(r'e-mail', re.IGNORECASE),
            re.compile(r'mail', re.IGNORECASE)
        ],
        'PhoneNumber': [
            re.compile(r'phone', re.IGNORECASE),
            re.compile(r'tel', re.IGNORECASE),
            re.compile(r'mobile', re.IGNORECASE),
            re.compile(r'cell', re.IGNORECASE)
        ],
        'SSN': [
            re.compile(r'ssn', re.IGNORECASE),
            re.compile(r'social.*security', re.IGNORECASE),
            re.compile(r'national.*id', re.IGNORECASE)
        ],
        'CreditCard': [
            re.compile(r'credit.*card', re.IGNORECASE),
            re.compile(r'card.*number', re.IGNORECASE),
            re.compile(r'ccn', re.IGNORECASE)
        ],
        'PersonName': [
            re.compile(r'name', re.IGNORECASE),
            re.compile(r'first.*name', re.IGNORECASE),
            re.compile(r'last.*name', re.IGNORECASE),
            re.compile(r'full.*name', re.IGNORECASE),
            re.compile(r'customer.*name', re.IGNORECASE),
            re.compile(r'user.*name', re.IGNORECASE)
        ],
        'Address': [
            re.compile(r'address', re.IGNORECASE),
            re.compile(r'street', re.IGNORECASE),
            re.compile(r'city', re.IGNORECASE),
            re.compile(r'zip', re.IGNORECASE),
            re.compile(r'postal', re.IGNORECASE)
        ],
        'DateOfBirth': [
            re.compile(r'date.*birth', re.IGNORECASE),
            re.compile(r'dob', re.IGNORECASE),
            re.compile(r'birth.*date', re.IGNORECASE)
        ],
        'IPAddress': [
            re.compile(r'ip.*address', re.IGNORECASE),
            re.compile(r'ip', re.IGNORECASE)
        ],
        'AccountNumber': [
            re.compile(r'account.*number', re.IGNORECASE),
            re.compile(r'acc.*no', re.IGNORECASE)
        ]
    }
    
    detected_types = []
    column_lower = column_name.lower()
    

    for pii_type, patterns in pii_patterns.items():
        for pattern in patterns:
            if pattern.search(column_name):
                if pii_type not in detected_types:
                    detected_types.append(pii_type)
                break
    

    if sample_data:
        sample_text = " ".join(str(val) for val in sample_data[:10])
        

        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        if email_pattern.search(sample_text) and 'Email' not in detected_types:
            detected_types.append('Email')
        

        phone_pattern = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s?\d{3}[-.]?\d{4}\b')
        if phone_pattern.search(sample_text) and 'PhoneNumber' not in detected_types:
            detected_types.append('PhoneNumber')
        

        ssn_pattern = re.compile(r'\b\d{3}-\d{2}-\d{4}\b')
        if ssn_pattern.search(sample_text) and 'SSN' not in detected_types:
            detected_types.append('SSN')
        

        cc_pattern = re.compile(r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b')
        if cc_pattern.search(sample_text) and 'CreditCard' not in detected_types:
            detected_types.append('CreditCard')
    
    return {
        "pii_detected": len(detected_types) > 0,
        "pii_types": detected_types
    }
