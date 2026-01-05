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
    """
    Detect PII in column using regex-based patterns only.
    Azure DLP is disabled - using regex patterns for performance and cost efficiency.
    """
    # FORCE regex-based detection (Azure DLP disabled)
    return _detect_pii_pattern_based(column_name, sample_data)


def _detect_pii_pattern_based(column_name: str, sample_data: Optional[List[str]] = None) -> Dict:
    import re
    
    if not column_name:
        return {"pii_detected": False, "pii_types": []}
    
    detected_types = []
    column_lower = column_name.lower()
    # Normalize: replace all separators with nothing for direct matching
    column_clean = re.sub(r'[_\-\s]', '', column_lower)
    
    # SUPER SIMPLE KEYWORD-BASED DETECTION - Check if keywords exist in column name
    # This is MUCH more reliable than regex patterns
    
    # Email - check first before other checks
    if 'email' in column_lower or ('mail' in column_lower and 'name' not in column_lower) or 'correo' in column_lower:
        detected_types.append('Email')
    
    # Phone - check before other checks
    if 'phone' in column_lower or 'tel' in column_lower or 'mobile' in column_lower or ('cell' in column_lower and 'name' not in column_lower):
        detected_types.append('PhoneNumber')
    
    # SSN
    if 'ssn' in column_lower or ('social' in column_lower and 'security' in column_lower) or ('national' in column_lower and 'id' in column_lower) or 'tin' in column_lower:
        detected_types.append('SSN')
    
    # Credit Card
    if ('credit' in column_lower and 'card' in column_lower) or ('card' in column_lower and 'number' in column_lower) or 'ccn' in column_lower or 'cvv' in column_lower or 'cvc' in column_lower:
        detected_types.append('CreditCard')
    
    # Person Name
    if ('name' in column_lower or 'fname' in column_lower or 'lname' in column_lower or 'firstname' in column_lower or 'lastname' in column_lower or 'fullname' in column_lower or 'surname' in column_lower or 
        ('reviewed' in column_lower and 'by' in column_lower) or 
        ('created' in column_lower and 'by' in column_lower) or 
        ('updated' in column_lower and 'by' in column_lower) or
        'owner' in column_lower or 'author' in column_lower):
        detected_types.append('PersonName')
    
    # Address
    if 'address' in column_lower or 'street' in column_lower or 'city' in column_lower or 'zip' in column_lower or 'postal' in column_lower or 'state' in column_lower or 'country' in column_lower or 'addr' in column_lower or 'province' in column_lower or 'county' in column_lower:
        detected_types.append('Address')
    
    # Date of Birth
    if 'dob' in column_lower or ('birth' in column_lower and 'date' in column_lower) or ('date' in column_lower and 'birth' in column_lower) or 'birthdate' in column_lower:
        detected_types.append('DateOfBirth')
    
    # IP Address
    if 'ip' in column_lower or 'ipaddr' in column_lower or 'ipv4' in column_lower or 'ipv6' in column_lower:
        detected_types.append('IPAddress')
    
    # Account Number / Account ID - PII
    if ('account' in column_lower and 'number' in column_lower) or ('account' in column_lower and 'id' in column_lower) or 'accountnum' in column_lower or 'accountnumber' in column_lower or 'accnum' in column_lower or ('account' in column_lower and 'code' in column_lower) or 'account_id' in column_lower or 'accountid' in column_clean:
        detected_types.append('AccountNumber')
    
    # Customer ID - PII
    if ('customer' in column_lower and 'id' in column_lower) or 'customerid' in column_clean or 'customer_id' in column_lower or ('cust' in column_lower and 'id' in column_lower):
        detected_types.append('CustomerID')
    
    # Transaction ID - PII (can identify individuals through transactions)
    if ('transaction' in column_lower and 'id' in column_lower) or 'transactionid' in column_clean or 'transaction_id' in column_lower or 'txn_id' in column_lower or 'txnid' in column_clean:
        detected_types.append('TransactionID')
    
    # User ID - MUST check this BEFORE PersonName to avoid conflicts
    if ('user' in column_lower and 'id' in column_lower) or 'userid' in column_clean or 'user_id' in column_lower or ('person' in column_lower and 'id' in column_lower) and 'name' not in column_lower:
        detected_types.append('UserID')
    
    # Generic ID detection - ANY column ending with _id is likely PII
    if column_lower.endswith('_id') or column_clean.endswith('id'):
        # But exclude some non-PII IDs
        non_pii_ids = ['alert_id', 'event_id', 'log_id', 'session_id', 'request_id', 'job_id', 'task_id', 'batch_id', 'process_id', 'thread_id']
        if column_lower not in non_pii_ids:
            # Add generic ID type if not already detected
            if not any(t in detected_types for t in ['CustomerID', 'UserID', 'AccountNumber', 'TransactionID']):
                detected_types.append('ID')
    
    # Passport
    if 'passport' in column_lower:
        detected_types.append('PassportNumber')
    
    # Driver License
    if ('driver' in column_lower and 'license' in column_lower) or ('driving' in column_lower and 'license' in column_lower) or ('dl' in column_lower and len(column_lower) < 10) or 'driverlic' in column_clean:
        detected_types.append('DriverLicense')
    
    # Bank Account
    if ('bank' in column_lower and 'account' in column_lower) or ('routing' in column_lower and 'number' in column_lower) or 'iban' in column_lower or 'swift' in column_lower:
        detected_types.append('BankAccount')
    
    # Medical Record
    if ('medical' in column_lower and 'record' in column_lower) or ('patient' in column_lower and 'id' in column_lower) or 'diagnosis' in column_lower:
        detected_types.append('MedicalRecord')
    
    # License Plate
    if ('license' in column_lower and 'plate' in column_lower) or ('plate' in column_lower and 'number' in column_lower) or ('vehicle' in column_lower and 'registration' in column_lower):
        detected_types.append('LicensePlate')
    
    # Password
    if 'password' in column_lower or 'passwd' in column_lower or 'pwd' in column_lower or 'secret' in column_lower or ('api' in column_lower and 'key' in column_lower) or 'token' in column_lower:
        detected_types.append('Password')
    
    # Gender
    if 'gender' in column_lower or 'sex' in column_lower:
        detected_types.append('Gender')
    
    # Race
    if 'race' in column_lower or 'ethnicity' in column_lower:
        detected_types.append('Race')
    
    # Religion
    if 'religion' in column_lower or 'religious' in column_lower:
        detected_types.append('Religion')
    
    # Biometric
    if 'fingerprint' in column_lower or 'iris' in column_lower or 'retina' in column_lower or 'biometric' in column_lower:
        detected_types.append('Biometric')
    
    # Remove duplicates
    detected_types = list(set(detected_types))
    
    # COMPREHENSIVE DATA PATTERN DETECTION
    if sample_data:
        sample_text = " ".join(str(val) for val in sample_data[:20] if val is not None)  # Check more samples
        
        # Email patterns - multiple formats
        email_patterns = [
            re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\.[A-Z|a-z]{2,}\b'),  # .co.uk, .com.au
        ]
        for pattern in email_patterns:
            if pattern.search(sample_text) and 'Email' not in detected_types:
                detected_types.append('Email')
                break
        
        # Phone patterns - comprehensive
        phone_patterns = [
            re.compile(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b'),  # US: 123-456-7890
            re.compile(r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}\b'),  # US: (123) 456-7890
            re.compile(r'\b\+?\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b'),  # International
            re.compile(r'\b\d{10,15}\b'),  # Long numeric strings (likely phone)
            re.compile(r'\b\d{4}[-.\s]?\d{3}[-.\s]?\d{3}\b'),  # UK: 0123 456 789
        ]
        for pattern in phone_patterns:
            if pattern.search(sample_text) and 'PhoneNumber' not in detected_types:
                detected_types.append('PhoneNumber')
                break
        
        # SSN patterns - multiple formats
        ssn_patterns = [
            re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),  # 123-45-6789
            re.compile(r'\b\d{3}\s\d{2}\s\d{4}\b'),  # 123 45 6789
            re.compile(r'\b\d{9}\b'),  # 123456789 (if in SSN context)
        ]
        for pattern in ssn_patterns:
            if pattern.search(sample_text) and 'SSN' not in detected_types:
                detected_types.append('SSN')
                break
        
        # Credit Card patterns - comprehensive
        cc_patterns = [
            re.compile(r'\b\d{4}[-.\s]?\d{4}[-.\s]?\d{4}[-.\s]?\d{4}\b'),  # 16 digits
            re.compile(r'\b\d{13,19}\b'),  # 13-19 digits (card range)
            re.compile(r'\b\d{3,4}\b'),  # CVV/CVC (3-4 digits, context dependent)
        ]
        for pattern in cc_patterns:
            if pattern.search(sample_text) and 'CreditCard' not in detected_types:
                detected_types.append('CreditCard')
                break
        
        # IP Address patterns
        ip_patterns = [
            re.compile(r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'),  # IPv4
            re.compile(r'\b([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b'),  # IPv6
        ]
        for pattern in ip_patterns:
            if pattern.search(sample_text) and 'IPAddress' not in detected_types:
                detected_types.append('IPAddress')
                break
        
        # Passport patterns
        passport_patterns = [
            re.compile(r'\b[A-Z]{1,2}\d{6,9}\b'),  # US/UK format
            re.compile(r'\b\d{8,9}\b'),  # Numeric passports
        ]
        for pattern in passport_patterns:
            if pattern.search(sample_text) and 'PassportNumber' not in detected_types:
                detected_types.append('PassportNumber')
                break
        
        # Driver License patterns
        dl_patterns = [
            re.compile(r'\b[A-Z]{1,2}\d{6,8}\b'),  # US format
            re.compile(r'\b\d{8,16}\b'),  # Numeric DL
        ]
        for pattern in dl_patterns:
            if pattern.search(sample_text) and 'DriverLicense' not in detected_types:
                detected_types.append('DriverLicense')
                break
        
        # Bank Account patterns
        bank_patterns = [
            re.compile(r'\b\d{9,17}\b'),  # Account numbers
            re.compile(r'\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b'),  # IBAN format
        ]
        for pattern in bank_patterns:
            if pattern.search(sample_text) and 'BankAccount' not in detected_types:
                detected_types.append('BankAccount')
                break
        
        # Date of Birth patterns
        dob_patterns = [
            re.compile(r'\b(0?[1-9]|1[0-2])[/-](0?[1-9]|[12][0-9]|3[01])[/-](19|20)\d{2}\b'),  # MM/DD/YYYY
            re.compile(r'\b(0?[1-9]|[12][0-9]|3[01])[/-](0?[1-9]|1[0-2])[/-](19|20)\d{2}\b'),  # DD/MM/YYYY
            re.compile(r'\b(19|20)\d{2}[/-](0?[1-9]|1[0-2])[/-](0?[1-9]|[12][0-9]|3[01])\b'),  # YYYY/MM/DD
        ]
        for pattern in dob_patterns:
            if pattern.search(sample_text) and 'DateOfBirth' not in detected_types:
                detected_types.append('DateOfBirth')
                break
    
    return {
        "pii_detected": len(detected_types) > 0,
        "pii_types": detected_types
    }
