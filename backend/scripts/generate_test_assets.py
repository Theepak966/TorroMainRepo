#!/usr/bin/env python3
"""
Generate 10,000 test Parquet files in Azure Blob Storage with Vision Plus Test folder structure.

Folder Structure:
Vision Plus Test/
  ATH1/data/lh_business_data_<date>/files.csv
  ATH2/data/lh_business_data_<date>/files.csv
  ...
  ATH15/data/lh_business_data_<date>/files.csv
  credit_card_1/data/lh_business_data_<date>/files.csv
  ...
  credit_card_15/data/lh_business_data_<date>/files.csv

Distribution:
- ~9,000 files with different schemas (unique column sets)
- ~1,000 files with the same schema (to test deduplication)

Usage:
    python generate_test_assets.py --connection-name <connection_name> --container <container> --connection-string <connection_string>
"""

import os
import sys
import argparse
import io
import random
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from azure.storage.blob import BlobServiceClient
    from azure.identity import ClientSecretCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    print("Warning: azure-storage-blob not available. Install with: pip install azure-storage-blob")

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    DATALAKE_AVAILABLE = True
except ImportError:
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        DATALAKE_AVAILABLE = True
    except ImportError:
        DATALAKE_AVAILABLE = False
        print("Warning: azure-storage-file-datalake not available. Install with: pip install azure-storage-file-datalake")

def generate_parquet_content(columns: List[str], num_rows: int = 10) -> bytes:
    """Generate Parquet file content with given columns."""
    data = {}
    
    # Generate data for each column
    for col in columns:
        col_data = []
        for i in range(num_rows):
            if 'id' in col.lower():
                col_data.append(f"{col}_{i+1}")
            elif 'date' in col.lower() or 'time' in col.lower():
                col_data.append((datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'))
            elif 'amount' in col.lower() or 'price' in col.lower() or 'balance' in col.lower():
                col_data.append(random.uniform(10, 10000))
            elif 'email' in col.lower():
                col_data.append(f"user{i+1}@example.com")
            elif 'phone' in col.lower() or 'mobile' in col.lower():
                col_data.append(f"+1{random.randint(2000000000, 9999999999)}")
            elif 'name' in col.lower():
                col_data.append(f"{col.title()} {i+1}")
            else:
                col_data.append(f"{col}_value_{i+1}")
        data[col] = col_data
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Convert to Parquet bytes
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    return buffer.read()

def get_unique_schema_columns(num: int) -> List[List[str]]:
    """Generate unique column schemas."""
    base_columns = [
        'id', 'name', 'email', 'phone', 'address', 'city', 'state', 'zip',
        'account_number', 'transaction_id', 'amount', 'balance', 'date',
        'status', 'type', 'category', 'description', 'created_at', 'updated_at',
        'user_id', 'product_id', 'order_id', 'quantity', 'price', 'total',
        'customer_name', 'customer_email', 'payment_method', 'shipping_address',
        'invoice_number', 'tax_amount', 'discount', 'subtotal', 'grand_total',
        'employee_id', 'department', 'salary', 'hire_date', 'manager_id',
        'product_name', 'sku', 'inventory_count', 'supplier', 'cost',
        'order_date', 'ship_date', 'tracking_number', 'carrier',
        'card_number', 'card_type', 'expiry_date', 'cvv', 'billing_address'
    ]
    
    schemas = []
    used_combinations = set()
    
    for i in range(num):
        # Generate unique combination
        while True:
            num_cols = random.randint(3, 15)
            cols = tuple(sorted(random.sample(base_columns, min(num_cols, len(base_columns)))))
            if cols not in used_combinations:
                used_combinations.add(cols)
                schemas.append(list(cols))
                break
    
    return schemas

def get_duplicate_schema_columns() -> List[str]:
    """Return a fixed schema for duplicate testing."""
    return ['account_number', 'transaction_id', 'name', 'mobile_number', 'card_details']

def upload_to_blob(blob_service_client, data_lake_client, container_name: str, 
                   folder_path: str, blob_name: str, content: bytes, use_datalake: bool = False):
    """Upload content to Azure Blob Storage or Data Lake."""
    blob_path = f"{folder_path.rstrip('/')}/{blob_name}" if folder_path else blob_name
    
    if use_datalake and DATALAKE_AVAILABLE and data_lake_client:
        # Use Data Lake API
        file_system_client = data_lake_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(file_path=blob_path)
        file_client.upload_data(content, overwrite=True)
    else:
        # Use Blob API
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_client.upload_blob(content, overwrite=True)
    
    print(f"  ✓ Uploaded: {blob_path}")

def main():
    parser = argparse.ArgumentParser(description='Generate 10,000 test CSV files with Vision Plus Test structure')
    parser.add_argument('--connection-name', required=True, help='Azure connection name')
    parser.add_argument('--container', required=True, help='Container/File System name')
    parser.add_argument('--account-name', help='Azure storage account name')
    parser.add_argument('--account-key', help='Azure storage account key')
    parser.add_argument('--connection-string', help='Azure connection string')
    parser.add_argument('--tenant-id', help='Azure tenant ID (for Service Principal auth)')
    parser.add_argument('--client-id', help='Azure client ID (for Service Principal auth)')
    parser.add_argument('--client-secret', help='Azure client secret (for Service Principal auth)')
    parser.add_argument('--use-datalake', action='store_true', help='Use Data Lake Gen2 endpoint (dfs)')
    parser.add_argument('--unique-count', type=int, default=9000, help='Number of unique schema files (default: 9000)')
    parser.add_argument('--duplicate-count', type=int, default=1000, help='Number of duplicate schema files (default: 1000)')
    
    args = parser.parse_args()
    
    if not AZURE_AVAILABLE:
        print("Error: azure-storage-blob package not installed.")
        print("Install with: pip install azure-storage-blob")
        sys.exit(1)
    
    use_datalake = args.use_datalake
    
    # Initialize blob service client and data lake client
    blob_service_client = None
    data_lake_client = None
    
    if args.connection_string:
        blob_service_client = BlobServiceClient.from_connection_string(args.connection_string)
        use_datalake = False
    elif args.account_name and args.tenant_id and args.client_id and args.client_secret:
        # Service Principal authentication
        credential = ClientSecretCredential(
            tenant_id=args.tenant_id,
            client_id=args.client_id,
            client_secret=args.client_secret
        )
        
        if use_datalake:
            if not DATALAKE_AVAILABLE:
                print("Error: azure-storage-file-datalake package not installed.")
                print("Install with: pip install azure-storage-file-datalake")
                sys.exit(1)
            dfs_account_url = f"https://{args.account_name}.dfs.core.windows.net"
            data_lake_client = DataLakeServiceClient(account_url=dfs_account_url, credential=credential)
            blob_service_client = data_lake_client._blob_service_client
        else:
            account_url = f"https://{args.account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    elif args.account_name and args.account_key:
        account_url = f"https://{args.account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=args.account_key)
        use_datalake = False
    else:
        print("Error: Need one of:")
        print("  --connection-string")
        print("  --account-name + --tenant-id + --client-id + --client-secret [--use-datalake]")
        print("  --account-name + --account-key")
        sys.exit(1)
    
    total_files = args.unique_count + args.duplicate_count
    print(f"Generating {total_files} test Parquet files with Vision Plus Test structure...")
    print(f"  - {args.unique_count} files with unique schemas")
    print(f"  - {args.duplicate_count} files with duplicate schema (for deduplication testing)")
    print()
    
    # Folder structure setup
    parent_folder = "Vision Plus Test"
    subfolders = []
    # ATH1 to ATH15
    for i in range(1, 16):
        subfolders.append(f"ATH{i}")
    # credit_card_1 to credit_card_15
    for i in range(1, 16):
        subfolders.append(f"credit_card_{i}")
    
    # Generate dated folders for each subfolder (10-15 per subfolder)
    dated_folders_per_subfolder = {}
    for subfolder in subfolders:
        num_dated = random.randint(10, 15)
        dates = []
        base_date = datetime.now() - timedelta(days=365)
        for j in range(num_dated):
            date_str = (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            dates.append(f"lh_business_data_{date_str}")
        dated_folders_per_subfolder[subfolder] = dates
    
    print(f"Folder structure:")
    print(f"  Parent: {parent_folder}")
    print(f"  Subfolders: {len(subfolders)} (ATH1-ATH15, credit_card_1-credit_card_15)")
    print(f"  Dated folders per subfolder: 10-15")
    print()
    
    # Generate unique schemas
    print(f"Generating {args.unique_count} unique schema files...")
    unique_schemas = get_unique_schema_columns(args.unique_count)
    duplicate_schema = get_duplicate_schema_columns()
    
    # Distribute files across folder structure
    file_counter = 0
    unique_schema_idx = 0
    
    # First, distribute unique schema files
    print(f"Distributing {args.unique_count} unique schema files...")
    for subfolder in subfolders:
        dated_folders = dated_folders_per_subfolder[subfolder]
        for dated_folder in dated_folders:
            # Random number of files per dated folder (1-5)
            num_files = random.randint(1, 5)
            for file_num in range(num_files):
                if unique_schema_idx >= len(unique_schemas):
                    break
                
                schema = unique_schemas[unique_schema_idx]
                content = generate_parquet_content(schema, num_rows=random.randint(5, 50))
                
                blob_path = f"{parent_folder}/{subfolder}/data/{dated_folder}/file_{file_num+1:03d}.parquet"
                upload_to_blob(blob_service_client, data_lake_client, args.container, "", blob_path, content, use_datalake)
                
                file_counter += 1
                unique_schema_idx += 1
                
                if file_counter % 500 == 0:
                    print(f"  Progress: {file_counter}/{args.unique_count} unique files uploaded")
                
                if unique_schema_idx >= len(unique_schemas):
                    break
            if unique_schema_idx >= len(unique_schemas):
                break
        if unique_schema_idx >= len(unique_schemas):
            break
    
    print(f"✓ Completed {unique_schema_idx} unique schema files\n")
    
    # Now distribute duplicate schema files (1,000 files with same columns)
    print(f"Distributing {args.duplicate_count} duplicate schema files...")
    duplicate_counter = 0
    
    for subfolder in subfolders:
        dated_folders = dated_folders_per_subfolder[subfolder]
        for dated_folder in dated_folders:
            # Random number of duplicate files per dated folder (0-3)
            num_files = random.randint(0, 3)
            for file_num in range(num_files):
                if duplicate_counter >= args.duplicate_count:
                    break
                
                content = generate_parquet_content(duplicate_schema, num_rows=random.randint(5, 50))
                
                blob_path = f"{parent_folder}/{subfolder}/data/{dated_folder}/duplicate_{file_num+1:03d}.parquet"
                upload_to_blob(blob_service_client, data_lake_client, args.container, "", blob_path, content, use_datalake)
                
                duplicate_counter += 1
                file_counter += 1
                
                if duplicate_counter % 100 == 0:
                    print(f"  Progress: {duplicate_counter}/{args.duplicate_count} duplicate files uploaded")
                
                if duplicate_counter >= args.duplicate_count:
                    break
            if duplicate_counter >= args.duplicate_count:
                break
        if duplicate_counter >= args.duplicate_count:
            break
    
    print(f"✓ Completed {duplicate_counter} duplicate schema files\n")
    
    print(f"✅ Successfully generated {file_counter} test Parquet files!")
    print(f"\nFolder structure created:")
    print(f"  {parent_folder}/")
    for subfolder in subfolders[:5]:  # Show first 5 as example
        print(f"    {subfolder}/data/")
        print(f"      lh_business_data_<date>/ (10-15 dated folders)")
    print(f"    ... ({len(subfolders)} total subfolders)")
    print(f"\nNext steps:")
    print(f"1. Run discovery on connection: {args.connection_name}")
    print(f"2. Point discovery to container: {args.container}, folder: '{parent_folder}'")
    print(f"3. Check that ~{args.unique_count} unique assets are discovered")
    print(f"4. Check that ~{args.duplicate_count} duplicate assets are discovered")
    print(f"5. Click 'Remove Duplicates' → 'Hide duplicates'")
    print(f"6. Verify that {args.duplicate_count - 1} duplicates are hidden (1 kept visible)")
    print(f"7. Check 'View hidden duplicates' with pagination")

if __name__ == '__main__':
    main()
