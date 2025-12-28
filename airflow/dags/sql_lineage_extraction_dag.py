"""
Airflow DAG to automatically extract SQL queries and create lineage relationships
Scans DAG files for SQL queries and extracts lineage
"""
from datetime import datetime, timedelta
from typing import List, Dict
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
import logging
import os
import re
import json
import pymysql
import sys

logger = logging.getLogger(__name__)

# Database configuration - use environment variables
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "torroforexcel"),
    "charset": "utf8mb4"
}


def extract_sql_from_dag_file(file_path: str) -> List[Dict]:
    """
    Extract SQL queries from a Python DAG file
    
    Returns:
        List of dicts with 'sql', 'line_number', 'context'
    """
    sql_queries = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Patterns to match SQL queries
        sql_patterns = [
            r'["\']([^"\']*SELECT[^"\']*)["\']',  # SQL in strings
            r'"""([^"]*SELECT[^"]*)"""',  # SQL in triple quotes
            r"'''([^']*SELECT[^']*)'''",  # SQL in triple single quotes
            r'sql\s*=\s*["\']([^"\']*)["\']',  # sql = "..."
            r'query\s*=\s*["\']([^"\']*)["\']',  # query = "..."
        ]
        
        for i, line in enumerate(lines, 1):
            for pattern in sql_patterns:
                matches = re.finditer(pattern, line, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    sql = match.group(1).strip()
                    if len(sql) > 20 and ('SELECT' in sql.upper() or 'INSERT' in sql.upper() or 'CREATE' in sql.upper()):
                        sql_queries.append({
                            'sql': sql,
                            'line_number': i,
                            'context': line.strip()[:100]  # First 100 chars for context
                        })
        
        logger.info('FN:extract_sql_from_dag_file file:{} queries_found:{}'.format(
            file_path, len(sql_queries)
        ))
        
    except Exception as e:
        logger.error('FN:extract_sql_from_dag_file file:{} error:{}'.format(file_path, str(e)))
    
    return sql_queries


def scan_dag_files_for_sql():
    """
    Scan all DAG files in the dags directory and extract SQL queries
    """
    dags_folder = os.path.join(os.path.dirname(__file__))
    all_sql_queries = []
    
    # Scan all Python files in dags folder
    for root, dirs, files in os.walk(dags_folder):
        for file in files:
            if file.endswith('.py') and file != '__init__.py':
                file_path = os.path.join(root, file)
                sql_queries = extract_sql_from_dag_file(file_path)
                for sql_query in sql_queries:
                    sql_query['source_file'] = file_path
                    all_sql_queries.append(sql_query)
    
    logger.info('FN:scan_dag_files_for_sql total_queries:{}'.format(len(all_sql_queries)))
    return all_sql_queries


def parse_sql_and_create_lineage():
    """
    Parse extracted SQL queries and create lineage relationships
    """
    try:
        # Import SQL lineage extractor
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))
        from utils.sql_lineage_extractor import extract_lineage_from_sql
        
        # Get SQL queries from DAG files
        sql_queries = scan_dag_files_for_sql()
        
        if not sql_queries:
            logger.info('FN:parse_sql_and_create_lineage message:No SQL queries found in DAG files')
            return
        
        # Connect to database
        conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        
        try:
            with conn.cursor() as cursor:
                created_count = 0
                skipped_count = 0
                
                for sql_data in sql_queries:
                    sql_query = sql_data['sql']
                    
                    try:
                        # Extract lineage from SQL
                        lineage_result = extract_lineage_from_sql(sql_query, dialect='mysql')
                        
                        if not lineage_result.get('target_table') or not lineage_result.get('source_tables'):
                            continue
                        
                        target_table = lineage_result['target_table']
                        source_tables = lineage_result.get('source_tables', [])
                        
                        # Find target asset
                        cursor.execute("""
                            SELECT id FROM assets 
                            WHERE name LIKE %s 
                            LIMIT 1
                        """, (f'%{target_table}%',))
                        target_asset = cursor.fetchone()
                        
                        if not target_asset:
                            logger.debug('FN:parse_sql_and_create_lineage target_table:{} message:Target asset not found'.format(target_table))
                            continue
                        
                        target_asset_id = target_asset['id']
                        
                        # Create relationships for each source table
                        for source_table in source_tables:
                            cursor.execute("""
                                SELECT id FROM assets 
                                WHERE name LIKE %s 
                                LIMIT 1
                            """, (f'%{source_table}%',))
                            source_asset = cursor.fetchone()
                            
                            if not source_asset:
                                continue
                            
                            source_asset_id = source_asset['id']
                            
                            # Check if relationship already exists
                            cursor.execute("""
                                SELECT id FROM lineage_relationships 
                                WHERE source_asset_id = %s AND target_asset_id = %s
                            """, (source_asset_id, target_asset_id))
                            
                            if cursor.fetchone():
                                skipped_count += 1
                                continue
                            
                            # Create relationship
                            cursor.execute("""
                                INSERT INTO lineage_relationships (
                                    source_asset_id, target_asset_id, relationship_type,
                                    source_type, target_type, column_lineage,
                                    transformation_type, transformation_description,
                                    source_system, source_job_name,
                                    confidence_score, extraction_method, discovered_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                                )
                            """, (
                                source_asset_id,
                                target_asset_id,
                                'transformation',
                                'table',  # source_type
                                'table',  # target_type
                                json.dumps(lineage_result.get('column_lineage', [])),
                                lineage_result.get('query_type', 'SELECT'),
                                f'Extracted from SQL in {os.path.basename(sql_data.get("source_file", "unknown"))}',
                                'airflow',
                                os.path.basename(sql_data.get("source_file", "unknown")),
                                lineage_result.get('confidence_score', 0.8),
                                'sql_parsing'
                            ))
                            
                            created_count += 1
                    
                    except Exception as e:
                        logger.warning('FN:parse_sql_and_create_lineage sql_query_error:{}'.format(str(e)))
                        skipped_count += 1
                        continue
                
                conn.commit()
                
                logger.info('FN:parse_sql_and_create_lineage created:{} skipped:{}'.format(
                    created_count, skipped_count
                ))
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error('FN:parse_sql_and_create_lineage error:{}'.format(str(e)), exc_info=True)


# Define DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_lineage_extraction',
    default_args=default_args,
    description='Automatically extract SQL queries from DAG files and create lineage relationships',
    schedule_interval=timedelta(hours=1),  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lineage', 'sql', 'automation'],
)

extract_sql_task = PythonOperator(
    task_id='extract_sql_and_create_lineage',
    python_callable=parse_sql_and_create_lineage,
    dag=dag,
)


