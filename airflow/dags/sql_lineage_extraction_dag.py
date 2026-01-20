from datetime import datetime, timedelta
from typing import List, Dict
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os
import re
import json
import pymysql
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger(__name__)


DB_CONFIG = config.DB_CONFIG


def extract_sql_from_dag_file(file_path: str) -> List[Dict]:
    sql_queries = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
        

        sql_patterns = [
            r'["\']([^"\']*SELECT[^"\']*)["\']',
            r'sql\s*=\s*["\']([^"\']*)["\']',
            r'query\s*=\s*["\']([^"\']*)["\']',
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
                            'context': line.strip()[:100]
                        })
        
        logger.info('FN:extract_sql_from_dag_file file:{} queries_found:{}'.format(
            file_path, len(sql_queries)
        ))
        
    except Exception as e:
        logger.error('FN:extract_sql_from_dag_file file:{} error:{}'.format(file_path, str(e)))
    
    return sql_queries


def scan_dag_files_for_sql():
    dags_folder = os.path.join(os.path.dirname(__file__))
    all_sql_queries = []
    

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
    try:

        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))
        from utils.sql_lineage_extractor import extract_lineage_from_sql
        

        sql_queries = scan_dag_files_for_sql()
        
        if not sql_queries:
            logger.info('FN:parse_sql_and_create_lineage message:No SQL queries found in DAG files')
            return
        

        conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        
        try:
            with conn.cursor() as cursor:
                created_count = 0
                skipped_count = 0
                
                for sql_data in sql_queries:
                    sql_query = sql_data['sql']
                    
                    try:

                        lineage_result = extract_lineage_from_sql(sql_query, dialect='mysql')
                        
                        if not lineage_result.get('target_table') or not lineage_result.get('source_tables'):
                            continue
                        
                        target_table = lineage_result['target_table']
                        source_tables = lineage_result.get('source_tables', [])
                        

                        # Get target asset ID
                        cursor.execute("""
                            SELECT id FROM assets 
                            WHERE name LIKE %s 
                            LIMIT 1
                        """, (target_table,))
                        target_result = cursor.fetchone()
                        if not target_result:
                            continue
                        target_asset_id = target_result['id']
                        
                        # Process each source table
                        for source_table in source_tables:
                            cursor.execute("""
                                SELECT id FROM assets 
                                WHERE name LIKE %s 
                                LIMIT 1
                            """, (source_table,))
                            source_result = cursor.fetchone()
                            if not source_result:
                                continue
                            source_asset_id = source_result['id']
                            
                            # Check if relationship already exists
                            cursor.execute("""
                                SELECT id FROM lineage_relationships 
                                WHERE source_asset_id = %s AND target_asset_id = %s
                            """, (source_asset_id, target_asset_id))
                            if cursor.fetchone():
                                continue
                            
                            # Insert new relationship
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
                                source_asset_id, target_asset_id, 'transformation',
                                'table', 'table', json.dumps([]),
                                None, None,
                                'sql_parsing', None,
                                1.0, 'sql_parsing'
                            ))
                            created_count += 1
                        
                        # Commit after processing all relationships
                        conn.commit()
                    except Exception as e:
                        logger.error(f'Error processing SQL query: {e}')
                        skipped_count += 1
                        continue
                
                logger.info(f'Created {created_count} lineage relationships, skipped {skipped_count}')
        except Exception as e:
            logger.error(f'Error in SQL lineage extraction: {e}')
            return 0
        finally:
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f'Error in parse_sql_and_create_lineage: {e}')
        return 0