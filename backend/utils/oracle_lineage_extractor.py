"""
Advanced Oracle Lineage Extraction Service
Extracts actual data lineage using multiple advanced methods:
1. Column-level lineage from SQL parsing
2. JOIN analysis
3. ML-based inference
4. Stored procedure/function parsing
5. Trigger analysis
6. Shared column matching
7. Column usage tracking
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

try:
    from utils.sql_lineage_extractor import extract_lineage_from_sql, get_lineage_extractor
    SQL_LINEAGE_AVAILABLE = True
except ImportError:
    SQL_LINEAGE_AVAILABLE = False
    logger.warning("SQL lineage extractor not available")

try:
    from utils.ml_lineage_inference import infer_relationships_ml, fuzzy_column_match
    ML_INFERENCE_AVAILABLE = True
except ImportError:
    ML_INFERENCE_AVAILABLE = False
    logger.warning("ML inference not available")

try:
    from utils.stored_procedure_parser import StoredProcedureParser
    PROCEDURE_PARSER_AVAILABLE = True
except ImportError:
    PROCEDURE_PARSER_AVAILABLE = False
    logger.warning("Stored procedure parser not available")


class OracleLineageExtractor:
    """Advanced Oracle lineage extraction using multiple methods"""
    
    def __init__(self, oracle_client):
        self.client = oracle_client
        self.procedure_parser = StoredProcedureParser() if PROCEDURE_PARSER_AVAILABLE else None
    
    def extract_comprehensive_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict,
        progress_callback: Optional[callable] = None
    ) -> List[Dict]:
        """
        Extract comprehensive lineage using all advanced methods.
        Returns list of lineage relationship dictionaries.
        """
        all_lineage = []
        
        # Method 1: Column-level lineage from SQL parsing (Views, MViews)
        if progress_callback:
            progress_callback("Extracting column-level lineage from SQL...")
        sql_lineage = self._extract_sql_column_lineage(schema, connector_id, asset_map)
        all_lineage.extend(sql_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:sql_column_lineage found:{len(sql_lineage)} relationships')
        
        # Method 2: JOIN analysis
        if progress_callback:
            progress_callback("Analyzing JOIN relationships...")
        join_lineage = self._extract_join_relationships(schema, connector_id, asset_map)
        all_lineage.extend(join_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:join_analysis found:{len(join_lineage)} relationships')
        
        # Method 3: ML-based inference for column matching
        if progress_callback:
            progress_callback("Inferring relationships using ML...")
        ml_lineage = self._extract_ml_inferred_lineage(schema, connector_id, asset_map)
        all_lineage.extend(ml_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:ml_inference found:{len(ml_lineage)} relationships')
        
        # Method 4: Stored procedure/function parsing
        if progress_callback:
            progress_callback("Parsing stored procedures and functions...")
        proc_lineage = self._extract_procedure_lineage(schema, connector_id, asset_map)
        all_lineage.extend(proc_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:procedure_parsing found:{len(proc_lineage)} relationships')
        
        # Method 5: Trigger analysis
        if progress_callback:
            progress_callback("Analyzing triggers...")
        trigger_lineage = self._extract_trigger_lineage(schema, connector_id, asset_map)
        all_lineage.extend(trigger_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:trigger_analysis found:{len(trigger_lineage)} relationships')
        
        # Method 6: Shared column matching
        if progress_callback:
            progress_callback("Finding shared columns...")
        shared_lineage = self._extract_shared_column_lineage(schema, connector_id, asset_map)
        all_lineage.extend(shared_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:shared_columns found:{len(shared_lineage)} relationships')
        
        # Method 7: Column usage tracking
        if progress_callback:
            progress_callback("Tracking column usage...")
        usage_lineage = self._extract_column_usage_lineage(schema, connector_id, asset_map)
        all_lineage.extend(usage_lineage)
        logger.info(f'FN:extract_comprehensive_lineage method:column_usage found:{len(usage_lineage)} relationships')
        
        # Deduplicate based on (source_asset_id, target_asset_id, source_job_id)
        deduplicated = self._deduplicate_lineage(all_lineage)
        logger.info(f'FN:extract_comprehensive_lineage total:{len(all_lineage)} deduplicated:{len(deduplicated)}')
        
        return deduplicated
    
    def _extract_sql_column_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract column-level lineage from SQL in views and materialized views"""
        lineage = []
        
        if not SQL_LINEAGE_AVAILABLE:
            return lineage
        
        try:
            # Extract from views
            views = self.client.list_views(schema)
            for view_info in views:
                view_name = view_info['view_name']
                view_id = f"{connector_id}_{schema}.{view_name}"
                
                if view_id not in asset_map:
                    continue
                
                view_def = self.client.get_view_definition(schema, view_name)
                if not view_def:
                    continue
                
                try:
                    lineage_result = extract_lineage_from_sql(view_def, dialect='oracle')
                    source_tables = lineage_result.get('source_tables', [])
                    column_lineage = lineage_result.get('column_lineage', [])
                    
                    for source_table in source_tables:
                        source_schema = source_table.get('schema') or schema
                        source_name = source_table.get('table', '')
                        source_id = f"{connector_id}_{source_schema}.{source_name}"
                        
                        if source_id in asset_map:
                            lineage.append({
                                "source_asset_id": source_id,
                                "target_asset_id": view_id,
                                "relationship_type": "view",
                                "source_type": "table",
                                "target_type": "view",
                                "column_lineage": column_lineage,
                                "transformation_type": "view",
                                "transformation_description": f"View definition: {view_name}",
                                "sql_query": view_def,
                                "source_system": "oracle_db",
                                "source_job_id": f"oracle_view_sql_{view_name}",
                                "source_job_name": "Oracle View SQL Parsing",
                                "confidence_score": 0.95,
                                "extraction_method": "sql_column_parsing",
                                "discovered_at": datetime.utcnow()
                            })
                except Exception as e:
                    logger.warning(f'FN:_extract_sql_column_lineage view:{view_name} error:{str(e)}')
            
            # Extract from materialized views
            mviews = self.client.list_materialized_views(schema)
            for mview_info in mviews:
                mview_name = mview_info['mview_name']
                mview_id = f"{connector_id}_{schema}.{mview_name}"
                
                if mview_id not in asset_map:
                    continue
                
                mview_def = self.client.get_materialized_view_definition(schema, mview_name)
                if not mview_def:
                    continue
                
                try:
                    lineage_result = extract_lineage_from_sql(mview_def, dialect='oracle')
                    source_tables = lineage_result.get('source_tables', [])
                    column_lineage = lineage_result.get('column_lineage', [])
                    
                    for source_table in source_tables:
                        source_schema = source_table.get('schema') or schema
                        source_name = source_table.get('table', '')
                        source_id = f"{connector_id}_{source_schema}.{source_name}"
                        
                        if source_id in asset_map:
                            lineage.append({
                                "source_asset_id": source_id,
                                "target_asset_id": mview_id,
                                "relationship_type": "materialized_view",
                                "source_type": "table",
                                "target_type": "materialized_view",
                                "column_lineage": column_lineage,
                                "transformation_type": "materialized_view",
                                "transformation_description": f"Materialized view: {mview_name}",
                                "sql_query": mview_def,
                                "source_system": "oracle_db",
                                "source_job_id": f"oracle_mview_sql_{mview_name}",
                                "source_job_name": "Oracle MView SQL Parsing",
                                "confidence_score": 0.95,
                                "extraction_method": "sql_column_parsing",
                                "discovered_at": datetime.utcnow()
                            })
                except Exception as e:
                    logger.warning(f'FN:_extract_sql_column_lineage mview:{mview_name} error:{str(e)}')
        
        except Exception as e:
            logger.error(f'FN:_extract_sql_column_lineage schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _extract_join_relationships(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract JOIN relationships from views and materialized views"""
        lineage = []
        
        if not SQL_LINEAGE_AVAILABLE:
            return lineage
        
        try:
            views = self.client.list_views(schema)
            for view_info in views:
                view_name = view_info['view_name']
                view_id = f"{connector_id}_{schema}.{view_name}"
                
                if view_id not in asset_map:
                    continue
                
                view_def = self.client.get_view_definition(schema, view_name)
                if not view_def:
                    continue
                
                # Extract JOIN conditions
                join_keys = self._extract_join_keys(view_def)
                
                if join_keys:
                    # Create relationships between joined tables
                    for join_key in join_keys:
                        table1_id = f"{connector_id}_{schema}.{join_key['table1']}"
                        table2_id = f"{connector_id}_{schema}.{join_key['table2']}"
                        
                        # Both tables feed into the view
                        for source_id in [table1_id, table2_id]:
                            if source_id in asset_map and source_id != view_id:
                                lineage.append({
                                    "source_asset_id": source_id,
                                    "target_asset_id": view_id,
                                    "relationship_type": "join",
                                    "source_type": "table",
                                    "target_type": "view",
                                    "column_lineage": [{
                                        "source_column": join_key['column1'] if source_id == table1_id else join_key['column2'],
                                        "target_column": join_key['column1'] if source_id == table1_id else join_key['column2'],
                                        "transformation": "join_key",
                                        "transformation_type": "join"
                                    }],
                                    "transformation_type": "join",
                                    "transformation_description": f"JOIN relationship via {join_key['column1']} = {join_key['column2']}",
                                    "sql_query": view_def,
                                    "source_system": "oracle_db",
                                    "source_job_id": f"oracle_join_{view_name}_{join_key['table1']}_{join_key['table2']}",
                                    "source_job_name": "Oracle JOIN Analysis",
                                    "confidence_score": 0.9,
                                    "extraction_method": "join_analysis",
                                    "discovered_at": datetime.utcnow()
                                })
        
        except Exception as e:
            logger.error(f'FN:_extract_join_relationships schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _extract_join_keys(self, sql_text: str) -> List[Dict]:
        """Extract JOIN keys from SQL text"""
        join_keys = []
        
        # Pattern: table1.col1 = table2.col2 or table1.col1 = table2.col2
        # Also handle: t1.col1 = t2.col2 (with aliases)
        patterns = [
            r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',  # table.col = table.col
            r'(\w+)\.(\w+)\s*=\s*(\w+)',  # table.col = col (self-join)
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, sql_text, re.IGNORECASE)
            for match in matches:
                groups = match.groups()
                if len(groups) >= 4:
                    table1, col1, table2, col2 = groups[:4]
                    join_keys.append({
                        'table1': table1,
                        'column1': col1,
                        'table2': table2,
                        'column2': col2
                    })
        
        return join_keys
    
    def _extract_ml_inferred_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract lineage using ML-based inference for column matching"""
        lineage = []
        
        if not ML_INFERENCE_AVAILABLE:
            return lineage
        
        try:
            # Get all tables in schema
            tables = self.client.list_tables(schema)
            
            # Compare each pair of tables
            for i, table1_info in enumerate(tables):
                table1_name = table1_info['table_name']
                table1_id = f"{connector_id}_{schema}.{table1_name}"
                
                if table1_id not in asset_map:
                    continue
                
                table1_asset = asset_map[table1_id]
                table1_cols = table1_asset.columns if hasattr(table1_asset, 'columns') else []
                
                for j, table2_info in enumerate(tables[i+1:], start=i+1):
                    table2_name = table2_info['table_name']
                    table2_id = f"{connector_id}_{schema}.{table2_name}"
                    
                    if table2_id not in asset_map:
                        continue
                    
                    table2_asset = asset_map[table2_id]
                    table2_cols = table2_asset.columns if hasattr(table2_asset, 'columns') else []
                    
                    # Use ML inference to find relationships
                    inferred_lineage, confidence = infer_relationships_ml(
                        table1_cols,
                        table2_cols,
                        min_matching_ratio=0.3
                    )
                    
                    if inferred_lineage and confidence >= 0.6:
                        # Create bidirectional relationships (lower confidence)
                        for direction in ['forward', 'reverse']:
                            source_id = table1_id if direction == 'forward' else table2_id
                            target_id = table2_id if direction == 'forward' else table1_id
                            source_cols = table1_cols if direction == 'forward' else table2_cols
                            target_cols = table2_cols if direction == 'forward' else table1_cols
                            
                            # Re-infer for reverse direction
                            if direction == 'reverse':
                                inferred_lineage, confidence = infer_relationships_ml(
                                    source_cols,
                                    target_cols,
                                    min_matching_ratio=0.3
                                )
                            
                            if inferred_lineage and confidence >= 0.6:
                                lineage.append({
                                    "source_asset_id": source_id,
                                    "target_asset_id": target_id,
                                    "relationship_type": "inferred",
                                    "source_type": "table",
                                    "target_type": "table",
                                    "column_lineage": inferred_lineage,
                                    "transformation_type": "ml_inference",
                                    "transformation_description": f"ML-inferred relationship based on column similarity (confidence: {confidence:.2f})",
                                    "sql_query": None,
                                    "source_system": "oracle_db",
                                    "source_job_id": f"oracle_ml_{table1_name}_{table2_name}",
                                    "source_job_name": "Oracle ML Inference",
                                    "confidence_score": min(confidence, 0.85),
                                    "extraction_method": "ml_inference",
                                    "discovered_at": datetime.utcnow()
                                })
        
        except Exception as e:
            logger.error(f'FN:_extract_ml_inferred_lineage schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _extract_procedure_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract lineage from stored procedures and functions"""
        lineage = []
        
        if not PROCEDURE_PARSER_AVAILABLE or not self.procedure_parser:
            return lineage
        
        try:
            # Get procedures
            procedures = self.client.list_procedures(schema)
            for proc_info in procedures:
                proc_name = proc_info['object_name']
                proc_subname = proc_info.get('procedure_name')
                full_name = f"{proc_name}.{proc_subname}" if proc_subname else proc_name
                proc_id = f"{connector_id}_{schema}.{full_name}"
                
                if proc_id not in asset_map:
                    continue
                
                # Get procedure source code
                proc_source = self.client.get_procedure_source(schema, proc_name, proc_subname)
                if not proc_source:
                    continue
                
                try:
                    # Parse procedure code
                    proc_lineage = self.procedure_parser.extract_lineage_from_procedure(
                        proc_source,
                        language='plsql',
                        procedure_name=full_name
                    )
                    
                    source_tables = proc_lineage.get('source_tables', [])
                    target_tables = proc_lineage.get('target_tables', [])
                    column_lineage = proc_lineage.get('column_lineage', [])
                    
                    # Create relationships from source to target
                    for source_table in source_tables:
                        source_schema = source_table.get('schema') or schema
                        source_name = source_table.get('table', '')
                        source_id = f"{connector_id}_{source_schema}.{source_name}"
                        
                        for target_table in target_tables:
                            target_schema = target_table.get('schema') or schema
                            target_name = target_table.get('table', '')
                            target_id = f"{connector_id}_{target_schema}.{target_name}"
                            
                            if source_id in asset_map and target_id in asset_map:
                                lineage.append({
                                    "source_asset_id": source_id,
                                    "target_asset_id": target_id,
                                    "relationship_type": "procedure",
                                    "source_type": "table",
                                    "target_type": "table",
                                    "column_lineage": column_lineage,
                                    "transformation_type": "procedure",
                                    "transformation_description": f"Stored procedure: {full_name}",
                                    "sql_query": proc_source,
                                    "source_system": "oracle_db",
                                    "source_job_id": f"oracle_proc_{full_name}",
                                    "source_job_name": "Oracle Procedure Parsing",
                                    "confidence_score": 0.85,
                                    "extraction_method": "procedure_parsing",
                                    "discovered_at": datetime.utcnow()
                                })
                            
                            # Also create relationship from source to procedure itself
                            if source_id in asset_map:
                                lineage.append({
                                    "source_asset_id": source_id,
                                    "target_asset_id": proc_id,
                                    "relationship_type": "procedure",
                                    "source_type": "table",
                                    "target_type": "procedure",
                                    "column_lineage": column_lineage,
                                    "transformation_type": "procedure",
                                    "transformation_description": f"Used in procedure: {full_name}",
                                    "sql_query": proc_source,
                                    "source_system": "oracle_db",
                                    "source_job_id": f"oracle_proc_input_{full_name}",
                                    "source_job_name": "Oracle Procedure Input",
                                    "confidence_score": 0.85,
                                    "extraction_method": "procedure_parsing",
                                    "discovered_at": datetime.utcnow()
                                })
                
                except Exception as e:
                    logger.warning(f'FN:_extract_procedure_lineage proc:{full_name} error:{str(e)}')
            
            # Get functions
            functions = self.client.list_functions(schema)
            for func_info in functions:
                func_name = func_info['function_name']
                func_id = f"{connector_id}_{schema}.{func_name}"
                
                if func_id not in asset_map:
                    continue
                
                # Get function source code
                func_source = self.client.get_function_source(schema, func_name)
                if not func_source:
                    continue
                
                try:
                    # Parse function code
                    func_lineage = self.procedure_parser.extract_lineage_from_procedure(
                        func_source,
                        language='plsql',
                        procedure_name=func_name
                    )
                    
                    source_tables = func_lineage.get('source_tables', [])
                    column_lineage = func_lineage.get('column_lineage', [])
                    
                    # Functions typically read from tables
                    for source_table in source_tables:
                        source_schema = source_table.get('schema') or schema
                        source_name = source_table.get('table', '')
                        source_id = f"{connector_id}_{source_schema}.{source_name}"
                        
                        if source_id in asset_map:
                            lineage.append({
                                "source_asset_id": source_id,
                                "target_asset_id": func_id,
                                "relationship_type": "function",
                                "source_type": "table",
                                "target_type": "function",
                                "column_lineage": column_lineage,
                                "transformation_type": "function",
                                "transformation_description": f"Used in function: {func_name}",
                                "sql_query": func_source,
                                "source_system": "oracle_db",
                                "source_job_id": f"oracle_func_{func_name}",
                                "source_job_name": "Oracle Function Parsing",
                                "confidence_score": 0.85,
                                "extraction_method": "procedure_parsing",
                                "discovered_at": datetime.utcnow()
                            })
                
                except Exception as e:
                    logger.warning(f'FN:_extract_procedure_lineage func:{func_name} error:{str(e)}')
        
        except Exception as e:
            logger.error(f'FN:_extract_procedure_lineage schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _extract_trigger_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract lineage from triggers"""
        lineage = []
        
        try:
            triggers = self.client.list_triggers(schema)
            for trigger_info in triggers:
                trigger_name = trigger_info['trigger_name']
                table_name = trigger_info['table_name']
                trigger_id = f"{connector_id}_{schema}.{trigger_name}"
                table_id = f"{connector_id}_{schema}.{table_name}"
                
                if trigger_id not in asset_map or table_id not in asset_map:
                    continue
                
                # Get trigger source code
                trigger_source = self.client.get_trigger_source(schema, trigger_name)
                if not trigger_source:
                    continue
                
                # Parse trigger for table references
                if SQL_LINEAGE_AVAILABLE:
                    try:
                        trigger_lineage = extract_lineage_from_sql(trigger_source, dialect='oracle')
                        source_tables = trigger_lineage.get('source_tables', [])
                        column_lineage = trigger_lineage.get('column_lineage', [])
                        
                        # Trigger is on table, so table -> trigger relationship
                        lineage.append({
                            "source_asset_id": table_id,
                            "target_asset_id": trigger_id,
                            "relationship_type": "trigger",
                            "source_type": "table",
                            "target_type": "trigger",
                            "column_lineage": column_lineage,
                            "transformation_type": "trigger",
                            "transformation_description": f"Trigger on table: {table_name}",
                            "sql_query": trigger_source,
                            "source_system": "oracle_db",
                            "source_job_id": f"oracle_trigger_{trigger_name}",
                            "source_job_name": "Oracle Trigger Analysis",
                            "confidence_score": 0.9,
                            "extraction_method": "trigger_analysis",
                            "discovered_at": datetime.utcnow()
                        })
                        
                        # Find tables referenced in trigger body
                        for source_table in source_tables:
                            source_schema = source_table.get('schema') or schema
                            source_name = source_table.get('table', '')
                            source_id = f"{connector_id}_{source_schema}.{source_name}"
                            
                            if source_id in asset_map and source_id != table_id:
                                lineage.append({
                                    "source_asset_id": source_id,
                                    "target_asset_id": trigger_id,
                                    "relationship_type": "trigger",
                                    "source_type": "table",
                                    "target_type": "trigger",
                                    "column_lineage": column_lineage,
                                    "transformation_type": "trigger",
                                    "transformation_description": f"Referenced in trigger: {trigger_name}",
                                    "sql_query": trigger_source,
                                    "source_system": "oracle_db",
                                    "source_job_id": f"oracle_trigger_ref_{trigger_name}",
                                    "source_job_name": "Oracle Trigger Reference",
                                    "confidence_score": 0.85,
                                    "extraction_method": "trigger_analysis",
                                    "discovered_at": datetime.utcnow()
                                })
                    except Exception as e:
                        logger.warning(f'FN:_extract_trigger_lineage trigger:{trigger_name} error:{str(e)}')
        
        except Exception as e:
            logger.error(f'FN:_extract_trigger_lineage schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _extract_shared_column_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract lineage based on shared columns between tables"""
        lineage = []
        
        try:
            # Get all tables
            tables = self.client.list_tables(schema)
            
            # Build column maps
            table_columns = {}
            for table_info in tables:
                table_name = table_info['table_name']
                table_id = f"{connector_id}_{schema}.{table_name}"
                
                if table_id not in asset_map:
                    continue
                
                columns = self.client.get_table_columns(schema, table_name)
                table_columns[table_id] = {
                    'name': table_name,
                    'columns': {col['name'].lower(): col for col in columns}
                }
            
            # Compare tables for shared columns
            table_list = list(table_columns.items())
            for i, (table1_id, table1_info) in enumerate(table_list):
                for j, (table2_id, table2_info) in enumerate(table_list[i+1:], start=i+1):
                    # Find shared columns
                    shared_cols = []
                    for col_name, col_info in table1_info['columns'].items():
                        if col_name in table2_info['columns']:
                            col2_info = table2_info['columns'][col_name]
                            # Check if data types match
                            if col_info.get('type') == col2_info.get('type'):
                                shared_cols.append({
                                    'column_name': col_info['name'],
                                    'data_type': col_info.get('type')
                                })
                    
                    # If significant shared columns, create relationship
                    if len(shared_cols) >= 2:  # At least 2 shared columns
                        column_lineage = [{
                            "source_column": col['column_name'],
                            "target_column": col['column_name'],
                            "transformation": "shared_column",
                            "transformation_type": "pass_through"
                        } for col in shared_cols]
                        
                        # Use ML inference to boost confidence
                        confidence = 0.7
                        if ML_INFERENCE_AVAILABLE:
                            table1_asset = asset_map.get(table1_id)
                            table2_asset = asset_map.get(table2_id)
                            if table1_asset and table2_asset:
                                table1_cols = table1_asset.columns if hasattr(table1_asset, 'columns') else []
                                table2_cols = table2_asset.columns if hasattr(table2_asset, 'columns') else []
                                _, ml_confidence = infer_relationships_ml(table1_cols, table2_cols, min_matching_ratio=0.2)
                                confidence = max(confidence, min(ml_confidence, 0.85))
                        
                        # Create bidirectional relationships
                        for direction in ['forward', 'reverse']:
                            source_id = table1_id if direction == 'forward' else table2_id
                            target_id = table2_id if direction == 'forward' else table1_id
                            
                            lineage.append({
                                "source_asset_id": source_id,
                                "target_asset_id": target_id,
                                "relationship_type": "shared_columns",
                                "source_type": "table",
                                "target_type": "table",
                                "column_lineage": column_lineage,
                                "transformation_type": "shared_columns",
                                "transformation_description": f"Shared {len(shared_cols)} columns: {', '.join([c['column_name'] for c in shared_cols[:5]])}",
                                "sql_query": None,
                                "source_system": "oracle_db",
                                "source_job_id": f"oracle_shared_cols_{table1_info['name']}_{table2_info['name']}",
                                "source_job_name": "Oracle Shared Columns Analysis",
                                "confidence_score": confidence,
                                "extraction_method": "column_matching",
                                "discovered_at": datetime.utcnow()
                            })
        
        except Exception as e:
            logger.error(f'FN:_extract_shared_column_lineage schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _extract_column_usage_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract lineage by tracking column usage in SQL"""
        lineage = []
        
        try:
            # Get all SQL source code from ALL_SOURCE
            with self.client.engine.connect() as conn:
                from sqlalchemy import text
                query = text("""
                    SELECT 
                        owner,
                        name,
                        type,
                        line,
                        text
                    FROM all_source
                    WHERE owner = :schema
                        AND type IN ('VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'TRIGGER')
                    ORDER BY owner, name, type, line
                """)
                result = conn.execute(query, {"schema": schema})
                
                # Group by object
                objects_sql = {}
                for row in result:
                    owner, name, obj_type, line, text_val = row
                    key = f"{owner}.{name}.{obj_type}"
                    if key not in objects_sql:
                        objects_sql[key] = {
                            'owner': owner,
                            'name': name,
                            'type': obj_type,
                            'sql_text': []
                        }
                    objects_sql[key]['sql_text'].append(text_val)
                
                # Extract table.column references
                for key, obj_info in objects_sql.items():
                    full_sql = ' '.join(obj_info['sql_text'])
                    obj_id = f"{connector_id}_{obj_info['owner']}.{obj_info['name']}"
                    
                    if obj_id not in asset_map:
                        continue
                    
                    # Pattern: schema.table.column or table.column
                    pattern = r'(\w+)\.(\w+)\.(\w+)|(\w+)\.(\w+)'
                    matches = re.finditer(pattern, full_sql, re.IGNORECASE)
                    
                    column_usage = defaultdict(set)
                    for match in matches:
                        if match.group(1):  # schema.table.column
                            ref_schema = match.group(1)
                            ref_table = match.group(2)
                            ref_column = match.group(3)
                        else:  # table.column
                            ref_schema = obj_info['owner']
                            ref_table = match.group(4)
                            ref_column = match.group(5)
                        
                        ref_id = f"{connector_id}_{ref_schema}.{ref_table}"
                        if ref_id in asset_map and ref_id != obj_id:
                            column_usage[ref_id].add(ref_column)
                    
                    # Create lineage relationships
                    for ref_id, columns in column_usage.items():
                        column_lineage = [{
                            "source_column": col,
                            "target_column": col,
                            "transformation": "column_usage",
                            "transformation_type": "pass_through"
                        } for col in columns]
                        
                        lineage.append({
                            "source_asset_id": ref_id,
                            "target_asset_id": obj_id,
                            "relationship_type": obj_info['type'].lower(),
                            "source_type": "table",
                            "target_type": obj_info['type'].lower(),
                            "column_lineage": column_lineage,
                            "transformation_type": "column_usage",
                            "transformation_description": f"Columns used in {obj_info['type']}: {', '.join(list(columns)[:5])}",
                            "sql_query": full_sql[:1000],  # Limit SQL length
                            "source_system": "oracle_db",
                            "source_job_id": f"oracle_col_usage_{obj_info['name']}",
                            "source_job_name": "Oracle Column Usage Analysis",
                            "confidence_score": 0.85,
                            "extraction_method": "sql_column_analysis",
                            "discovered_at": datetime.utcnow()
                        })
        
        except Exception as e:
            logger.error(f'FN:_extract_column_usage_lineage schema:{schema} error:{str(e)}')
        
        return lineage
    
    def _deduplicate_lineage(self, lineage: List[Dict]) -> List[Dict]:
        """Deduplicate lineage based on (source_asset_id, target_asset_id, source_job_id)"""
        seen = set()
        deduplicated = []
        
        for rel in lineage:
            key = (
                rel.get('source_asset_id'),
                rel.get('target_asset_id'),
                rel.get('source_job_id')
            )
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(rel)
            else:
                # If duplicate, keep the one with higher confidence
                existing = next((r for r in deduplicated if (
                    r.get('source_asset_id') == rel.get('source_asset_id') and
                    r.get('target_asset_id') == rel.get('target_asset_id') and
                    r.get('source_job_id') == rel.get('source_job_id')
                )), None)
                
                if existing and rel.get('confidence_score', 0) > existing.get('confidence_score', 0):
                    # Replace with higher confidence
                    deduplicated.remove(existing)
                    deduplicated.append(rel)
        
        return deduplicated
    
    def _extract_folder_hierarchy_lineage(
        self,
        schema: str,
        connector_id: str,
        asset_map: Dict
    ) -> List[Dict]:
        """Extract folder hierarchy relationships based on schema/table structure"""
        lineage = []
        
        try:
            # Group assets by schema and type
            schema_tables = []
            schema_views = []
            schema_mviews = []
            
            for asset_id, asset in asset_map.items():
                if not asset_id.startswith(connector_id):
                    continue
                
                asset_schema = None
                if hasattr(asset, 'catalog') and asset.catalog:
                    asset_schema = asset.catalog
                elif '.' in asset_id:
                    # Extract schema from asset_id format: connector_id_SCHEMA.TABLE
                    parts = asset_id.replace(f"{connector_id}_", "").split('.', 1)
                    if len(parts) == 2:
                        asset_schema = parts[0]
                
                if asset_schema != schema:
                    continue
                
                asset_type = None
                if hasattr(asset, 'type'):
                    asset_type = asset.type
                
                asset_name = None
                if hasattr(asset, 'name'):
                    asset_name = asset.name
                elif '.' in asset_id:
                    parts = asset_id.split('.', 1)
                    if len(parts) == 2:
                        asset_name = parts[1]
                
                if not asset_name:
                    continue
                
                if asset_type == 'table':
                    schema_tables.append({
                        'asset_id': asset_id,
                        'name': asset_name,
                        'asset': asset
                    })
                elif asset_type == 'view':
                    schema_views.append({
                        'asset_id': asset_id,
                        'name': asset_name,
                        'asset': asset
                    })
                elif asset_type == 'materialized_view':
                    schema_mviews.append({
                        'asset_id': asset_id,
                        'name': asset_name,
                        'asset': asset
                    })
            
            # Create hierarchy: tables -> views, tables -> mviews
            # This represents data flow: base tables feed into views/mviews
            for table_info in schema_tables:
                table_id = table_info['asset_id']
                
                # Tables -> Views (views depend on tables)
                for view_info in schema_views:
                    view_id = view_info['asset_id']
                    lineage.append({
                        "source_asset_id": table_id,
                        "target_asset_id": view_id,
                        "relationship_type": "folder_hierarchy",
                        "source_type": "table",
                        "target_type": "view",
                        "column_lineage": None,
                        "transformation_type": "folder_hierarchy",
                        "transformation_description": f"Schema hierarchy: {schema}.{table_info['name']} -> {schema}.{view_info['name']}",
                        "sql_query": None,
                        "source_system": "oracle_db",
                        "source_job_id": f"oracle_folder_hierarchy_{schema}_{table_info['name']}_{view_info['name']}",
                        "source_job_name": "Oracle Folder Hierarchy",
                        "confidence_score": 0.6,
                        "extraction_method": "folder_hierarchy",
                        "discovered_at": datetime.utcnow()
                    })
                
                # Tables -> Materialized Views
                for mview_info in schema_mviews:
                    mview_id = mview_info['asset_id']
                    lineage.append({
                        "source_asset_id": table_id,
                        "target_asset_id": mview_id,
                        "relationship_type": "folder_hierarchy",
                        "source_type": "table",
                        "target_type": "materialized_view",
                        "column_lineage": None,
                        "transformation_type": "folder_hierarchy",
                        "transformation_description": f"Schema hierarchy: {schema}.{table_info['name']} -> {schema}.{mview_info['name']}",
                        "sql_query": None,
                        "source_system": "oracle_db",
                        "source_job_id": f"oracle_folder_hierarchy_{schema}_{table_info['name']}_{mview_info['name']}",
                        "source_job_name": "Oracle Folder Hierarchy",
                        "confidence_score": 0.6,
                        "extraction_method": "folder_hierarchy",
                        "discovered_at": datetime.utcnow()
                    })
        
        except Exception as e:
            logger.error(f'FN:_extract_folder_hierarchy_lineage schema:{schema} error:{str(e)}')
        
        return lineage

