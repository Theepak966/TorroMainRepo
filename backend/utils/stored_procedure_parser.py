"""
Stored Procedure Lineage Extractor
Handles PL/SQL, T-SQL, and other stored procedure languages.
Extracts lineage from stored procedures including dynamic SQL.
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict

try:
    import sqlglot
    from sqlglot import parse_one, exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

logger = logging.getLogger(__name__)


class StoredProcedureParser:
    """Extract lineage from stored procedures (PL/SQL, T-SQL, etc.)"""
    
    def __init__(self):
        self.dynamic_sql_patterns = {
            'oracle': [
                r'EXECUTE\s+IMMEDIATE\s+([^;]+)',
                r'EXEC\s+IMMEDIATE\s+([^;]+)',
                r'DBMS_SQL\.PARSE\s*\([^,]+,\s*([^,]+)',
            ],
            'sqlserver': [
                r'sp_executesql\s+@?(\w+)\s*[,)]',
                r'EXEC\s+sp_executesql\s+@?(\w+)',
                r'EXECUTE\s+sp_executesql\s+@?(\w+)',
            ],
            'postgres': [
                r'EXECUTE\s+([^;]+)',
                r'EXEC\s+([^;]+)',
            ]
        }
    
    def extract_lineage_from_procedure(
        self, 
        procedure_code: str, 
        language: str = 'plsql',
        procedure_name: Optional[str] = None
    ) -> Dict:
        """
        Extract lineage from stored procedure.
        
        Args:
            procedure_code: Full procedure code
            language: 'plsql', 'tsql', 'postgres', etc.
            procedure_name: Name of the procedure
        
        Returns:
            Dict with extracted lineage information
        """
        result = {
            'procedure_name': procedure_name,
            'language': language,
            'static_sql_lineage': [],
            'dynamic_sql_lineage': [],
            'procedure_dependencies': [],
            'all_source_tables': [],
            'all_target_tables': [],
            'column_lineage': [],
            'confidence_score': 0.7,
            'extraction_method': 'stored_procedure_parsing'
        }
        
        try:
            # Extract static SQL statements
            static_sql = self._extract_static_sql(procedure_code, language)
            for sql_stmt in static_sql:
                lineage = self._parse_sql_statement(sql_stmt, language)
                if lineage:
                    result['static_sql_lineage'].append(lineage)
                    # Handle both list of strings and list of dicts
                    source_tables = lineage.get('source_tables', [])
                    for table in source_tables:
                        if isinstance(table, dict):
                            table_name = table.get('full_name') or table.get('table') or str(table)
                        else:
                            table_name = str(table)
                        if table_name and table_name not in result['all_source_tables']:
                            result['all_source_tables'].append(table_name)
                    
                    target_table = lineage.get('target_table')
                    if target_table:
                        if isinstance(target_table, dict):
                            target_name = target_table.get('full_name') or target_table.get('table') or str(target_table)
                        else:
                            target_name = str(target_table)
                        if target_name and target_name not in result['all_target_tables']:
                            result['all_target_tables'].append(target_name)
                    
                    result['column_lineage'].extend(lineage.get('column_lineage', []))
            
            # Extract dynamic SQL
            dynamic_sql = self._extract_dynamic_sql(procedure_code, language)
            for dyn_sql in dynamic_sql:
                lineage = self._parse_dynamic_sql(dyn_sql, language)
                if lineage:
                    result['dynamic_sql_lineage'].append(lineage)
                    result['all_source_tables'].extend(lineage.get('source_tables', []))
                    if lineage.get('target_table'):
                        result['all_target_tables'].append(lineage.get('target_table'))
                    result['column_lineage'].extend(lineage.get('column_lineage', []))
            
            # Extract procedure dependencies (calls to other procedures)
            result['procedure_dependencies'] = self._extract_procedure_dependencies(
                procedure_code, language
            )
            
            # Deduplicate tables
            result['all_source_tables'] = list(set(result['all_source_tables']))
            result['all_target_tables'] = list(set(result['all_target_tables']))
            
            # Calculate confidence
            if result['static_sql_lineage'] or result['dynamic_sql_lineage']:
                result['confidence_score'] = 0.8
            if result['dynamic_sql_lineage']:
                # Lower confidence for dynamic SQL (harder to parse)
                result['confidence_score'] = 0.7
            
            logger.info(
                f'Extracted lineage from procedure {procedure_name}: '
                f'{len(result["all_source_tables"])} sources, '
                f'{len(result["all_target_tables"])} targets'
            )
            
        except Exception as e:
            logger.error(f'Error extracting procedure lineage: {e}', exc_info=True)
            result['error'] = str(e)
            result['confidence_score'] = 0.3
        
        return result
    
    def _extract_static_sql(self, procedure_code: str, language: str) -> List[str]:
        """Extract static SQL statements from procedure"""
        sql_statements = []
        
        try:
            # Remove comments
            code = self._remove_comments(procedure_code, language)
            
            # Extract SQL statements based on language
            if language.lower() in ['plsql', 'oracle']:
                # PL/SQL: Look for SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE
                # Use more flexible patterns that handle multi-line SQL
                patterns = [
                    r'(SELECT\s+[^;]+?;)',
                    r'(INSERT\s+(?:INTO\s+)?\w+\s+[^;]+?;)',
                    r'(UPDATE\s+\w+\s+[^;]+?;)',
                    r'(DELETE\s+(?:FROM\s+)?\w+\s+[^;]+?;)',
                    r'(MERGE\s+[^;]+?;)',
                    r'(CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+[^;]+?;)',
                ]
            elif language.lower() in ['tsql', 'sqlserver']:
                # T-SQL: Similar patterns
                patterns = [
                    r'(SELECT\s+[^;]+?;)',
                    r'(INSERT\s+(?:INTO\s+)?\w+\s+[^;]+?;)',
                    r'(UPDATE\s+\w+\s+[^;]+?;)',
                    r'(DELETE\s+(?:FROM\s+)?\w+\s+[^;]+?;)',
                    r'(MERGE\s+[^;]+?;)',
                    r'(CREATE\s+(?:TABLE|VIEW)\s+[^;]+?;)',
                ]
            else:
                # Generic SQL
                patterns = [
                    r'(SELECT\s+[^;]+?;)',
                    r'(INSERT\s+[^;]+?;)',
                    r'(UPDATE\s+[^;]+?;)',
                    r'(DELETE\s+[^;]+?;)',
                ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, code, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    sql_stmt = match.group(1).strip()
                    # Clean up the SQL statement
                    sql_stmt = re.sub(r'\s+', ' ', sql_stmt)  # Normalize whitespace
                    if len(sql_stmt) > 10:  # Filter out very short matches
                        sql_statements.append(sql_stmt)
        
        except Exception as e:
            logger.debug(f'Error extracting static SQL: {e}')
        
        return sql_statements
    
    def _extract_dynamic_sql(self, procedure_code: str, language: str) -> List[Dict]:
        """Extract dynamic SQL from procedure"""
        dynamic_sql_list = []
        
        try:
            code = self._remove_comments(procedure_code, language)
            patterns = self.dynamic_sql_patterns.get(language.lower(), [])
            
            for pattern in patterns:
                matches = re.finditer(pattern, code, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    # Extract the SQL variable or string
                    sql_ref = match.group(1).strip()
                    
                    # Try to find the actual SQL string value
                    sql_value = self._resolve_sql_variable(code, sql_ref, language)
                    
                    if sql_value:
                        dynamic_sql_list.append({
                            'sql_string': sql_value,
                            'variable_name': sql_ref,
                            'pattern_matched': pattern,
                            'language': language
                        })
        
        except Exception as e:
            logger.debug(f'Error extracting dynamic SQL: {e}')
        
        return dynamic_sql_list
    
    def _resolve_sql_variable(self, code: str, var_name: str, language: str) -> Optional[str]:
        """Try to resolve SQL variable to actual SQL string"""
        try:
            # Remove quotes and whitespace
            var_name_clean = var_name.strip().strip("'\"")
            
            # Look for variable assignment
            if language.lower() in ['plsql', 'oracle']:
                # PL/SQL: v_sql := 'SELECT ...';
                pattern = rf'{re.escape(var_name_clean)}\s*:=\s*([\'"])(.*?)\1'
            elif language.lower() in ['tsql', 'sqlserver']:
                # T-SQL: @sql = 'SELECT ...'
                pattern = rf'@{re.escape(var_name_clean)}\s*=\s*([\'"])(.*?)\1'
            else:
                pattern = rf'{re.escape(var_name_clean)}\s*=\s*([\'"])(.*?)\1'
            
            match = re.search(pattern, code, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(2)
            
            # Try string concatenation patterns
            # v_sql := 'SELECT * FROM ' || table_name;
            concat_pattern = rf'{re.escape(var_name_clean)}\s*:=\s*([\'"])(.*?)\1\s*\|\|'
            match = re.search(concat_pattern, code, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(2) + '...'  # Partial SQL
        
        except Exception as e:
            logger.debug(f'Error resolving SQL variable: {e}')
        
        return None
    
    def _parse_sql_statement(self, sql_stmt: str, language: str) -> Optional[Dict]:
        """Parse a SQL statement using SQLGlot"""
        if not SQLGLOT_AVAILABLE:
            # Fallback to regex extraction
            return self._fallback_sql_extraction(sql_stmt)
        
        try:
            # Map language to SQLGlot dialect
            dialect_map = {
                'plsql': 'oracle',
                'tsql': 'tsql',
                'sqlserver': 'tsql',
                'postgres': 'postgres',
                'mysql': 'mysql'
            }
            dialect = dialect_map.get(language.lower(), 'mysql')
            
            # Use existing SQL lineage extractor
            from utils.sql_lineage_extractor import SQLLineageExtractor
            extractor = SQLLineageExtractor()
            result = extractor.extract_lineage(sql_stmt, dialect)
            
            # If extraction failed, fallback to regex
            if not result or not result.get('source_tables'):
                return self._fallback_sql_extraction(sql_stmt)
            
            return result
        
        except Exception as e:
            logger.debug(f'Error parsing SQL statement: {e}')
            # Fallback to regex extraction
            return self._fallback_sql_extraction(sql_stmt)
    
    def _parse_dynamic_sql(self, dynamic_sql_info: Dict, language: str) -> Optional[Dict]:
        """Parse extracted dynamic SQL"""
        sql_string = dynamic_sql_info.get('sql_string')
        if not sql_string:
            return None
        
        # If SQL is partial (ends with ...), try to extract what we can
        if sql_string.endswith('...'):
            sql_string = sql_string[:-3]
            # Try to extract table names from partial SQL
            return self._extract_partial_sql_lineage(sql_string)
        
        result = self._parse_sql_statement(sql_string, language)
        if not result:
            # Fallback to regex extraction
            return self._fallback_sql_extraction(sql_string)
        return result
    
    def _extract_partial_sql_lineage(self, partial_sql: str) -> Dict:
        """Extract lineage from partial/incomplete SQL"""
        result = self._fallback_sql_extraction(partial_sql)
        result['confidence_score'] = 0.5  # Lower confidence for partial SQL
        result['extraction_method'] = 'partial_dynamic_sql'
        return result
    
    def _fallback_sql_extraction(self, sql_stmt: str) -> Dict:
        """Fallback SQL extraction using regex when SQLGlot is not available"""
        result = {
            'source_tables': [],
            'target_table': None,
            'column_lineage': [],
            'query_type': None,
            'confidence_score': 0.5
        }
        
        try:
            # Extract FROM clause tables
            from_matches = re.findall(r'FROM\s+(\w+)', sql_stmt, re.IGNORECASE)
            result['source_tables'] = list(set(from_matches))
            
            # Extract JOIN tables
            join_matches = re.findall(r'JOIN\s+(\w+)', sql_stmt, re.IGNORECASE)
            result['source_tables'].extend(join_matches)
            result['source_tables'] = list(set(result['source_tables']))
            
            # Extract target table
            insert_match = re.search(r'INSERT\s+INTO\s+(\w+)', sql_stmt, re.IGNORECASE)
            if insert_match:
                result['target_table'] = insert_match.group(1)
                result['query_type'] = 'INSERT'
            
            update_match = re.search(r'UPDATE\s+(\w+)', sql_stmt, re.IGNORECASE)
            if update_match:
                result['target_table'] = update_match.group(1)
                result['query_type'] = 'UPDATE'
            
            delete_match = re.search(r'DELETE\s+FROM\s+(\w+)', sql_stmt, re.IGNORECASE)
            if delete_match:
                result['target_table'] = delete_match.group(1)
                result['query_type'] = 'DELETE'
        
        except Exception as e:
            logger.debug(f'Error in fallback SQL extraction: {e}')
        
        return result
    
    def _extract_procedure_dependencies(self, procedure_code: str, language: str) -> List[str]:
        """Extract calls to other procedures/functions"""
        dependencies = []
        
        try:
            code = self._remove_comments(procedure_code, language)
            
            if language.lower() in ['plsql', 'oracle']:
                # PL/SQL: procedure_name(...) or schema.procedure_name(...)
                pattern = r'(\w+(?:\.\w+)?)\s*\([^)]*\)'
            elif language.lower() in ['tsql', 'sqlserver']:
                # T-SQL: EXEC procedure_name or EXECUTE procedure_name
                pattern = r'EXEC(?:UTE)?\s+(\w+(?:\.\w+)?)'
            else:
                pattern = r'(\w+)\s*\([^)]*\)'
            
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                proc_name = match.group(1)
                # Filter out common SQL functions
                if proc_name.upper() not in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE']:
                    dependencies.append(proc_name)
        
        except Exception as e:
            logger.debug(f'Error extracting procedure dependencies: {e}')
        
        return list(set(dependencies))
    
    def _remove_comments(self, code: str, language: str) -> str:
        """Remove comments from procedure code"""
        try:
            # Remove single-line comments
            if language.lower() in ['plsql', 'oracle', 'tsql', 'sqlserver']:
                # -- comments
                code = re.sub(r'--.*$', '', code, flags=re.MULTILINE)
                # /* */ comments
                code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
            else:
                # Generic: remove #, --, /* */
                code = re.sub(r'#.*$', '', code, flags=re.MULTILINE)
                code = re.sub(r'--.*$', '', code, flags=re.MULTILINE)
                code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        except:
            pass
        
        return code


