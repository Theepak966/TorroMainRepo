"""
Advanced Enterprise-Grade SQL Lineage Extractor
Handles complex SQL constructs, column-level lineage, transformations, and more.
Comparable to DataHub, Collibra, and other enterprise lineage systems.
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
    logging.warning('FN:sql_lineage_extractor sqlglot_not_available:{}'.format(True))

logger = logging.getLogger(__name__)


class SQLLineageExtractor:
    """Enterprise-grade SQL lineage extractor with advanced parsing capabilities"""
    
    # Aggregation functions
    AGGREGATION_FUNCTIONS = {
        'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'STDDEV', 'VARIANCE',
        'STDDEV_POP', 'STDDEV_SAMP', 'VAR_POP', 'VAR_SAMP',
        'COLLECT', 'COLLECT_LIST', 'COLLECT_SET', 'ARRAY_AGG',
        'STRING_AGG', 'GROUP_CONCAT', 'LISTAGG'
    }
    
    # Window functions
    WINDOW_FUNCTIONS = {
        'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'PERCENT_RANK',
        'CUME_DIST', 'NTILE', 'LAG', 'LEAD', 'FIRST_VALUE',
        'LAST_VALUE', 'NTH_VALUE'
    }
    
    # Transformation types
    TRANSFORMATION_TYPES = {
        'pass_through': 'Direct column mapping',
        'aggregate': 'Aggregation function',
        'window': 'Window function',
        'case': 'CASE/WHEN conditional',
        'calculation': 'Mathematical calculation',
        'string': 'String transformation',
        'date': 'Date/time transformation',
        'cast': 'Type casting',
        'coalesce': 'COALESCE/NVL function',
        'join': 'JOIN operation',
        'filter': 'WHERE/HAVING filter',
        'union': 'UNION operation',
        'distinct': 'DISTINCT operation',
        'subquery': 'Subquery reference',
        'pivot': 'PIVOT operation',
        'unpivot': 'UNPIVOT operation',
        'explode': 'Array/JSON explode operation',
        'flatten': 'Nested structure flattening',
        'json_path': 'JSON path expression',
        'regex': 'Regular expression transformation',
        'conditional_aggregate': 'Conditional aggregation',
        'lateral': 'LATERAL join/table function',
        'unnest': 'UNNEST array operation'
    }
    
    def __init__(self):
        if not SQLGLOT_AVAILABLE:
            logger.warning('FN:SQLLineageExtractor.__init__ message:SQLGlot not available, lineage extraction will be limited')
    
    def extract_lineage(self, sql_query: str, dialect: str = 'mysql') -> Dict:
        """
        Extract comprehensive lineage from SQL query.
        Returns table-level and column-level lineage with transformation details.
        """
        if not SQLGLOT_AVAILABLE:
            return self._fallback_extraction(sql_query)
        
        try:
            # Try parsing with multiple dialects if initial parse fails
            parsed = None
            dialects_to_try = [dialect, 'mysql', 'postgres', 'oracle', 'sqlite', 'spark', 'hive']
            
            for d in dialects_to_try:
                try:
                    parsed = parse_one(sql_query, dialect=d)
                    if parsed:
                        dialect = d
                        break
                except:
                    continue
            
            if not parsed:
                return self._fallback_extraction(sql_query)
            
            result = {
                'source_tables': [],
                'target_table': None,
                'target_schema': None,
                'target_database': None,
                'column_lineage': [],
                'query_type': None,
                'transformation_details': {},
                'confidence_score': 0.8,
                'extraction_method': 'advanced_sql_parsing',
                'dialect_used': dialect,
                'complexity_score': 0.0
            }
            
            # Determine query type and extract target
            query_type, target_info = self._extract_query_type_and_target(parsed)
            result['query_type'] = query_type
            result['target_table'] = target_info.get('table')
            result['target_schema'] = target_info.get('schema')
            result['target_database'] = target_info.get('database')
            
            # Calculate confidence based on query type
            confidence_map = {
                'CREATE_VIEW': 0.95,
                'CREATE_TABLE': 0.95,
                'INSERT': 0.9,
                'MERGE': 0.9,
                'UPDATE': 0.85,
                'DELETE': 0.8,
                'SELECT': 0.7,
                'CTAS': 0.9
            }
            result['confidence_score'] = confidence_map.get(query_type, 0.8)
            
            # Extract source tables with full qualification
            source_tables_info = self._extract_source_tables_advanced(parsed)
            result['source_tables'] = [t['full_name'] for t in source_tables_info]
            
            # Calculate complexity score
            result['complexity_score'] = self._calculate_complexity(parsed)
            
            # Extract detailed column lineage
            if result['target_table']:
                column_lineage = self._extract_column_lineage_advanced(
                    parsed, 
                    result['target_table'],
                    source_tables_info
                )
                result['column_lineage'] = column_lineage
            
            # Extract transformation details
            result['transformation_details'] = self._extract_transformation_details(parsed)
            
            logger.info('FN:extract_lineage query_type:{} source_tables_count:{} target_table:{} confidence:{} complexity:{}'.format(
                result['query_type'], len(result['source_tables']), result['target_table'], 
                result['confidence_score'], result['complexity_score']
            ))
            
            return result
            
        except Exception as e:
            logger.error('FN:extract_lineage error:{}'.format(str(e)), exc_info=True)
            return self._fallback_extraction(sql_query)
    
    def _extract_query_type_and_target(self, parsed) -> Tuple[str, Dict]:
        """Extract query type and target table information"""
        target_info = {'table': None, 'schema': None, 'database': None}
        
        if isinstance(parsed, exp.CreateView):
            target_info = self._extract_qualified_name(parsed.this)
            return 'CREATE_VIEW', target_info
        elif isinstance(parsed, exp.Create):
            target_info = self._extract_qualified_name(parsed.this)
            return 'CREATE_TABLE', target_info
        elif isinstance(parsed, exp.Insert):
            target_info = self._extract_qualified_name(parsed.this)
            return 'INSERT', target_info
        elif isinstance(parsed, exp.Merge):
            target_info = self._extract_qualified_name(parsed.this)
            return 'MERGE', target_info
        elif isinstance(parsed, exp.Update):
            target_info = self._extract_qualified_name(parsed.this)
            return 'UPDATE', target_info
        elif isinstance(parsed, exp.Delete):
            target_info = self._extract_qualified_name(parsed.this)
            return 'DELETE', target_info
        elif isinstance(parsed, exp.Select):
            # Check if it's a CTAS (CREATE TABLE AS SELECT)
            if hasattr(parsed, 'parent') and isinstance(parsed.parent, exp.Create):
                target_info = self._extract_qualified_name(parsed.parent.this)
                return 'CTAS', target_info
            return 'SELECT', target_info
        elif isinstance(parsed, exp.Union):
            return 'UNION', target_info
        
        return 'UNKNOWN', target_info
    
    def _extract_qualified_name(self, expression) -> Dict:
        """Extract fully qualified name (database.schema.table)"""
        result = {'table': None, 'schema': None, 'database': None, 'full_name': None}
        
        try:
            if isinstance(expression, exp.Table):
                result['table'] = expression.name
                if hasattr(expression, 'db') and expression.db:
                    result['database'] = expression.db
                if hasattr(expression, 'catalog') and expression.catalog:
                    result['schema'] = expression.catalog
                elif hasattr(expression, 'db') and expression.db:
                    # In some dialects, db might be schema
                    result['schema'] = expression.db
            elif isinstance(expression, exp.Identifier):
                result['table'] = expression.name
            elif hasattr(expression, 'this'):
                return self._extract_qualified_name(expression.this)
            
            # Build full name
            parts = []
            if result['database']:
                parts.append(result['database'])
            if result['schema']:
                parts.append(result['schema'])
            if result['table']:
                parts.append(result['table'])
            
            if parts:
                result['full_name'] = '.'.join(parts)
            elif result['table']:
                result['full_name'] = result['table']
            
        except Exception as e:
            logger.debug(f'Error extracting qualified name: {e}')
        
        return result
    
    def _extract_source_tables_advanced(self, parsed) -> List[Dict]:
        """Extract source tables with full qualification and aliases"""
        tables_info = []
        seen = set()
        
        try:
            # Extract from FROM clauses
            for from_expr in parsed.find_all(exp.From):
                table_info = self._extract_table_info(from_expr.this, from_expr)
                if table_info and table_info['full_name'] not in seen:
                    tables_info.append(table_info)
                    seen.add(table_info['full_name'])
            
            # Extract from JOIN clauses
            for join_expr in parsed.find_all(exp.Join):
                table_info = self._extract_table_info(join_expr.this, join_expr)
                if table_info and table_info['full_name'] not in seen:
                    tables_info.append(table_info)
                    seen.add(table_info['full_name'])
            
            # Extract from CTEs (CTEs themselves aren't sources, but their bodies contain sources)
            for cte in parsed.find_all(exp.CTE):
                if hasattr(cte, 'this'):
                    cte_tables = self._extract_source_tables_advanced(cte.this)
                    for table_info in cte_tables:
                        if table_info['full_name'] not in seen:
                            tables_info.append(table_info)
                            seen.add(table_info['full_name'])
            
            # Extract from subqueries
            for subquery in parsed.find_all(exp.Subquery):
                sub_tables = self._extract_source_tables_advanced(subquery)
                for table_info in sub_tables:
                    if table_info['full_name'] not in seen:
                        tables_info.append(table_info)
                        seen.add(table_info['full_name'])
            
            # Extract from UNION/UNION ALL/INTERSECT/EXCEPT
            for union in parsed.find_all((exp.Union, exp.Intersect, exp.Except)):
                if hasattr(union, 'expressions'):
                    for expr in union.expressions:
                        union_tables = self._extract_source_tables_advanced(expr)
                        for table_info in union_tables:
                            if table_info['full_name'] not in seen:
                                tables_info.append(table_info)
                                seen.add(table_info['full_name'])
            
            # Extract from INSERT ... SELECT
            if isinstance(parsed, exp.Insert):
                select_expr = parsed.find(exp.Select)
                if select_expr:
                    insert_tables = self._extract_source_tables_advanced(select_expr)
                    for table_info in insert_tables:
                        if table_info['full_name'] not in seen:
                            tables_info.append(table_info)
                            seen.add(table_info['full_name'])
            
            # Extract from CREATE VIEW ... AS SELECT
            if isinstance(parsed, exp.CreateView):
                select_expr = parsed.find(exp.Select)
                if select_expr:
                    view_tables = self._extract_source_tables_advanced(select_expr)
                    for table_info in view_tables:
                        if table_info['full_name'] not in seen:
                            tables_info.append(table_info)
                            seen.add(table_info['full_name'])
            
            # Extract from MERGE statements
            if isinstance(parsed, exp.Merge):
                # Source table in USING clause
                if hasattr(parsed, 'using'):
                    using_info = self._extract_table_info(parsed.using, None)
                    if using_info and using_info['full_name'] not in seen:
                        tables_info.append(using_info)
                        seen.add(using_info['full_name'])
            
        except Exception as e:
            logger.debug('FN:_extract_source_tables_advanced error:{}'.format(str(e)))
        
        return tables_info
    
    def _extract_table_info(self, expression, parent_expr) -> Optional[Dict]:
        """Extract table information including alias"""
        try:
            table_info = self._extract_qualified_name(expression)
            
            # Extract alias
            alias = None
            if parent_expr and hasattr(parent_expr, 'alias'):
                alias = parent_expr.alias
            elif hasattr(expression, 'alias'):
                alias = expression.alias
            
            if alias:
                table_info['alias'] = alias
            
            return table_info
        except:
            return None
    
    def _extract_column_lineage_advanced(
        self, 
        parsed, 
        target_table: str,
        source_tables_info: List[Dict]
    ) -> List[Dict]:
        """Extract detailed column-level lineage with transformations"""
        column_lineage = []
        
        try:
            # Build alias map for source tables
            alias_map = {}
            for table_info in source_tables_info:
                if 'alias' in table_info and table_info['alias']:
                    alias_map[table_info['alias']] = table_info['full_name']
                alias_map[table_info['full_name']] = table_info['full_name']
            
            # Handle different query types
            if isinstance(parsed, exp.Insert):
                column_lineage.extend(
                    self._extract_insert_column_lineage(parsed, alias_map)
                )
            elif isinstance(parsed, (exp.Create, exp.CreateView)):
                column_lineage.extend(
                    self._extract_create_column_lineage(parsed, alias_map)
                )
            elif isinstance(parsed, exp.Merge):
                column_lineage.extend(
                    self._extract_merge_column_lineage(parsed, alias_map)
                )
            elif isinstance(parsed, exp.Update):
                column_lineage.extend(
                    self._extract_update_column_lineage(parsed, alias_map)
                )
            elif isinstance(parsed, exp.Select):
                column_lineage.extend(
                    self._extract_select_column_lineage(parsed, alias_map, target_table)
                )
        
        except Exception as e:
            logger.debug('FN:_extract_column_lineage_advanced error:{}'.format(str(e)))
        
        return column_lineage
    
    def _extract_insert_column_lineage(self, parsed, alias_map: Dict) -> List[Dict]:
        """Extract column lineage from INSERT statements"""
        column_lineage = []
        
        try:
            select_expr = parsed.find(exp.Select)
            if not select_expr:
                return column_lineage
            
            # Get target columns
            target_columns = []
            if parsed.expression:
                for col in parsed.expression.expressions:
                    if isinstance(col, exp.Column):
                        target_columns.append(col.name)
            
            # Get source columns from SELECT
            source_expressions = select_expr.expressions
            if len(target_columns) == len(source_expressions):
                for i, (target_col, source_expr) in enumerate(zip(target_columns, source_expressions)):
                    source_info = self._analyze_column_expression(source_expr, alias_map)
                    column_lineage.append({
                        'source_column': source_info.get('column'),
                        'source_table': source_info.get('table'),
                        'target_column': target_col,
                        'target_table': self._extract_qualified_name(parsed.this).get('full_name'),
                        'transformation_type': source_info.get('transformation_type', 'pass_through'),
                        'transformation_expression': source_info.get('expression'),
                        'confidence_score': source_info.get('confidence', 0.8)
                    })
        
        except Exception as e:
            logger.debug(f'Error extracting INSERT column lineage: {e}')
        
        return column_lineage
    
    def _extract_create_column_lineage(self, parsed, alias_map: Dict) -> List[Dict]:
        """Extract column lineage from CREATE TABLE/VIEW statements"""
        column_lineage = []
        
        try:
            select_expr = parsed.find(exp.Select)
            if not select_expr:
                return column_lineage
            
            target_table_info = self._extract_qualified_name(parsed.this)
            target_table = target_table_info.get('full_name')
            
            for col_expr in select_expr.expressions:
                target_col = None
                
                # Get target column name
                if isinstance(col_expr, exp.Alias):
                    target_col = col_expr.alias
                    source_expr = col_expr.this
                elif isinstance(col_expr, exp.Column):
                    target_col = col_expr.name
                    source_expr = col_expr
                else:
                    # Unnamed expression - use position
                    target_col = f"col_{len(column_lineage) + 1}"
                    source_expr = col_expr
                
                # Analyze source expression
                source_info = self._analyze_column_expression(source_expr, alias_map)
                
                column_lineage.append({
                    'source_column': source_info.get('column'),
                    'source_table': source_info.get('table'),
                    'target_column': target_col,
                    'target_table': target_table,
                    'transformation_type': source_info.get('transformation_type', 'pass_through'),
                    'transformation_expression': source_info.get('expression'),
                    'confidence_score': source_info.get('confidence', 0.8)
                })
        
        except Exception as e:
            logger.debug(f'Error extracting CREATE column lineage: {e}')
        
        return column_lineage
    
    def _extract_merge_column_lineage(self, parsedMerge, alias_map: Dict) -> List[Dict]:
        """Extract column lineage from MERGE statements"""
        column_lineage = []
        
        try:
            target_table_info = self._extract_qualified_name(parsed.this)
            target_table = target_table_info.get('full_name')
            
            # Extract WHEN MATCHED and WHEN NOT MATCHED clauses
            for when in parsed.find_all(exp.When):
                for update in when.find_all(exp.Update):
                    for set_expr in update.expressions:
                        if isinstance(set_expr, exp.EQ):
                            target_col = set_expr.left.name if isinstance(set_expr.left, exp.Column) else None
                            source_info = self._analyze_column_expression(set_expr.right, alias_map)
                            
                            if target_col:
                                column_lineage.append({
                                    'source_column': source_info.get('column'),
                                    'source_table': source_info.get('table'),
                                    'target_column': target_col,
                                    'target_table': target_table,
                                    'transformation_type': source_info.get('transformation_type', 'pass_through'),
                                    'transformation_expression': source_info.get('expression'),
                                    'confidence_score': source_info.get('confidence', 0.85)
                                })
        
        except Exception as e:
            logger.debug(f'Error extracting MERGE column lineage: {e}')
        
        return column_lineage
    
    def _extract_update_column_lineage(self, parsedUpdate, alias_map: Dict) -> List[Dict]:
        """Extract column lineage from UPDATE statements"""
        column_lineage = []
        
        try:
            target_table_info = self._extract_qualified_name(parsed.this)
            target_table = target_table_info.get('full_name')
            
            for set_expr in parsed.expressions:
                if isinstance(set_expr, exp.EQ):
                    target_col = set_expr.left.name if isinstance(set_expr.left, exp.Column) else None
                    source_info = self._analyze_column_expression(set_expr.right, alias_map)
                    
                    if target_col:
                        column_lineage.append({
                            'source_column': source_info.get('column'),
                            'source_table': source_info.get('table'),
                            'target_column': target_col,
                            'target_table': target_table,
                            'transformation_type': source_info.get('transformation_type', 'pass_through'),
                            'transformation_expression': source_info.get('expression'),
                            'confidence_score': source_info.get('confidence', 0.85)
                        })
        
        except Exception as e:
            logger.debug(f'Error extracting UPDATE column lineage: {e}')
        
        return column_lineage
    
    def _extract_select_column_lineage(self, parsedSelect, alias_map: Dict, target_table: str) -> List[Dict]:
        """Extract column lineage from SELECT statements"""
        column_lineage = []
        
        try:
            for col_expr in parsed.expressions:
                target_col = None
                
                if isinstance(col_expr, exp.Alias):
                    target_col = col_expr.alias
                    source_expr = col_expr.this
                elif isinstance(col_expr, exp.Column):
                    target_col = col_expr.name
                    source_expr = col_expr
                else:
                    target_col = f"expr_{len(column_lineage) + 1}"
                    source_expr = col_expr
                
                source_info = self._analyze_column_expression(source_expr, alias_map)
                
                column_lineage.append({
                    'source_column': source_info.get('column'),
                    'source_table': source_info.get('table'),
                    'target_column': target_col,
                    'target_table': target_table,
                    'transformation_type': source_info.get('transformation_type', 'pass_through'),
                    'transformation_expression': source_info.get('expression'),
                    'confidence_score': source_info.get('confidence', 0.7)
                })
        
        except Exception as e:
            logger.debug(f'Error extracting SELECT column lineage: {e}')
        
        return column_lineage
    
    def _analyze_column_expression(self, expression, alias_map: Dict) -> Dict:
        """Analyze a column expression to extract source column, table, and transformation"""
        result = {
            'column': None,
            'table': None,
            'transformation_type': 'pass_through',
            'expression': str(expression),
            'confidence': 0.8
        }
        
        try:
            # Direct column reference
            if isinstance(expression, exp.Column):
                result['column'] = expression.name
                if hasattr(expression, 'table') and expression.table:
                    table_name = expression.table
                    # Resolve alias
                    result['table'] = alias_map.get(table_name, table_name)
                result['transformation_type'] = 'pass_through'
                result['confidence'] = 0.95
            
            # Aggregation function
            elif isinstance(expression, exp.Agg):
                func_name = expression.this.upper() if hasattr(expression, 'this') else None
                if func_name in self.AGGREGATION_FUNCTIONS:
                    result['transformation_type'] = 'aggregate'
                    result['expression'] = str(expression)
                    # Try to find source column in arguments
                    if hasattr(expression, 'expressions') and expression.expressions:
                        arg = expression.expressions[0]
                        if isinstance(arg, exp.Column):
                            result['column'] = arg.name
                            if hasattr(arg, 'table') and arg.table:
                                result['table'] = alias_map.get(arg.table, arg.table)
                    result['confidence'] = 0.85
            
            # Window function
            elif isinstance(expression, exp.Window):
                result['transformation_type'] = 'window'
                result['expression'] = str(expression)
                # Extract column from window function
                if hasattr(expression, 'this'):
                    window_info = self._analyze_column_expression(expression.this, alias_map)
                    result['column'] = window_info.get('column')
                    result['table'] = window_info.get('table')
                result['confidence'] = 0.8
            
            # CASE statement
            elif isinstance(expression, exp.Case):
                result['transformation_type'] = 'case'
                result['expression'] = str(expression)
                # Extract columns from CASE conditions and values
                columns = []
                for when in expression.find_all(exp.When):
                    if hasattr(when, 'this'):
                        when_info = self._analyze_column_expression(when.this, alias_map)
                        if when_info.get('column'):
                            columns.append(when_info['column'])
                    if hasattr(when, 'then'):
                        then_info = self._analyze_column_expression(when.then, alias_map)
                        if then_info.get('column'):
                            columns.append(then_info['column'])
                if columns:
                    result['column'] = columns[0]  # Primary source column
                result['confidence'] = 0.75
            
            # Mathematical operations
            elif isinstance(expression, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)):
                result['transformation_type'] = 'calculation'
                result['expression'] = str(expression)
                # Extract columns from operands
                columns = []
                for operand in [expression.left, expression.right]:
                    if isinstance(operand, exp.Column):
                        columns.append(operand.name)
                        if hasattr(operand, 'table') and operand.table:
                            result['table'] = alias_map.get(operand.table, operand.table)
                if columns:
                    result['column'] = columns[0]
                result['confidence'] = 0.7
            
            # Function expressions - check multiple function types
            elif isinstance(expression, exp.Function):
                func_name = expression.this.upper() if hasattr(expression, 'this') else None
                if not func_name:
                    func_name = str(expression.this).upper() if hasattr(expression, 'this') else None
                
                # String functions
                string_funcs = {'CONCAT', 'SUBSTRING', 'UPPER', 'LOWER', 'TRIM', 'REPLACE', 'LENGTH'}
                if func_name in string_funcs:
                    result['transformation_type'] = 'string'
                    result['expression'] = str(expression)
                    if hasattr(expression, 'expressions') and expression.expressions:
                        arg = expression.expressions[0]
                        if isinstance(arg, exp.Column):
                            result['column'] = arg.name
                            if hasattr(arg, 'table') and arg.table:
                                result['table'] = alias_map.get(arg.table, arg.table)
                    result['confidence'] = 0.75
                
                # Regex functions
                elif func_name and any(regex_func in func_name for regex_func in ['REGEXP', 'REGEX']):
                    result['transformation_type'] = 'regex'
                    result['expression'] = str(expression)
                    if hasattr(expression, 'expressions') and expression.expressions:
                        arg = expression.expressions[0]
                        if isinstance(arg, exp.Column):
                            result['column'] = arg.name
                            if hasattr(arg, 'table') and arg.table:
                                result['table'] = alias_map.get(arg.table, arg.table)
                    result['confidence'] = 0.75
                
                # JSON path functions
                elif func_name and any(json_func in func_name for json_func in ['JSON_EXTRACT', 'JSON_VALUE', 'JSON_QUERY', 'JSON_PATH', 'GET_JSON_OBJECT']):
                    result['transformation_type'] = 'json_path'
                    result['expression'] = str(expression)
                    if hasattr(expression, 'expressions') and expression.expressions:
                        arg = expression.expressions[0]
                        if isinstance(arg, exp.Column):
                            result['column'] = arg.name
                            if hasattr(arg, 'table') and arg.table:
                                result['table'] = alias_map.get(arg.table, arg.table)
                    result['confidence'] = 0.75
                
                # Array/explode functions
                elif func_name in {'EXPLODE', 'UNNEST', 'FLATTEN', 'LATERAL_VIEW'}:
                    result['transformation_type'] = 'explode' if func_name in {'EXPLODE', 'UNNEST'} else 'flatten'
                    result['expression'] = str(expression)
                    if hasattr(expression, 'expressions') and expression.expressions:
                        arg = expression.expressions[0]
                        if isinstance(arg, exp.Column):
                            result['column'] = arg.name
                            if hasattr(arg, 'table') and arg.table:
                                result['table'] = alias_map.get(arg.table, arg.table)
                    result['confidence'] = 0.7
            
            # PIVOT operations
            elif isinstance(expression, exp.Pivot):
                result['transformation_type'] = 'pivot'
                result['expression'] = str(expression)
                # Extract source columns from pivot
                if hasattr(expression, 'expressions'):
                    for expr in expression.expressions:
                        if isinstance(expr, exp.Column):
                            result['column'] = expr.name
                            if hasattr(expr, 'table') and expr.table:
                                result['table'] = alias_map.get(expr.table, expr.table)
                            break
                result['confidence'] = 0.8
            
            # UNPIVOT operations
            elif isinstance(expression, exp.Unpivot):
                result['transformation_type'] = 'unpivot'
                result['expression'] = str(expression)
                result['confidence'] = 0.8
            
            # LATERAL joins
            elif isinstance(expression, exp.Lateral):
                result['transformation_type'] = 'lateral'
                result['expression'] = str(expression)
                if hasattr(expression, 'this'):
                    lateral_info = self._analyze_column_expression(expression.this, alias_map)
                    result['column'] = lateral_info.get('column')
                    result['table'] = lateral_info.get('table')
                result['confidence'] = 0.75
            
            # Alias (recurse into underlying expression)
            elif isinstance(expression, exp.Alias):
                return self._analyze_column_expression(expression.this, alias_map)
            
            # Subquery (lower confidence)
            elif isinstance(expression, exp.Subquery):
                result['transformation_type'] = 'subquery'
                result['expression'] = str(expression)
                result['confidence'] = 0.6
            
            # COALESCE/NVL
            elif isinstance(expression, exp.Coalesce):
                result['transformation_type'] = 'coalesce'
                result['expression'] = str(expression)
                if hasattr(expression, 'expressions') and expression.expressions:
                    arg = expression.expressions[0]
                    if isinstance(arg, exp.Column):
                        result['column'] = arg.name
                        if hasattr(arg, 'table') and arg.table:
                            result['table'] = alias_map.get(arg.table, arg.table)
                result['confidence'] = 0.8
        
        except Exception as e:
            logger.debug(f'Error analyzing column expression: {e}')
        
        return result
    
    def _extract_transformation_details(self, parsed) -> Dict:
        """Extract detailed transformation information"""
        details = {
            'has_joins': False,
            'has_aggregations': False,
            'has_window_functions': False,
            'has_subqueries': False,
            'has_ctes': False,
            'has_unions': False,
            'has_case_statements': False,
            'join_count': 0,
            'aggregation_count': 0,
            'complexity_factors': []
        }
        
        try:
            # Check for joins
            joins = list(parsed.find_all(exp.Join))
            if joins:
                details['has_joins'] = True
                details['join_count'] = len(joins)
                details['complexity_factors'].append(f'{len(joins)} join(s)')
            
            # Check for aggregations
            aggs = list(parsed.find_all(exp.Agg))
            if aggs:
                details['has_aggregations'] = True
                details['aggregation_count'] = len(aggs)
                details['complexity_factors'].append(f'{len(aggs)} aggregation(s)')
            
            # Check for window functions
            windows = list(parsed.find_all(exp.Window))
            if windows:
                details['has_window_functions'] = True
                details['complexity_factors'].append('window functions')
            
            # Check for subqueries
            subqueries = list(parsed.find_all(exp.Subquery))
            if subqueries:
                details['has_subqueries'] = True
                details['complexity_factors'].append(f'{len(subqueries)} subquery(ies)')
            
            # Check for CTEs
            ctes = list(parsed.find_all(exp.CTE))
            if ctes:
                details['has_ctes'] = True
                details['complexity_factors'].append(f'{len(ctes)} CTE(s)')
            
            # Check for UNIONs
            unions = list(parsed.find_all((exp.Union, exp.Intersect, exp.Except)))
            if unions:
                details['has_unions'] = True
                details['complexity_factors'].append('UNION/INTERSECT/EXCEPT')
            
            # Check for CASE statements
            cases = list(parsed.find_all(exp.Case))
            if cases:
                details['has_case_statements'] = True
                details['complexity_factors'].append(f'{len(cases)} CASE statement(s)')
        
        except Exception as e:
            logger.debug(f'Error extracting transformation details: {e}')
        
        return details
    
    def _calculate_complexity(self, parsed) -> float:
        """Calculate complexity score (0.0 to 1.0)"""
        complexity = 0.0
        
        try:
            # Base complexity
            complexity += 0.1
            
            # Add complexity for each construct
            complexity += min(0.1 * len(list(parsed.find_all(exp.Join))), 0.3)
            complexity += min(0.1 * len(list(parsed.find_all(exp.Subquery))), 0.3)
            complexity += min(0.1 * len(list(parsed.find_all(exp.CTE))), 0.2)
            complexity += min(0.05 * len(list(parsed.find_all(exp.Agg))), 0.2)
            complexity += min(0.1 * len(list(parsed.find_all(exp.Union))), 0.2)
            complexity += min(0.05 * len(list(parsed.find_all(exp.Case))), 0.1)
            complexity += min(0.1 * len(list(parsed.find_all(exp.Pivot))), 0.15)
            complexity += min(0.1 * len(list(parsed.find_all(exp.Unpivot))), 0.15)
            complexity += min(0.1 * len(list(parsed.find_all(exp.Lateral))), 0.15)
            
            # Cap at 1.0
            complexity = min(complexity, 1.0)
        
        except:
            pass
        
        return complexity
    
    def _fallback_extraction(self, sql_query: str) -> Dict:
        """Fallback regex-based extraction when SQLGlot fails"""
        result = {
            'source_tables': [],
            'target_table': None,
            'column_lineage': [],
            'query_type': 'UNKNOWN',
            'confidence_score': 0.3,
            'extraction_method': 'regex_fallback',
            'complexity_score': 0.0
        }
        
        try:
            sql_upper = sql_query.upper()
            
            # INSERT
            insert_match = re.search(r'INSERT\s+(?:INTO\s+)?(\w+(?:\.\w+)*)', sql_upper, re.IGNORECASE)
            if insert_match:
                result['target_table'] = insert_match.group(1)
                result['query_type'] = 'INSERT'
                result['confidence_score'] = 0.5
            
            # CREATE TABLE/VIEW
            create_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(\w+(?:\.\w+)*)', sql_upper, re.IGNORECASE)
            if create_match:
                result['target_table'] = create_match.group(1)
                result['query_type'] = 'CREATE'
                result['confidence_score'] = 0.5
            
            # FROM clauses
            from_matches = re.findall(r'FROM\s+(\w+(?:\.\w+)*)', sql_upper, re.IGNORECASE)
            result['source_tables'] = list(set(from_matches))
            
            # JOIN clauses
            join_matches = re.findall(r'JOIN\s+(\w+(?:\.\w+)*)', sql_upper, re.IGNORECASE)
            result['source_tables'].extend(join_matches)
            result['source_tables'] = list(set(result['source_tables']))
        
        except Exception as e:
            logger.error('FN:_fallback_extraction error:{}'.format(str(e)))
        
        return result


_lineage_extractor = None

def get_lineage_extractor() -> SQLLineageExtractor:
    global _lineage_extractor
    if _lineage_extractor is None:
        _lineage_extractor = SQLLineageExtractor()
    return _lineage_extractor


def extract_lineage_from_sql(sql_query: str, dialect: str = 'mysql') -> Dict:
    extractor = get_lineage_extractor()
    return extractor.extract_lineage(sql_query, dialect)
