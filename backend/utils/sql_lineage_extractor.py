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
    
    def __init__(self):
        if not SQLGLOT_AVAILABLE:
            logger.warning('FN:SQLLineageExtractor.__init__ message:SQLGlot not available, lineage extraction will be limited')
    
    def extract_lineage(self, sql_query: str, dialect: str = 'mysql') -> Dict:
        if not SQLGLOT_AVAILABLE:
            return self._fallback_extraction(sql_query)
        
        try:

            parsed = parse_one(sql_query, dialect=dialect)
            
            if not parsed:
                return self._fallback_extraction(sql_query)
            
            result = {
                'source_tables': [],
                'target_table': None,
                'column_lineage': [],
                'query_type': None,
                'confidence_score': 0.8,
                'extraction_method': 'sql_parsing'
            }
            

            if isinstance(parsed, exp.Create):
                result['query_type'] = 'CREATE'
                result['target_table'] = self._extract_table_name(parsed.this)
                result['confidence_score'] = 0.9
            elif isinstance(parsed, exp.Insert):
                result['query_type'] = 'INSERT'
                result['target_table'] = self._extract_table_name(parsed.this)
                result['confidence_score'] = 0.9
            elif isinstance(parsed, exp.Select):
                result['query_type'] = 'SELECT'
                result['confidence_score'] = 0.7
            elif isinstance(parsed, exp.CreateView):
                result['query_type'] = 'CREATE_VIEW'
                result['target_table'] = self._extract_table_name(parsed.this)
                result['confidence_score'] = 0.9
            

            source_tables = self._extract_source_tables(parsed)
            result['source_tables'] = list(set(source_tables))
            

            if result['target_table']:
                column_lineage = self._extract_column_lineage(parsed, result['target_table'])
                result['column_lineage'] = column_lineage
            
            logger.info('FN:extract_lineage query_type:{} source_tables_count:{} target_table:{} confidence:{}'.format(
                result['query_type'], len(result['source_tables']), result['target_table'], result['confidence_score']
            ))
            
            return result
            
        except Exception as e:
            logger.error('FN:extract_lineage error:{}'.format(str(e)))
            return self._fallback_extraction(sql_query)
    
    def _extract_table_name(self, expression) -> Optional[str]:
        try:
            if isinstance(expression, exp.Table):
                return expression.name
            elif isinstance(expression, exp.Identifier):
                return expression.name
            elif hasattr(expression, 'this'):
                return self._extract_table_name(expression.this)
        except Exception:
            pass
        return None
    
    def _extract_source_tables(self, parsed) -> List[str]:
        tables = []
        
        try:

            for from_expr in parsed.find_all(exp.From):
                table = self._extract_table_name(from_expr.this)
                if table:
                    tables.append(table)
            

            for join_expr in parsed.find_all(exp.Join):
                table = self._extract_table_name(join_expr.this)
                if table:
                    tables.append(table)
            

            for subquery in parsed.find_all(exp.Subquery):
                sub_tables = self._extract_source_tables(subquery)
                tables.extend(sub_tables)
                
        except Exception as e:
            logger.debug('FN:_extract_source_tables error:{}'.format(str(e)))
        
        return tables
    
    def _extract_column_lineage(self, parsed, target_table: str) -> List[Dict]:
        column_lineage = []
        
        try:

            if isinstance(parsed, exp.Insert):
                select_expr = parsed.find(exp.Select)
                if select_expr:

                    target_columns = []
                    if parsed.expression:
                        for col in parsed.expression.expressions:
                            if isinstance(col, exp.Column):
                                target_columns.append(col.name)
                    

                    source_columns = []
                    for col in select_expr.expressions:
                        if isinstance(col, exp.Column):
                            source_columns.append(col.name)
                        elif isinstance(col, exp.Alias):
                            source_columns.append(col.alias)
                    

                    if len(target_columns) == len(source_columns):
                        for i, target_col in enumerate(target_columns):
                            column_lineage.append({
                                'source_column': source_columns[i],
                                'target_column': target_col,
                                'transformation': 'pass_through',
                                'transformation_type': 'pass_through'
                            })
            

            elif isinstance(parsed, (exp.Create, exp.CreateView)):
                select_expr = parsed.find(exp.Select)
                if select_expr:

                    for i, col_expr in enumerate(select_expr.expressions):
                        target_col = None
                        source_col = None
                        transformation = 'pass_through'
                        
                        if isinstance(col_expr, exp.Alias):
                            target_col = col_expr.alias

                            if isinstance(col_expr.this, exp.Column):
                                source_col = col_expr.this.name
                            elif isinstance(col_expr.this, exp.Agg):
                                transformation = 'aggregate'
                                source_col = str(col_expr.this)
                        elif isinstance(col_expr, exp.Column):
                            target_col = col_expr.name
                            source_col = col_expr.name
                        
                        if target_col and source_col:
                            column_lineage.append({
                                'source_column': source_col,
                                'target_column': target_col,
                                'transformation': transformation,
                                'transformation_type': transformation
                            })
        
        except Exception as e:
            logger.debug('FN:_extract_column_lineage error:{}'.format(str(e)))
        
        return column_lineage
    
    def _fallback_extraction(self, sql_query: str) -> Dict:
        result = {
            'source_tables': [],
            'target_table': None,
            'column_lineage': [],
            'query_type': 'UNKNOWN',
            'confidence_score': 0.3,
            'extraction_method': 'regex_fallback'
        }
        
        try:
            sql_upper = sql_query.upper()
            

            insert_match = re.search(r'INSERT\s+INTO\s+(\w+)', sql_upper, re.IGNORECASE)
            if insert_match:
                result['target_table'] = insert_match.group(1)
                result['query_type'] = 'INSERT'
                result['confidence_score'] = 0.5
            

            create_match = re.search(r'CREATE\s+TABLE\s+(\w+)', sql_upper, re.IGNORECASE)
            if create_match:
                result['target_table'] = create_match.group(1)
                result['query_type'] = 'CREATE'
                result['confidence_score'] = 0.5
            

            from_matches = re.findall(r'FROM\s+(\w+)', sql_upper, re.IGNORECASE)
            result['source_tables'] = list(set(from_matches))
            

            join_matches = re.findall(r'JOIN\s+(\w+)', sql_upper, re.IGNORECASE)
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


