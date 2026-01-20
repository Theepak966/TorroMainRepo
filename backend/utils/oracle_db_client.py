import logging
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

try:
    import cx_Oracle
    ORACLE_AVAILABLE = True
except ImportError:
    try:
        import oracledb as cx_Oracle
        ORACLE_AVAILABLE = True
    except ImportError:
        ORACLE_AVAILABLE = False
        logger.warning("Oracle driver not available. Install cx_Oracle or oracledb")


class OracleDBClient:
    def __init__(self, config: Dict):
        if not ORACLE_AVAILABLE:
            raise ImportError("Oracle driver (cx_Oracle or oracledb) not installed. Install with: pip install cx_Oracle or pip install oracledb")
        
        self.config = config
        self.schema_filter = config.get('schema_filter', '').strip()
        
        # Check if JDBC URL is provided
        jdbc_url = config.get('jdbc_url', '').strip()
        
        if jdbc_url:
            # Parse JDBC URL
            self.username = config.get('username')
            self.password = config.get('password')
            
            if not all([jdbc_url, self.username, self.password]):
                raise ValueError("Missing required JDBC connection parameters: jdbc_url, username, password")
            
            # Parse JDBC URL to extract connection details
            connection_string = self._parse_jdbc_url(jdbc_url)
        else:
            # Standard connection parameters
            self.host = config.get('host')
            self.port = config.get('port', '1521')
            self.service_name = config.get('service_name')
            self.username = config.get('username')
            self.password = config.get('password')
            
            if not all([self.host, self.port, self.service_name, self.username, self.password]):
                raise ValueError("Missing required Oracle connection parameters: host, port, service_name, username, password")
            
            # Build connection string from individual parameters
            encoded_password = quote_plus(self.password)
            # Try oracledb first (newer), fallback to cx_oracle
            try:
                import oracledb
                driver = "oracle+oracledb"
            except ImportError:
                driver = "oracle+cx_oracle"
            # Oracle connection string format: oracle+driver://user:pass@host:port/service_name
            connection_string = f"{driver}://{self.username}:{encoded_password}@{self.host}:{self.port}/{self.service_name}"
        
        self.engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            echo=False
        )
        self.connection = None
    
    def _parse_jdbc_url(self, jdbc_url: str) -> str:
        """
        Parse JDBC URL and convert to SQLAlchemy connection string format.
        Supports formats:
        - jdbc:oracle:thin:@//host:port/service_name (most common)
        - jdbc:oracle:thin:@host:port:service_name (SID format)
        - jdbc:oracle:thin:@host:port/service_name
        - jdbc:oracle:oci:@host:port/service_name (OCI driver)
        - jdbc:oracle:thin:@(DESCRIPTION=...) (TNS connect descriptor)
        """
        import re
        
        # Try oracledb first (newer), fallback to cx_oracle
        try:
            import oracledb
            driver = "oracle+oracledb"
        except ImportError:
            driver = "oracle+cx_oracle"
        
        # Remove jdbc: prefix if present
        url = jdbc_url.replace('jdbc:', '')
        
        # Pattern 1: //host:port/service_name format (most common)
        pattern1 = r'oracle:(?:thin|oci):@//([^:/]+):(\d+)/(.+)'
        match1 = re.match(pattern1, url)
        if match1:
            host, port, service_name = match1.groups()
            # Remove query parameters if present
            service_name = service_name.split('?')[0].split('#')[0]
            encoded_password = quote_plus(self.password)
            return f"{driver}://{self.username}:{encoded_password}@{host}:{port}/{service_name}"
        
        # Pattern 2: host:port:service_name format (SID)
        pattern2 = r'oracle:(?:thin|oci):@([^:/]+):(\d+):(.+)'
        match2 = re.match(pattern2, url)
        if match2:
            host, port, service_name = match2.groups()
            # Remove query parameters if present
            service_name = service_name.split('?')[0].split('#')[0]
            encoded_password = quote_plus(self.password)
            return f"{driver}://{self.username}:{encoded_password}@{host}:{port}/{service_name}"
        
        # Pattern 3: host:port/service_name format
        pattern3 = r'oracle:(?:thin|oci):@([^:/]+):(\d+)/(.+)'
        match3 = re.match(pattern3, url)
        if match3:
            host, port, service_name = match3.groups()
            # Remove query parameters if present
            service_name = service_name.split('?')[0].split('#')[0]
            encoded_password = quote_plus(self.password)
            return f"{driver}://{self.username}:{encoded_password}@{host}:{port}/{service_name}"
        
        # Pattern 4: TNS connect descriptor format
        # jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=port))(CONNECT_DATA=(SERVICE_NAME=service)))
        tns_pattern = r'oracle:(?:thin|oci):@\(DESCRIPTION=.*?HOST=([^\)]+).*?PORT=(\d+).*?(?:SERVICE_NAME|SID)=([^\)]+)'
        match4 = re.search(tns_pattern, url, re.IGNORECASE | re.DOTALL)
        if match4:
            host, port, service_name = match4.groups()
            host = host.strip()
            service_name = service_name.strip()
            encoded_password = quote_plus(self.password)
            return f"{driver}://{self.username}:{encoded_password}@{host}:{port}/{service_name}"
        
        # Pattern 5: Try to extract from any format with @ symbol
        # This is a fallback for edge cases
        fallback_pattern = r'@([^:]+):(\d+)[:/]([^?&#]+)'
        match5 = re.search(fallback_pattern, url)
        if match5:
            host, port, service_name = match5.groups()
            host = host.strip().lstrip('/')
            service_name = service_name.strip()
            encoded_password = quote_plus(self.password)
            return f"{driver}://{self.username}:{encoded_password}@{host}:{port}/{service_name}"
        
        raise ValueError(
            f"Invalid JDBC URL format: {jdbc_url}\n"
            f"Supported formats:\n"
            f"  - jdbc:oracle:thin:@//host:port/service_name\n"
            f"  - jdbc:oracle:thin:@host:port:service_name (SID)\n"
            f"  - jdbc:oracle:thin:@host:port/service_name\n"
            f"  - jdbc:oracle:oci:@host:port/service_name\n"
            f"  - jdbc:oracle:thin:@(DESCRIPTION=...) (TNS)"
        )
    
    def test_connection(self) -> Dict:
        """Test the Oracle database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 FROM DUAL"))
                result.fetchone()
            return {"success": True, "message": "Connection successful"}
        except Exception as e:
            logger.error(f'FN:test_connection error:{str(e)}')
            return {"success": False, "message": f"Connection failed: {str(e)}"}
    
    def list_schemas(self) -> List[str]:
        """List all schemas in the database"""
        try:
            with self.engine.connect() as conn:
                # Get list of schemas (excluding system schemas)
                query = text("""
                    SELECT DISTINCT username 
                    FROM all_users 
                    WHERE username NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'OUTLN', 'DBSNMP', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'ORDPLUGINS', 'SI_INFORMTN_SCHEMA', 'WMSYS', 'EXFSYS', 'DMSYS', 'TSMSYS')
                    ORDER BY username
                """)
                result = conn.execute(query)
                schemas = [row[0] for row in result]
                
                # Filter schemas if schema_filter is provided
                if self.schema_filter:
                    filter_list = [s.strip().upper() for s in self.schema_filter.split(',')]
                    schemas = [s for s in schemas if s.upper() in filter_list]
                
                return schemas
        except Exception as e:
            logger.error(f'FN:list_schemas error:{str(e)}')
            return []
    
    def list_tables(self, schema: str) -> List[Dict]:
        """List all tables in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT table_name, num_rows, last_analyzed
                    FROM all_tables
                    WHERE owner = :schema
                    ORDER BY table_name
                """)
                result = conn.execute(query, {"schema": schema})
                tables = []
                for row in result:
                    tables.append({
                        "table_name": row[0],
                        "row_count": row[1] if row[1] else 0,
                        "last_analyzed": row[2].isoformat() if row[2] else None
                    })
                return tables
        except Exception as e:
            logger.error(f'FN:list_tables schema:{schema} error:{str(e)}')
            return []
    
    def get_table_columns(self, schema: str, table_name: str) -> List[Dict]:
        """Get column information for a table"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        column_name,
                        data_type,
                        data_length,
                        data_precision,
                        data_scale,
                        nullable,
                        column_id
                    FROM all_tab_columns
                    WHERE owner = :schema AND table_name = :table_name
                    ORDER BY column_id
                """)
                result = conn.execute(query, {"schema": schema, "table_name": table_name})
                columns = []
                for row in result:
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "length": row[2],
                        "precision": row[3],
                        "scale": row[4],
                        "nullable": row[5] == 'Y',
                        "position": row[6]
                    })
                return columns
        except Exception as e:
            logger.error(f'FN:get_table_columns schema:{schema} table:{table_name} error:{str(e)}')
            return []
    
    def get_table_metadata(self, schema: str, table_name: str) -> Dict:
        """Get comprehensive metadata for a table"""
        try:
            with self.engine.connect() as conn:
                # Get table info with more metadata
                query = text("""
                    SELECT 
                        t.num_rows,
                        t.blocks,
                        t.avg_row_len,
                        t.last_analyzed,
                        t.tablespace_name,
                        t.initial_extent,
                        t.next_extent,
                        t.pct_free,
                        t.pct_used,
                        t.ini_trans,
                        t.max_trans,
                        o.created,
                        o.last_ddl_time,
                        o.status
                    FROM all_tables t
                    JOIN all_objects o ON t.owner = o.owner AND t.table_name = o.object_name AND o.object_type = 'TABLE'
                    WHERE t.owner = :schema AND t.table_name = :table_name
                """)
                result = conn.execute(query, {"schema": schema, "table_name": table_name})
                row = result.fetchone()
                
                if not row:
                    return {}
                
                metadata = {
                    "row_count": row[0] if row[0] else 0,
                    "blocks": row[1] if row[1] else 0,
                    "avg_row_length": row[2] if row[2] else 0,
                    "last_analyzed": row[3].isoformat() if row[3] else None,
                    "tablespace": row[4] if row[4] else None,
                    "initial_extent": row[5] if row[5] else None,
                    "next_extent": row[6] if row[6] else None,
                    "pct_free": row[7] if row[7] is not None else None,
                    "pct_used": row[8] if row[8] is not None else None,
                    "ini_trans": row[9] if row[9] else None,
                    "max_trans": row[10] if row[10] else None,
                    "created": row[11].isoformat() if row[11] else None,
                    "last_ddl_time": row[12].isoformat() if row[12] else None,
                    "status": row[13] if row[13] else None
                }
                
                # Get columns
                columns = self.get_table_columns(schema, table_name)
                metadata["columns"] = columns
                metadata["column_count"] = len(columns)
                
                return metadata
        except Exception as e:
            logger.error(f'FN:get_table_metadata schema:{schema} table:{table_name} error:{str(e)}')
            return {}
    
    def list_views(self, schema: str) -> List[Dict]:
        """List all views in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        v.view_name, 
                        v.text_length,
                        o.created,
                        o.last_ddl_time,
                        o.status,
                        v.read_only
                    FROM all_views v
                    JOIN all_objects o ON v.owner = o.owner AND v.view_name = o.object_name AND o.object_type = 'VIEW'
                    WHERE v.owner = :schema
                    ORDER BY v.view_name
                """)
                result = conn.execute(query, {"schema": schema})
                views = []
                for row in result:
                    views.append({
                        "view_name": row[0],
                        "text_length": row[1] if row[1] else 0,
                        "created": row[2].isoformat() if row[2] else None,
                        "last_ddl_time": row[3].isoformat() if row[3] else None,
                        "status": row[4] if row[4] else None,
                        "read_only": row[5] if row[5] else None
                    })
                return views
        except Exception as e:
            logger.error(f'FN:list_views schema:{schema} error:{str(e)}')
            return []
    
    def get_view_definition(self, schema: str, view_name: str) -> Optional[str]:
        """Get the SQL definition of a view"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT text
                    FROM all_views
                    WHERE owner = :schema AND view_name = :view_name
                    ORDER BY view_id
                """)
                result = conn.execute(query, {"schema": schema, "view_name": view_name})
                rows = result.fetchall()
                if rows:
                    # Views can have multiple rows if definition is long
                    definition = ''.join([row[0] for row in rows])
                    return definition
                return None
        except Exception as e:
            logger.error(f'FN:get_view_definition schema:{schema} view:{view_name} error:{str(e)}')
            return None
    
    def list_materialized_views(self, schema: str) -> List[Dict]:
        """List all materialized views in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT mview_name, num_rows, last_refresh_date
                    FROM all_mviews
                    WHERE owner = :schema
                    ORDER BY mview_name
                """)
                result = conn.execute(query, {"schema": schema})
                mviews = []
                for row in result:
                    mviews.append({
                        "mview_name": row[0],
                        "row_count": row[1] if row[1] else 0,
                        "last_refresh_date": row[2].isoformat() if row[2] else None
                    })
                return mviews
        except Exception as e:
            logger.error(f'FN:list_materialized_views schema:{schema} error:{str(e)}')
            return []
    
    def get_materialized_view_definition(self, schema: str, mview_name: str) -> Optional[str]:
        """Get the SQL definition of a materialized view"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT query
                    FROM all_mviews
                    WHERE owner = :schema AND mview_name = :mview_name
                """)
                result = conn.execute(query, {"schema": schema, "mview_name": mview_name})
                row = result.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f'FN:get_materialized_view_definition schema:{schema} mview:{mview_name} error:{str(e)}')
            return None
    
    def list_procedures(self, schema: str) -> List[Dict]:
        """List all procedures in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT object_name, procedure_name
                    FROM all_procedures
                    WHERE owner = :schema
                    ORDER BY object_name, procedure_name
                """)
                result = conn.execute(query, {"schema": schema})
                procedures = []
                for row in result:
                    procedures.append({
                        "object_name": row[0],  # Package name if part of package, or procedure name
                        "procedure_name": row[1]  # Procedure name within package, or NULL if standalone
                    })
                return procedures
        except Exception as e:
            logger.error(f'FN:list_procedures schema:{schema} error:{str(e)}')
            return []
    
    def list_functions(self, schema: str) -> List[Dict]:
        """List all functions in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT object_name
                    FROM all_objects
                    WHERE owner = :schema 
                    AND object_type = 'FUNCTION'
                    ORDER BY object_name
                """)
                result = conn.execute(query, {"schema": schema})
                functions = []
                for row in result:
                    functions.append({
                        "function_name": row[0]
                    })
                return functions
        except Exception as e:
            logger.error(f'FN:list_functions schema:{schema} error:{str(e)}')
            return []
    
    def list_packages(self, schema: str) -> List[Dict]:
        """List all packages in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT object_name
                    FROM all_objects
                    WHERE owner = :schema 
                    AND object_type IN ('PACKAGE', 'PACKAGE BODY')
                    ORDER BY object_name, object_type
                """)
                result = conn.execute(query, {"schema": schema})
                packages = []
                seen = set()
                for row in result:
                    pkg_name = row[0]
                    if pkg_name not in seen:
                        packages.append({
                            "package_name": pkg_name
                        })
                        seen.add(pkg_name)
                return packages
        except Exception as e:
            logger.error(f'FN:list_packages schema:{schema} error:{str(e)}')
            return []
    
    def get_procedure_source(self, schema: str, object_name: str, procedure_name: Optional[str] = None) -> Optional[str]:
        """Get the source code of a procedure or function"""
        try:
            with self.engine.connect() as conn:
                if procedure_name:
                    # Procedure within a package
                    query = text("""
                        SELECT text
                        FROM all_source
                        WHERE owner = :schema 
                        AND name = :object_name
                        AND type = 'PACKAGE BODY'
                        ORDER BY line
                    """)
                    result = conn.execute(query, {"schema": schema, "object_name": object_name})
                else:
                    # Standalone procedure or function
                    query = text("""
                        SELECT text
                        FROM all_source
                        WHERE owner = :schema 
                        AND name = :object_name
                        AND type IN ('PROCEDURE', 'FUNCTION')
                        ORDER BY line
                    """)
                    result = conn.execute(query, {"schema": schema, "object_name": object_name})
                
                rows = result.fetchall()
                if rows:
                    source = ''.join([row[0] for row in rows])
                    return source
                return None
        except Exception as e:
            logger.error(f'FN:get_procedure_source schema:{schema} object:{object_name} error:{str(e)}')
            return None
    
    def list_triggers(self, schema: str) -> List[Dict]:
        """List all triggers in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT trigger_name, table_name, trigger_type, triggering_event, status
                    FROM all_triggers
                    WHERE owner = :schema
                    ORDER BY trigger_name
                """)
                result = conn.execute(query, {"schema": schema})
                triggers = []
                for row in result:
                    triggers.append({
                        "trigger_name": row[0],
                        "table_name": row[1],
                        "trigger_type": row[2],
                        "triggering_event": row[3],
                        "status": row[4]
                    })
                return triggers
        except Exception as e:
            logger.error(f'FN:list_triggers schema:{schema} error:{str(e)}')
            return []
    
    def get_trigger_source(self, schema: str, trigger_name: str) -> Optional[str]:
        """Get the source code of a trigger"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT trigger_body
                    FROM all_triggers
                    WHERE owner = :schema AND trigger_name = :trigger_name
                """)
                result = conn.execute(query, {"schema": schema, "trigger_name": trigger_name})
                row = result.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f'FN:get_trigger_source schema:{schema} trigger:{trigger_name} error:{str(e)}')
            return None
    
    def get_function_source(self, schema: str, function_name: str) -> Optional[str]:
        """Get the source code of a function"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT text
                    FROM all_source
                    WHERE owner = :schema 
                    AND name = :function_name
                    AND type = 'FUNCTION'
                    ORDER BY line
                """)
                result = conn.execute(query, {"schema": schema, "function_name": function_name})
                source_lines = [row[0] for row in result]
                if source_lines:
                    return '\n'.join(source_lines)
                return None
        except Exception as e:
            logger.error(f'FN:get_function_source schema:{schema} function:{function_name} error:{str(e)}')
            return None
    
    def get_foreign_keys(self, schema: str) -> List[Dict]:
        """Get all foreign key relationships in a schema"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        a.table_name,
                        a.constraint_name,
                        a.column_name,
                        c_pk.table_name AS referenced_table,
                        b.column_name AS referenced_column
                    FROM all_cons_columns a
                    JOIN all_constraints c ON a.owner = c.owner 
                        AND a.constraint_name = c.constraint_name
                    JOIN all_constraints c_pk ON c.r_owner = c_pk.owner 
                        AND c.r_constraint_name = c_pk.constraint_name
                    JOIN all_cons_columns b ON c_pk.owner = b.owner 
                        AND c_pk.constraint_name = b.constraint_name 
                        AND b.position = a.position
                    WHERE c.constraint_type = 'R'
                        AND a.owner = :schema
                    ORDER BY a.table_name, a.constraint_name, a.position
                """)
                result = conn.execute(query, {"schema": schema})
                fks = []
                current_fk = None
                for row in result:
                    table_name, constraint_name, column_name, ref_table, ref_column = row
                    
                    if not current_fk or current_fk['constraint_name'] != constraint_name:
                        if current_fk:
                            fks.append(current_fk)
                        current_fk = {
                            "constraint_name": constraint_name,
                            "table_name": table_name,
                            "referenced_table": ref_table,
                            "columns": [],
                            "referenced_columns": []
                        }
                    
                    current_fk['columns'].append(column_name)
                    current_fk['referenced_columns'].append(ref_column)
                
                if current_fk:
                    fks.append(current_fk)
                
                return fks
        except Exception as e:
            logger.error(f'FN:get_foreign_keys schema:{schema} error:{str(e)}')
            return []
    
    def get_view_dependencies(self, schema: str, view_name: str) -> List[Dict]:
        """Get all tables/views that a view depends on"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT referenced_owner, referenced_name, referenced_type
                    FROM all_dependencies
                    WHERE owner = :schema 
                    AND name = :view_name
                    AND type = 'VIEW'
                    ORDER BY referenced_name
                """)
                result = conn.execute(query, {"schema": schema, "view_name": view_name})
                dependencies = []
                for row in result:
                    dependencies.append({
                        "referenced_owner": row[0],
                        "referenced_name": row[1],
                        "referenced_type": row[2]
                    })
                return dependencies
        except Exception as e:
            logger.error(f'FN:get_view_dependencies schema:{schema} view:{view_name} error:{str(e)}')
            return []
    
    def get_procedure_dependencies(self, schema: str, object_name: str, procedure_name: Optional[str] = None) -> List[Dict]:
        """Get all objects that a procedure/function depends on"""
        try:
            with self.engine.connect() as conn:
                # For procedures in packages, search by package name
                search_name = object_name
                search_type = 'PACKAGE BODY' if procedure_name else 'PROCEDURE'
                
                query = text("""
                    SELECT DISTINCT referenced_owner, referenced_name, referenced_type
                    FROM all_dependencies
                    WHERE owner = :schema 
                    AND name = :object_name
                    AND type = :object_type
                    ORDER BY referenced_name
                """)
                result = conn.execute(query, {
                    "schema": schema, 
                    "object_name": search_name,
                    "object_type": search_type
                })
                dependencies = []
                for row in result:
                    dependencies.append({
                        "referenced_owner": row[0],
                        "referenced_name": row[1],
                        "referenced_type": row[2]
                    })
                return dependencies
        except Exception as e:
            logger.error(f'FN:get_procedure_dependencies schema:{schema} object:{object_name} error:{str(e)}')
            return []
    
    def get_materialized_view_dependencies(self, schema: str, mview_name: str) -> List[Dict]:
        """Get all objects that a materialized view depends on"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT referenced_owner, referenced_name, referenced_type
                    FROM all_dependencies
                    WHERE owner = :schema 
                    AND name = :mview_name
                    AND type = 'MATERIALIZED VIEW'
                    ORDER BY referenced_name
                """)
                result = conn.execute(query, {"schema": schema, "mview_name": mview_name})
                dependencies = []
                for row in result:
                    dependencies.append({
                        "referenced_owner": row[0],
                        "referenced_name": row[1],
                        "referenced_type": row[2]
                    })
                return dependencies
        except Exception as e:
            logger.error(f'FN:get_materialized_view_dependencies schema:{schema} mview:{mview_name} error:{str(e)}')
            return []
    
    def close(self):
        """Close the connection"""
        if self.engine:
            self.engine.dispose()

