import ibis

from app.model import OracleConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    ConstraintType,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata


class OracleMetadata(Metadata):
    """
    Oracle metadata extraction for WrenAI.
    
    VIEWS-ONLY ARCHITECTURE:
    This implementation discovers Oracle VIEWS exclusively, not tables.
    - Optimized for view-based reporting databases
    - Tables are internal implementation details and not exposed
    - Relationships defined via manual configuration (YAML)
    
    Key features:
    - Dynamic user extraction (not hardcoded 'SYSTEM')
    - Quoted identifier support for view names with spaces
    - Permission-safe version detection
    """
    
    def __init__(self, connection_info: OracleConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.oracle.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        # Get dynamic user from connection info (not hardcoded 'SYSTEM')
        user = self.connection_info.user.get_secret_value()
        
        sql = f"""
            SELECT
                t.owner AS TABLE_CATALOG,
                t.owner AS TABLE_SCHEMA,
                t.table_name AS TABLE_NAME,
                c.column_name AS COLUMN_NAME,
                c.data_type AS DATA_TYPE,
                c.nullable AS IS_NULLABLE,
                c.column_id AS ORDINAL_POSITION,
                tc.comments AS TABLE_COMMENT,
                cc.comments AS COLUMN_COMMENT
            FROM
                all_views v
            JOIN
                all_tab_columns c
                ON v.owner = c.owner
                AND v.view_name = c.table_name
            LEFT JOIN
                all_tab_comments tc
                ON tc.owner = v.owner
                AND tc.table_name = v.view_name
            LEFT JOIN
                all_col_comments cc
                ON cc.owner = c.owner
                AND cc.table_name = c.table_name
                AND cc.column_name = c.column_name
            WHERE
                v.owner = '{user}'
            ORDER BY
                v.view_name, c.column_id;
        """
        #  Provide the pre-build schema explicitly with uppercase column names
        #  To avoid potential ibis get schema error:
        #  Solve oracledb DatabaseError: ORA-00942: table or view not found
        schema = ibis.schema(
            {
                "TABLE_CATALOG": "string",
                "TABLE_SCHEMA": "string",
                "TABLE_NAME": "string",
                "COLUMN_NAME": "string",
                "DATA_TYPE": "string",
                "IS_NULLABLE": "string",
                "ORDINAL_POSITION": "int64",
                "TABLE_COMMENT": "string",
                "COLUMN_COMMENT": "string",
            }
        )
        response = (
            self.connection.sql(sql, schema=schema)
            .to_pandas()
            .to_dict(orient="records")
        )

        unique_tables = {}
        for row in response:
            # Use uppercase keys that match the provided schema.
            schema_table = self._format_compact_table_name(
                row["TABLE_SCHEMA"], row["TABLE_NAME"]
            )
            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description=row["TABLE_COMMENT"],
                    columns=[],
                    properties=TableProperties(
                        schema=row["TABLE_SCHEMA"],
                        catalog="",  # Oracle doesn't use catalogs.
                        table=row["TABLE_NAME"],
                    ),
                    primaryKey="",
                )

            unique_tables[schema_table].columns.append(
                Column(
                    name=row["COLUMN_NAME"],
                    type=self._transform_column_type(row["DATA_TYPE"]),
                    notNull=row["IS_NULLABLE"] == "N",
                    description=row["COLUMN_COMMENT"],
                    properties=None,
                )
            )

        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        """
        Auto-detect view-to-view relationships for views-only architecture.
        
        STRATEGY:
        Since views don't have FK constraints, we discover relationships by:
        1. Query all_dependencies to find base tables each view depends on
        2. Query all_constraints to get FK relationships from those base tables
        3. Map table FKs to view-to-view relationships when multiple views share tables
        
        This provides automatic relationship detection with zero configuration!
        
        Returns:
            List of view-to-view relationships based on underlying table FKs
        """
        user = self.connection_info.user.get_secret_value()
        constraints = []
        
        try:
            # Step 1: Get all views in this schema
            views_schema = ibis.schema({"VIEW_NAME": "string"})
            views_sql = f"""
                SELECT view_name
                FROM all_views
                WHERE owner = '{user}'
                ORDER BY view_name
            """
            views_df = self.connection.sql(views_sql, schema=views_schema).to_pandas()
            views = views_df['VIEW_NAME'].tolist()
            
            if not views:
                return []
            
            # Step 2: Map each view to its base tables using all_dependencies
            view_to_tables = {}
            table_to_owner = {}
            
            deps_schema = ibis.schema({
                "VIEW_NAME": "string",
                "BASE_TABLE": "string", 
                "BASE_OWNER": "string"
            })
            
            for view in views:
                deps_sql = f"""
                    SELECT 
                        name AS VIEW_NAME,
                        referenced_name AS BASE_TABLE,
                        referenced_owner AS BASE_OWNER
                    FROM all_dependencies
                    WHERE owner = '{user}'
                    AND name = '{view}'
                    AND type = 'VIEW'
                    AND referenced_type = 'TABLE'
                """
                try:
                    deps_df = self.connection.sql(deps_sql, schema=deps_schema).to_pandas()
                    if not deps_df.empty:
                        tables = [(row['BASE_TABLE'], row['BASE_OWNER']) 
                                 for _, row in deps_df.iterrows()]
                        view_to_tables[view] = tables
                        
                        # Track table ownership for FK queries
                        for table, owner in tables:
                            if table not in table_to_owner:
                                table_to_owner[table] = owner
                except Exception:
                    # View may not have dependencies or permission issues
                    continue
            
            if not table_to_owner:
                return []
            
            # Step 3: Get FK constraints from all base tables
            table_fks = {}
            fk_schema = ibis.schema({
                "TABLE_NAME": "string",
                "COLUMN_NAME": "string",
                "REF_TABLE": "string",
                "REF_COLUMN": "string"
            })
            
            for table, owner in table_to_owner.items():
                fk_sql = f"""
                    SELECT 
                        c.table_name AS TABLE_NAME,
                        cc.column_name AS COLUMN_NAME,
                        rc.table_name AS REF_TABLE,
                        rcc.column_name AS REF_COLUMN
                    FROM all_constraints c
                    JOIN all_cons_columns cc 
                        ON c.constraint_name = cc.constraint_name 
                        AND c.owner = cc.owner
                    JOIN all_constraints rc 
                        ON c.r_constraint_name = rc.constraint_name
                    JOIN all_cons_columns rcc 
                        ON rc.constraint_name = rcc.constraint_name 
                        AND rc.owner = rcc.owner
                    WHERE c.constraint_type = 'R'
                    AND c.owner = '{owner}'
                    AND c.table_name = '{table}'
                """
                try:
                    fk_df = self.connection.sql(fk_sql, schema=fk_schema).to_pandas()
                    if not fk_df.empty:
                        table_fks[table] = fk_df.to_dict('records')
                except Exception:
                    # Table might not be accessible or have no FKs
                    continue
            
            # Step 4: Map table FKs to view-to-view relationships
            # For each view, check if its base tables have FKs to tables used by other views
            for view1 in views:
                if view1 not in view_to_tables:
                    continue
                    
                view1_tables = {t[0] for t in view_to_tables[view1]}
                
                # Check FKs from this view's base tables
                for table in view1_tables:
                    if table not in table_fks:
                        continue
                    
                    for fk in table_fks[table]:
                        ref_table = fk['REF_TABLE']
                        
                        # Find other views that use the referenced table
                        for view2 in views:
                            if view2 == view1 or view2 not in view_to_tables:
                                continue
                            
                            view2_tables = {t[0] for t in view_to_tables[view2]}
                            if ref_table in view2_tables:
                                # Found a relationship! Create constraint
                                constraint_name = f"VIEW_FK_{view1}_{view2}_{fk['COLUMN_NAME']}".replace(" ", "_")
                                
                                constraints.append(
                                    Constraint(
                                        constraintName=constraint_name[:128],  # Oracle name limit
                                        constraintTable=self._format_compact_table_name(user, view1),
                                        constraintColumn=fk['COLUMN_NAME'],
                                        constraintedTable=self._format_compact_table_name(user, view2),
                                        constraintedColumn=fk['REF_COLUMN'],
                                        constraintType=ConstraintType.FOREIGN_KEY,
                                    )
                                )
            
            return constraints
            
        except Exception as e:
            # If auto-detection fails, return empty list rather than breaking WrenAI
            # This ensures the system remains functional even with permission issues
            print(f"Warning: Could not auto-detect view relationships: {e}")
            return []

    def get_version(self) -> str:
        """
        Get Oracle database version.
        
        Uses fallback approach to avoid permission issues with v$instance.
        Many Oracle users don't have SELECT privileges on v$instance system view.
        
        Returns:
            Oracle version string (defaults to "19.0.0.0.0" for Oracle ADB 19c)
        """
        try:
            # Try v$instance first (requires permissions)
            schema = ibis.schema({"VERSION": "string"})
            return (
                self.connection.sql("SELECT version FROM v$instance", schema=schema)
                .to_pandas()
                .iloc[0, 0]
            )
        except Exception:
            # Fallback: Return hardcoded version for Oracle ADB 19c
            # This ensures metadata discovery never fails on version check
            return "19.0.0.0.0"

    def _format_compact_table_name(self, schema: str, table: str):
        """
        Format Oracle view names, adding quotes for names with spaces.
        
        Oracle requires double quotes around object names containing spaces:
        - CORRECT: SCHEMA."RT Customer"
        - INCORRECT: SCHEMA.RT Customer (causes ORA-00923)
        
        This is critical for views-only architecture where views often have spaces.
        
        Args:
            schema: Oracle schema/owner name
            table: View name (method kept as 'table' for WrenAI compatibility)
        
        Returns:
            Formatted object reference for use in SQL
        """
        if ' ' in table:
            # Escape any internal quotes (although rare in Oracle view names)
            escaped_table = table.replace('"', '""')
            return f'{schema}."{escaped_table}"'
        
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, referenced_table_name, referenced_column_name
    ):
        return f"{table_name}_{column_name}_{referenced_table_name}_{referenced_column_name}"

    def _transform_column_type(self, data_type):
        switcher = {
            "CHAR": RustWrenEngineColumnType.CHAR,
            "NCHAR": RustWrenEngineColumnType.CHAR,
            "VARCHAR2": RustWrenEngineColumnType.VARCHAR,
            "NVARCHAR2": RustWrenEngineColumnType.VARCHAR,
            "CLOB": RustWrenEngineColumnType.TEXT,
            "NCLOB": RustWrenEngineColumnType.TEXT,
            "NUMBER": RustWrenEngineColumnType.DECIMAL,
            "FLOAT": RustWrenEngineColumnType.FLOAT8,
            "BINARY_FLOAT": RustWrenEngineColumnType.FLOAT8,
            "BINARY_DOUBLE": RustWrenEngineColumnType.DOUBLE,
            "DATE": RustWrenEngineColumnType.TIMESTAMP,  # Oracle DATE includes time.
            "TIMESTAMP": RustWrenEngineColumnType.TIMESTAMP,
            "TIMESTAMP WITH TIME ZONE": RustWrenEngineColumnType.TIMESTAMPTZ,
            "TIMESTAMP WITH LOCAL TIME ZONE": RustWrenEngineColumnType.TIMESTAMPTZ,
            "INTERVAL YEAR TO MONTH": RustWrenEngineColumnType.INTERVAL,
            "INTERVAL DAY TO SECOND": RustWrenEngineColumnType.INTERVAL,
            "BLOB": RustWrenEngineColumnType.BYTEA,
            "BFILE": RustWrenEngineColumnType.BYTEA,
            "RAW": RustWrenEngineColumnType.BYTEA,
            "LONG RAW": RustWrenEngineColumnType.BYTEA,
            "ROWID": RustWrenEngineColumnType.CHAR,
            "UROWID": RustWrenEngineColumnType.CHAR,
            "JSON": RustWrenEngineColumnType.JSON,
            "OSON": RustWrenEngineColumnType.JSON,
            "VARCHAR2 WITH JSON": RustWrenEngineColumnType.JSON,
            "BLOB WITH JSON": RustWrenEngineColumnType.JSON,
            "CLOB WITH JSON": RustWrenEngineColumnType.JSON,
        }
        return switcher.get(data_type.upper(), RustWrenEngineColumnType.UNKNOWN)
