import ibis

from typing import List
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
                v.owner AS TABLE_CATALOG,
                v.owner AS TABLE_SCHEMA,
                v.view_name AS TABLE_NAME,
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
                # For Oracle, just use the table name (no schema prefix)
                # The connection is already authenticated to the correct schema
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description=row["TABLE_COMMENT"],
                    columns=[],
                    properties=TableProperties(
                        schema="",  # Empty - not needed
                        catalog="",  # Oracle doesn't use catalogs
                        table=schema_table,  # Just the table name with quotes if needed
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


    def get_constraints(self) -> List[Constraint]:
        """
        Auto-detect view-to-view relationships using column naming patterns.

        TEMPORARILY DISABLED: Returning empty list to unblock development.
        The column query against all_tab_columns appears to hang on large view sets.

        TODO: Implement alternative approach:
        - Use table metadata already retrieved in get_table_list()
        - Cache column information to avoid repeated queries
        - Or implement manual relationship definition UI

        Returns:
            Empty list (relationships disabled for now)
        """
        return []

        # ORIGINAL CODE COMMENTED OUT - WAS HANGING ON COLUMN QUERY
        # constraints = []
        # try:
        #     print(f"🔍 Starting constraint detection for user {user}...")
        #
        #     # Step 1: Get list of views first (fast query)
        #     views_schema = ibis.schema({"VIEW_NAME": "string"})
        #     views_sql = f"""
        #         SELECT view_name AS VIEW_NAME
        #         FROM all_views
        #         WHERE owner = '{user}'
        #     """
        #     views_df = self.connection.sql(views_sql, schema=views_schema).to_pandas()
        #     view_list = views_df['VIEW_NAME'].tolist()
        #
        #     print(f"🔍 Found {len(view_list)} views")
        #
        #     if not view_list:
        #         return []
        #
        #     # Step 2: Get columns for those views (single query with explicit list)
        #     columns_schema = ibis.schema({
        #         "TABLE_NAME": "string",
        #         "COLUMN_NAME": "string",
        #         "COLUMN_ID": "int64"
        #     })
        #
        #     # Use IN clause with explicit list instead of subquery
        #     view_list_str = "', '".join(view_list)
        #     columns_sql = f"""
        #         SELECT
        #             table_name AS TABLE_NAME,
        #             column_name AS COLUMN_NAME,
        #             column_id AS COLUMN_ID
        #         FROM all_tab_columns
        #         WHERE owner = '{user}'
        #         AND table_name IN ('{view_list_str}')
        #         ORDER BY table_name, column_id
        #     """
        #
        #     print(f"🔍 Querying columns for {len(view_list)} views...")
        #     columns_df = self.connection.sql(columns_sql, schema=columns_schema).to_pandas()
        #     print(f"🔍 Retrieved {len(columns_df)} total columns")
        #
        #     if columns_df.empty:
        #         return []
        #
        #     # Build index: view_name -> list of column names
        #     view_columns = {}
        #     for _, row in columns_df.iterrows():
        #         view = row['TABLE_NAME']
        #         col = row['COLUMN_NAME']
        #         if view not in view_columns:
        #             view_columns[view] = []
        #         view_columns[view].append(col)
        #
        #     # Identify primary key columns per view
        #     # Pattern: column name ends with "Primary Key"
        #     # Build entity -> views map (which views have this entity as PK)
        #     entity_to_views = {}  # entity name -> list of (view, pk_column)
        #     view_primary_keys = {}  # view -> list of PK columns
        #
        #     print(f"🔍 Analyzing {len(view_columns)} views for Primary Key columns...")
        #
        #     for view, columns in view_columns.items():
        #         pks = [col for col in columns if col.endswith('Primary Key')]
        #         if pks:
        #             view_primary_keys[view] = pks
        #             # Map each entity to the views that have it as PK
        #             for pk in pks:
        #                 entity = pk.replace(' Primary Key', '')
        #                 if entity not in entity_to_views:
        #                     entity_to_views[entity] = []
        #                 entity_to_views[entity].append((view, pk))
        #
        #     print(f"🔍 Found {len(view_primary_keys)} views with Primary Key columns")
        #     print(f"🔍 Identified {len(entity_to_views)} distinct entities: {list(entity_to_views.keys())[:10]}...")
        #
        #     # Detect relationships by matching column names
        #     for source_view, source_cols in view_columns.items():
        #         # Find foreign key candidates (columns ending with "Primary Key")
        #         fk_candidates = [col for col in source_cols if col.endswith('Primary Key')]
        #
        #         for fk_col in fk_candidates:
        #             # Extract entity name from FK column
        #             entity = fk_col.replace(' Primary Key', '')
        #
        #             # Find views that have this entity as their primary key
        #             if entity in entity_to_views:
        #                 for target_view, target_pk_col in entity_to_views[entity]:
        #                     if source_view == target_view:
        #                         # Skip self-references (e.g., Part Primary Key in RT Parts table itself)
        #                         continue
        #
        #                     # Create relationship!
        #                     constraint_name = f"FK_{source_view}_{target_view}_{entity}".replace(" ", "_")
        #
        #                     constraints.append(
        #                         Constraint(
        #                             constraintName=constraint_name[:128],  # Oracle name limit
        #                             constraintTable=self._format_compact_table_name(user, source_view),
        #                             constraintColumn=fk_col,
        #                             constraintedTable=self._format_compact_table_name(user, target_view),
        #                             constraintedColumn=target_pk_col,
        #                             constraintType=ConstraintType.FOREIGN_KEY,
        #                         )
        #                     )
        #
        #     print(f"✅ Detected {len(constraints)} view relationships using column naming patterns")
        #     if constraints:
        #         print(f"📊 Sample relationships:")
        #         for c in constraints[:5]:
        #             print(f"  - {c.constraintTable}.{c.constraintColumn} -> {c.constraintedTable}.{c.constraintedColumn}")
        #     return constraints
        #
        # except Exception as e:
        #     # If auto-detection fails, return empty list rather than breaking WrenAI
        #     # This ensures the system remains functional even with permission issues
        #     print(f"Warning: Could not auto-detect view relationships: {e}")
        #     return []

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
        Format Oracle table/view name with explicit quoting.

        Oracle views/tables in this system ALL have spaces in their names.
        We must quote them in the MDL manifest just like column expressions are quoted.

        This matches the pattern used for column expressions:
        - Column: "expression":"\"Part Primary Key\""
        - Table:  "table":"\"RT SN Claim\""

        When JSON is parsed, the escaped quotes become actual quotes, so wren-engine
        receives: "RT SN Claim" as a single quoted identifier.

        Args:
            schema: Oracle schema name (not used - connection is already authenticated)
            table: Table/view name (contains spaces, needs quoting)

        Returns:
            Quoted table name: "TABLE NAME" (no schema prefix - connection handles that)
        """
        # Only return the table name with quotes - no schema prefix needed
        # The Oracle connection is already authenticated to the correct schema
        return f'"{table}"'

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
