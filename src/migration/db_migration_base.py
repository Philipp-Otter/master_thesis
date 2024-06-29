from sqlalchemy import text
from src.core.config import settings
from src.utils.utils import (
    print_info,
    print_warning,
    create_table_dump,
    restore_table_dump,
)
from src.migration.db_bridge import DBBridge
from src.core.enums import MigrationTables, StudyAreaGeomMigration


class DBMigrationBase(DBBridge):
    def __init__(self, db_source, db_target, study_area_ids: list[int]):
        """Migration class.

        Args:
            engine_source (_type_): Sync SQLAlchemy engine_source.
            engine_target (_type_): Sync SQLAlchemy engine_target.
            study_area_ids (list[int]): List of study area ids.
        """
        # Create engines
        self.db_source = db_source
        self.engine_source = db_source.return_sqlalchemy_engine()

        self.db_target = db_target
        self.engine_target = db_target.return_sqlalchemy_engine()

        super().__init__(engine=self.engine_target)
        self.study_area_ids = study_area_ids
        # TODO: Check where to out this variable.
        self.schema_migration = "migration"

    def check_table_schema_matches(self, table_name: MigrationTables):
        """Check if the schema of the old table matches the schema of the foreign table.

        Args:
            table_name (str): Table name.

        Raises:
            Exception: If the schema of the old table does not match the schema of the foreign table.
        """
        stmt = text(
            f"""
                WITH cte_info AS (
                    SELECT column_name, udt_name
                    FROM {self.information_schema}.columns
                    WHERE table_schema = '{self.schema}'
                    AND table_name  = '{table_name}'
                ), cte_foreign_info AS (
                    SELECT column_name, udt_name
                    FROM {self.information_schema_bridge}.columns
                    WHERE table_schema = '{self.schema_foreign}'
                    AND table_name  = '{table_name}'
                )
                SELECT CASE
                    WHEN EXISTS (
                        SELECT * FROM cte_info
                        EXCEPT
                        SELECT * FROM cte_foreign_info
                    ) OR EXISTS (
                        SELECT * FROM cte_foreign_info
                        EXCEPT
                        SELECT * FROM cte_info
                    )
                    THEN FALSE
                    ELSE TRUE
                END AS identical;
            """
        )
        result = self.engine_target.execute(stmt)
        result = result.fetchone()

        # Check if result is empty if not the schema is not the same and the migration should be aborted.
        if result[0] is False:
            raise Exception(
                f"Schema of table {table_name} does not match the schema of the foreign table."
            )
        else:
            print_info(
                f"Schema of table {table_name} matches the schema of the foreign table."
            )

    def create_migration_table(
        self, table_name: MigrationTables, index_columns: list[str]
    ) -> list[str]:
        """Creates the migration table and returns the column names of the created table.

        Args:
            table_name (MigrationTables): Table name.
            index_columns (list[str]): Index columns. It will create the default PostgreSQL index for the specified columns together.

        Returns:
            list[str]: Column names of created table.
        """
        # Get columns of table and their types.
        columns = self.get_column_from_table(table_name)

        column_names = []
        data_types = []
        for column in columns:
            data_types.append(column[1])
            data_types.append("text")
            column_names.append(column[0])
            column_names.append(f"{column[0]}_check")

        # Create columns for migration table.
        columns_type = ""
        for column_name, column_type in zip(column_names, data_types):
            columns_type += f"{column_name} {column_type}, "
        columns_type = columns_type[:-2]

        sql_base = f"""
            DROP TABLE IF EXISTS {self.schema_migration}.{table_name}_to_migrate;
            CREATE TABLE {self.schema_migration}.{table_name}_to_migrate (
                {columns_type},
                action text CHECK (action in ('insert', 'update', 'delete'))
            );"""
        self.engine_target.execute(text(sql_base))

        # Create index for migration table.
        index_columns = ", ".join(index_columns)
        sql_index = f"""
            CREATE INDEX {table_name}_index ON {self.schema_migration}.{table_name}_to_migrate ({index_columns});
        """
        self.engine_target.execute(text(sql_index))
        return column_names, data_types

    def get_column_from_table(self, table_name: MigrationTables) -> list[list[str]]:
        """Gets the column names and their types from the table.

        Args:
            table_name (MigrationTables): Table name.

        Returns:
            list[list[str]]: Nested list with column names and their types.
        """

        # Get columns of table and their types.
        get_columns = text(
            f"""
                SELECT column_name, udt_name
                FROM {self.information_schema}.columns
                WHERE table_schema = '{self.schema}'
                AND table_name = '{table_name}'
            """
        )
        columns = self.engine_target.execute(get_columns)
        columns = [list(column) for column in columns.fetchall()]
        return columns

    def create_on_condition(self, columns_to_match: list[str]) -> str:
        """Create the join 'and' condition based on the columns that should be matched.

        Args:
            columns_to_match (list[str]): Define the column names that are used to match the rows via 'and' condition.

        Returns:
            str: SQL query part.
        """

        sql_on_condition = ""
        for column in columns_to_match:
            sql_on_condition += f"old_data.{column} = new_data.{column} AND "
        sql_on_condition = sql_on_condition[:-5]
        return sql_on_condition

    def get_data_to_migrate(self, table_name: MigrationTables) -> str:
        """Prepares a query to select the relevant rows from the table using study area filter and table specific logic.

        Args:
            table_name (MigrationTables): Table name.

        Raises:
            Exception: Raises expection when a table name is passed that is not supported for migration.
        """

        sql_select_query = f"""
            DROP TABLE IF EXISTS {self.schema_migration}.{table_name};
            CREATE TABLE {self.schema_migration}.{table_name} AS
        """
        # Custom logic for each supported table.
        if table_name == MigrationTables.study_area.value:
            sql_select_query += f"""
                SELECT *
                FROM {self.schema}.{table_name} 
                WHERE id IN ({str(self.study_area_ids)[1:-1]})
            """
        elif table_name == MigrationTables.sub_study_area.value:
            sql_select_query += f"""
                SELECT *
                FROM {self.schema}.{table_name}
                WHERE study_area_id IN ({str(self.study_area_ids)[1:-1]})
            """
        elif table_name == MigrationTables.poi.value:
            sql_select_query += f"""
                SELECT DISTINCT p.*
                FROM {self.schema}.{table_name} p, {self.schema}.study_area s
                WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
                AND ST_Intersects(s.{StudyAreaGeomMigration.poi.value}, p.geom)
            """
        elif table_name == MigrationTables.node.value:
            sql_helper_table = f"""
                DROP TABLE IF EXISTS {self.schema_migration}.{table_name}_ids;
                CREATE TABLE {self.schema_migration}.{table_name}_ids AS
                SELECT DISTINCT UNNEST(ARRAY[e.source, e.target]) AS id
                FROM {self.schema}.{MigrationTables.edge.value} e, {self.schema}.study_area s
                WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
                AND ST_Intersects(s.{StudyAreaGeomMigration.edge.value}, e.geom); 
                CREATE INDEX ON {self.schema_migration}.{table_name}_ids (id);
            """
            self.engine_source.execute(text(sql_helper_table))
            sql_select_query += f""" 
                SELECT n.*
                FROM {self.schema}.{table_name} n, {self.schema_migration}.{table_name}_ids ids
                WHERE n.id = ids.id;
            """
        elif table_name == MigrationTables.edge.value:
            sql_select_query += f"""
                SELECT e.*
                FROM {self.schema}.{MigrationTables.edge.value} e, {self.schema}.study_area s
                WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
                AND ST_Intersects(s.{StudyAreaGeomMigration.edge.value}, e.geom); 
            """
        elif table_name == MigrationTables.aoi.value:
            sql_select_query += f"""
                SELECT DISTINCT a.*
                FROM {self.schema}.{table_name} a, {self.schema}.study_area s
                WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
                AND ST_Intersects(s.{StudyAreaGeomMigration.aoi.value}, a.geom);
            """
        elif table_name == MigrationTables.population.value:
            sql_select_query += f"""
            SELECT p.*
            FROM {self.schema}.{table_name} p, {self.schema}.study_area s
            WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
            AND ST_Intersects(s.{StudyAreaGeomMigration.population.value}, p.geom);
            """
        elif table_name == MigrationTables.building.value:
            sql_select_query += f"""
            SELECT b.*
            FROM {self.schema}.{table_name} b, {self.schema}.study_area s
            WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
            AND ST_Intersects(s.{StudyAreaGeomMigration.building.value}, b.geom)
            AND ST_Intersects(s.{StudyAreaGeomMigration.building.value}, ST_CENTROID(b.geom));
            """
        else:
            raise Exception(f"Table {table_name} is not supported for migration.")

        self.engine_source.execute(text(sql_select_query))
        # Dump table from source database.
        create_table_dump(
            db_config=self.db_source.db_config,
            schema=self.schema_migration,
            table_name=table_name,
        )
        # Restore table to target database.
        restore_table_dump(
            db_config=self.db_target.db_config,
            schema=self.schema_migration,
            table_name=table_name,
        )
        # Create indices for the migration table.
        self.engine_target.execute(
            text(
                f"""
                ALTER TABLE {self.schema_migration}.{table_name} ADD PRIMARY KEY (id);
                CREATE INDEX ON {self.schema_migration}.{table_name} USING GIST (geom);
                """
            )
        )

    def prepare_rows_to_update(
        self,
        table_name: MigrationTables,
        columns_to_match: list[str],
        columns_to_exclude: list[str] = [],
    ):
        """Select the rows that have a match in the existing table and inserts them into the migration table.

        Args:
            table_name (MigrationTables): Table name.
            columns_to_match (list[str]): Define the column names that are used to match the rows via 'and' condition.
            columns_to_exclude (list[str], optional): Specify the columns that should be excluded for the update. Defaults to [].
        """

        # Get columns of table and their types.
        columns = self.get_column_from_table(table_name)

        # Create the On condition for the SQL join.
        sql_on_condition = self.create_on_condition(columns_to_match)

        # Create the Where condition for the SQL join.
        # TODO: Add different logic for geometry type and work on performance. Hausdorff distance?
        sql_where_condition = ""
        sql_select_condition = ""
        column_names = []
        for column in columns:
            column_name, data_type = column

            # Check if column is in the list of columns to exclude then avoid the check and set check type to be excluded.
            if column_name in columns_to_exclude:
                sql_select_condition += (
                    f"new_data.{column_name}, 'excluded' AS {column_name}_check,"
                )
            elif column_name == "id":
                sql_select_condition += (
                    f"old_data.{column_name}, 'excluded' AS {column_name}_check,"
                )
            # Check if column is a geometry column and use ST_ASTEXT to compare the geometry.
            elif data_type == "geometry":
                sql_select_condition += f"""
                    new_data.{column_name}, CASE WHEN ST_ASTEXT(old_data.{column_name}) <> ST_ASTEXT(new_data.{column_name}) 
                    THEN 'changed' ELSE 'unchanged' 
                    END AS {column_name}_check,
                """
                sql_where_condition += f"ST_ASTEXT(old_data.{column_name}) <> ST_ASTEXT(new_data.{column_name}) OR "
            # If column is not a geometry column then use the <> operator to compare the values.
            else:
                sql_select_condition += f"""
                    new_data.{column_name}, CASE WHEN old_data.{column_name} <> new_data.{column_name}
                    THEN 'changed' ELSE 'unchanged'
                    END AS {column_name}_check,
                """
                sql_where_condition += (
                    f"old_data.{column_name} <> new_data.{column_name} OR "
                )
            column_names.append(column_name)
            column_names.append(f"{column_name}_check")

        sql_where_condition = sql_where_condition[:-4]

        # Merge query parts.
        stmt = text(
            f"""
                INSERT INTO {self.schema_migration}.{table_name}_to_migrate ({', '.join(column_names)}, action)
                SELECT {sql_select_condition} 'update' as action
                FROM {self.schema}.{table_name} old_data 
                LEFT JOIN {self.schema_migration}.{table_name} new_data
                ON {sql_on_condition} 
                AND ({sql_where_condition})
                WHERE new_data.{columns_to_match[0]} IS NOT NULL;
            """
        )
        self.engine_target.execute(stmt)

    def prepare_rows_to_insert(
        self, table_name: MigrationTables, columns_to_match: list[str]
    ):
        """Select the new rows and inserts them into the migration table.

        Args:
            table_name (MigrationTables): Table name.
            columns_to_match (list[str]): Define the column names that are used to match the rows via 'and' condition.
        """
        # Get columns of table and their types.
        columns = self.get_column_from_table(table_name)

        # Get on condition for the SQL join.
        sql_on_condition = self.create_on_condition(columns_to_match)

        # Insert statement for new data.
        stmt = text(
            f"""
                INSERT INTO {self.schema_migration}.{table_name}_to_migrate ({', '.join([column[0] for column in columns])}, action)
                SELECT {', '.join(["new_data." + column[0] for column in columns])}, 'insert' as action
                FROM  {self.schema_migration}.{table_name} new_data
                LEFT JOIN {self.schema}.{table_name} old_data
                ON {sql_on_condition} 
                WHERE old_data.{columns_to_match[0]} IS NULL;
            """
        )
        self.engine_target.execute(stmt)

    def prepare_rows_to_delete(
        self, table_name: MigrationTables, columns_to_match: list[str]
    ):
        """Select the rows that have no match in the existing table and inserts them into the migration table.

        Args:
            table_name (MigrationTables): _description_
            columns_to_match (list[str]): _description_
            study_area_id (int): _description_
        """
        # Get columns of table and their types.
        columns = self.get_column_from_table(table_name)

        # Get on condition for the SQL join.
        sql_on_condition = self.create_on_condition(columns_to_match)

        # Insert statement for new data.
        stmt = text(
            f"""
                INSERT INTO {self.schema_migration}.{table_name}_to_migrate ({', '.join([column[0] for column in columns])}, action)
                SELECT {', '.join(["old_data." + column[0] for column in columns])}, 'delete' as action
                FROM (
                    SELECT t.* 
                    FROM {self.schema}.{table_name} t, {self.schema}.study_area s
                    WHERE s.id IN ({str(self.study_area_ids)[1:-1]})
                    AND ST_Intersects(s.{StudyAreaGeomMigration.__dict__[table_name].value}, t.geom) 
                ) old_data
                LEFT JOIN {self.schema_migration}.{table_name} new_data
                ON {sql_on_condition} 
                WHERE new_data.{columns_to_match[0]} IS NULL;
            """
        )
        self.engine_target.execute(stmt)

    def prompt_user_check(self):
        """Prompts the user to check the migration table and to confirm the migration."""
        # Ask user if migration table has been checked.
        print_warning("Have you checked the migration table? Is everything correct? (y/n)")
        answer = input()
        if answer == "n":
            raise Exception(
                "Please check the migration table and run the migration again."
            )
        elif answer == "y":
            print_info("Migrating data...")
        else:
            raise Exception("Please answer with y or n.")

    def insert_migration_data(self, table_name: MigrationTables, columns: list[str]):
        """Inserts the data from the migration table into the new database.

        Args:
            table_name (MigrationTables): Table name.
            columns (list[str]): List of column names.
        """
        # Insert new data from migration table
        stmt = text(
            f"""
                INSERT INTO {self.schema}.{table_name} ({', '.join([column[0] for column in columns])})
                SELECT {', '.join([column[0] for column in columns])}
                FROM {self.schema_migration}.{table_name}_to_migrate
                WHERE action = 'insert'; 
            """
        )
        self.engine_target.execute(stmt)

    def create_migration_schemas(self):
        """Creates the migration schemas at the source and target database."""
        # Create schema for migration at Source DB.
        self.engine_source.execute(
            text(
                f"""
                DROP SCHEMA IF EXISTS migration CASCADE;
                CREATE SCHEMA IF NOT EXISTS migration;
                """
            )
        )
        # Create schema for migration at Target DB.
        self.engine_target.execute(
            text(
                f"""
                DROP SCHEMA IF EXISTS migration CASCADE;
                CREATE SCHEMA IF NOT EXISTS migration;
                """
            )
        )

    def perform_standard_migration(
        self,
        table_name: MigrationTables,
        columns_to_match: list[str],
        columns_to_exclude: list[str] = [],
        with_delete: bool = True,
    ):
        """Perform the migration for the given table using the provided conditions.

        Args:
            table_name (MigrationTables): Table name.
            columns_to_match (list[str]): Define the column names that are used to match the rows via 'and' condition.
            columns_to_exclude (list[str], optional): Specify the columns that should be excluded for the update. Defaults to [].

        Raises:
            Exception: Raises expection when the user does answer with n.
            Exception: Raises expection when the user does answer with n or y.
        """

        # Check if table is node or edge table. If so then exit.
        if table_name == MigrationTables.node or table_name == MigrationTables.edge:
            raise Exception(
                "Please use the insert_network function to migrate the node and edge tables."
            )

        print_info(f"Starting migration for table {table_name}...")

        # Check if table schema matches the schema in the migration table.
        self.check_table_schema_matches(table_name)

        # Create migration table.
        self.create_migration_table(table_name, ["id"])

        # Create the query to select the relevant rows to be checked for migration.
        self.get_data_to_migrate(table_name)

        print_info(f"Starting migration for study areas: {self.study_area_ids}...")
        self.prepare_rows_to_update(
            table_name,
            columns_to_match,
            columns_to_exclude=columns_to_exclude,
        )
        self.prepare_rows_to_insert(
            table_name,
            columns_to_match=columns_to_match,
        )
        
        if with_delete == True:
            self.prepare_rows_to_delete(table_name, columns_to_match=columns_to_match)

        # Ask user if migration table has been checked.
        self.prompt_user_check()

        # Get columns of table and their types.
        columns = self.get_column_from_table(table_name)

        # Insert new data from migration table
        self.insert_migration_data(table_name, columns)

        # Update existing data from migration table
        # Build the SET part of the SQL statement. Make sure to only update the columns that have changed.
        update_column_sql = ""
        for column in columns:
            update_column_sql += f"""{column[0]} = CASE WHEN migration_table.{column[0]}_check IN ('unchanged', 'excluded') THEN {self.schema}.{table_name}.{column[0]} ELSE migration_table.{column[0]} END,"""
        update_column_sql = update_column_sql[:-1]

        # Combine the SQL statement parts.
        stmt = text(
            f"""
                UPDATE {self.schema}.{table_name}
                SET {update_column_sql}
                FROM {self.schema_migration}.{table_name}_to_migrate migration_table
                WHERE {self.schema}.{table_name}.id = migration_table.id
                AND migration_table.action = 'update';
            """
        )
        self.engine_target.execute(stmt)

        if with_delete == True:
            # Delete data from migration table
            stmt = text(
                f"""
                    DELETE FROM {self.schema}.{table_name}
                    USING {self.schema_migration}.{table_name}_to_migrate migration_table
                    WHERE {self.schema}.{table_name}.id = migration_table.id
                    AND migration_table.action = 'delete';
                """
            )
            self.engine_target.execute(stmt)
