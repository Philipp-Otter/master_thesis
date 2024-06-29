from typing import Union

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from src.core.config import settings
from src.utils.utils import print_info
from src.core.enums import MigrationTables

class DBBridge:
    def __init__(
        self,
        engine,
        db_name_foreign: str = settings.POSTGRES_DB_RD,
        user_foreign: str = settings.POSTGRES_USER_RD,
        host_foreign: str = settings.POSTGRES_HOST_RD,
        port_foreign: Union[str, int] = settings.POSTGRES_PORT_RD,
        password_foreign: str = settings.POSTGRES_PASSWORD_RD,
        schema_foreign: str = settings.POSTGRES_SCHEMA_RD,
    ):
        self.engine = engine
        self.schema = settings.POSTGRES_SCHEMA_GOAT
        self.db_name_foreign = db_name_foreign
        self.user_foreign = user_foreign
        self.host_foreign = host_foreign
        self.port_foreign = port_foreign
        self.password_foreign = password_foreign
        self.schema_foreign = schema_foreign
        self.schema_bridge = "foreign_" + schema_foreign + "_" + db_name_foreign
        self.information_schema = "information_schema"
        self.information_schema_bridge = "foreign_information_schema_" + db_name_foreign

    def upgrade_postgres_fdw(self):
        """Create postgres_fdw extension on database if not exists."""
        self.engine.execute(
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"
        )

    def downgrade_postgres_fdw(self):
        """Drop postgres_fdw extension on database if exists."""
        self.engine.execute(
            "DROP EXTENSION IF EXISTS postgres_fdw;"
        )

    def upgrade_foreign_server(
        self,
    ):
        """Create foreign server on database."""
        create_foreign_server = text(
            f"""
            CREATE SERVER {self.db_name_foreign}
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host :host, port :port, dbname :dbname);
            """
        )
        values = {
            "host": self.host_foreign,
            "port": str(self.port_foreign),
            "dbname": self.db_name_foreign,
        }
        self.engine.execute(create_foreign_server, values)
        print_info(f"Foreign server {self.db_name_foreign} created.")

    def downgrade_foreign_server(self):
        """Drop foreign server on database."""

        drop_foreign_server = text(
            f"DROP SERVER IF EXISTS {self.db_name_foreign} CASCADE;"
        )
        values = {"foreign_server": "foreign_server"}
        self.engine.execute(drop_foreign_server, values)

    def upgrade_mapping_user(
        self,
    ):
        """Create user mapping on database for foreign server."""
        create_mapping_user = text(
            f"""CREATE USER MAPPING FOR {settings.POSTGRES_USER_GOAT}
                SERVER {self.db_name_foreign}
                OPTIONS (user :server_user, password :password);
            """
        )
        values = {"server_user": self.user_foreign, "password": self.password_foreign}
        self.engine.execute(create_mapping_user, values)
        print_info(f"User mapping for {self.db_name_foreign} created.")

    def downgrade_mapping_user(self):
        """Drop user mapping on database for foreign server."""
        drop_mapping_user = text(
            f"DROP USER MAPPING IF EXISTS FOR {settings.POSTGRES_USER} SERVER {self.db_name_foreign};"
        )
        self.engine.execute(drop_mapping_user)

    def upgrade_schema(self, schema: str):
        """Create schema on database for migration.

        Args:
            schema (str): Schema name.
        """
        create_foreign_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        self.engine.execute(text(create_foreign_schema))

    def downgrade_schema(self):
        """Drop schema on database for migration."""

        drop_foreign_schema = f"DROP SCHEMA IF EXISTS {self.schema_bridge};"
        self.engine.execute(text(drop_foreign_schema))

    def create_bridge(self, schema_bridge: str, schema_foreign: str):
        """Imports the tables from the foreign schema into the bridge schema.

        Args:
            schema_bridge (str): Local schema name where foreign tables are added.
            schema_foreign (str): Foreign schema name.
        """
        self.upgrade_schema(schema_bridge)
        tables_to_import = MigrationTables.__members__.values()
        tables_to_import_str = ", ".join(tables_to_import)
        create_foreign_table = f"""IMPORT FOREIGN SCHEMA {schema_foreign}
        LIMIT TO ({tables_to_import_str}, columns)
        FROM SERVER {self.db_name_foreign} INTO {schema_bridge};"""
        self.engine.execute(text(create_foreign_table))
        print_info(f"Foreign schema {schema_foreign} imported into {schema_bridge}.")

    def upgrade_foreign_tables(
        self,
    ):
        """Imports the tables from the foreign schema into the bridge schema."""
        # Information schema is needed for the foreign tables
        self.create_bridge(self.information_schema_bridge, self.information_schema)
        # Read specified schema from foreign database
        self.create_bridge(self.schema_bridge, self.schema_foreign)

    def downgrade_foreign_tables(self, table_name: Union[str, list[str]]):
        """Drop foreign tables on database.

        Args:
            table_name (Union[str, list[str]]): Table name or list of table names.
        """
        if type(table_name) == str:
            table_names = [table_name]
        else:
            table_names = table_name
        for table_name in table_names:
            full_table_name = f"{self.schema_bridge}.{table_name}"
            drop_foreign_table = text(
                f"""DROP FOREIGN TABLE IF EXISTS {full_table_name};"""
            )

            self.engine.execute(drop_foreign_table)

    def bridge_initialize(self):
        """Initialize the migration."""
        self.upgrade_postgres_fdw()
        self.downgrade_foreign_server()
        self.upgrade_foreign_server()
        self.upgrade_mapping_user()
        self.upgrade_foreign_tables()
