from src.migration.db_migration_base import DBMigrationBase
from src.core.enums import MigrationTables
from src.utils.utils import (
    print_info,
    print_hashtags,
)
from src.core.config import settings
from sqlalchemy import text


# TODO: Finish this
# TODO: Add logic for the other tables
class DBMigration(DBMigrationBase):
    def __init__(self, db_source, db_target, study_area_ids: list[int]):
        """Migration class.

        Args:
            engine_source (_type_): Sync SQLAlchemy engine_source.
            engine_target (_type_): Sync SQLAlchemy engine_target.
            study_area_ids (list[int]): List of study area ids.
        """
        super().__init__(
            db_source=db_source, db_target=db_target, study_area_ids=study_area_ids
        )

    def insert_network(self):

        # Check if table schema matches the schema in the migration table.
        self.check_table_schema_matches(MigrationTables.node.value)
        self.check_table_schema_matches(MigrationTables.edge.value)

        # Get columns of table and their types.
        self.create_migration_table(MigrationTables.node.value, ["id"])[
            0
        ]
        self.create_migration_table(MigrationTables.edge.value, ["id"])[
            0
        ]

        print_info(f"Starting migration for study areas {self.study_area_ids}...")

        # Get data to migrate.
        self.get_data_to_migrate(MigrationTables.node.value)
        self.get_data_to_migrate(MigrationTables.edge.value)

        self.prepare_rows_to_insert(
            MigrationTables.node.value,
            columns_to_match=["id"],
        )
        self.prepare_rows_to_insert(
            MigrationTables.edge.value,
            columns_to_match=["id"],
        )

        # Ask user if migration table has been checked.
        self.prompt_user_check()

        # Delete scenarios from network tables in reverse order. First edges and then nodes
        # TODO: In future we should not store the scenarios in the network tables but in seperate tables.
        for table_name in [MigrationTables.edge.value, MigrationTables.node.value]:
            self.engine_target.execute(
                text(
                    f"""
                    DELETE FROM {self.schema}.{table_name}
                    WHERE scenario_id IS NOT NULL
                    """
                )
            )
        # Insert data from migration table into network tables and reset serial.
        for table_name in [MigrationTables.node.value, MigrationTables.edge.value]:
            # Insert new data from migration table
            self.insert_migration_data(
                table_name,
                columns=self.get_column_from_table(table_name),
            )
            # Reset serial columns
            self.engine_target.execute(
                text(
                    f"""
                    SELECT setval('basic.{table_name}_id_seq', (SELECT max(id) FROM basic.{table_name}));
                    """
                )
            )
        # Recompute scenarios
        # Get relevant scenario ids
        scenario_ids = self.engine_target.execute(
            text(
                """
                SELECT DISTINCT scenario_id
                FROM customer.way_modified
                """
            )
        )
        scenario_ids = [scenario_id[0] for scenario_id in scenario_ids.fetchall()]
        for scenario_id in scenario_ids:
            self.engine_target.execute(
                text(
                    f"""
                    SELECT basic.network_modification({scenario_id})
                    """
                )
            )
            print_info(f"Recomputed scenario {scenario_id}.")

        print_info(f"Finished network migration for study areas {self.study_area_ids}.")


def main():

    print_hashtags()
    print_info("Starting migration...")
    print_hashtags()

    from src.db.db import Database

    # Connect to databases.
    db_source = Database(settings.RAW_DATABASE_URI)
    db_target = Database(settings.GOAT_DATABASE_URI)

    # Initialize migration.
    migration = DBMigration(
        db_source=db_source,
        db_target=db_target,
        study_area_ids=[
            6412,
            6533,
            6413,
            6534,
            6535,
            6631,
            6414,
            6434,
            6435,
            6436,
            6438,
            6439,
            6440,
            6531,
            6532
            # 9571,
            # 9677,
            # 8125,
            # 9679,
            # 9575,
            # 9676,
            # 9561,
            # 9663,
            # 6411,
            # 6432,
            # 6433,
            # 6437,
            # 7133,
            # 7134,
            # 8211,
            # 7339,
            # 8121,
            # 8126,
            # 8127,
            # 8212,
            # 8215,
            # 10043,
            # 8216,
            # 10045,
            # 10046,
            # 10041,
            # 99999998,
            # 99999997,
            # 99999996,
            # 99999995,
        ],
    )
    # Initialize FDW bridge.
    migration.bridge_initialize()
    # Create migration schemas.
    migration.create_migration_schemas()

    # Perform migration for Study Area and Sub Study Area.
    migration.perform_standard_migration("study_area", columns_to_match=["id"], columns_to_exclude=["setting"], with_delete=False)
    # migration.perform_standard_migration("sub_study_area", columns_to_match=["area", "study_area_id"], with_delete=False)
    # migration.perform_standard_migration(
    #     "building", columns_to_match=["id"], with_delete=False
    # )
    # migration.perform_standard_migration("population", columns_to_match=["id"], with_delete=False)
    # migration.insert_network()
    # migration.perform_standard_migration("aoi", columns_to_match=["id"])
    # migration.perform_standard_migration(
    #     "poi", columns_to_match=["uid"], columns_to_exclude=["id"]
    # )

    print_hashtags()
    print_info("Migration finished.")
    print_hashtags()

    db_source.conn.close()
    db_target.conn.close()


if __name__ == "__main__":
    main()
