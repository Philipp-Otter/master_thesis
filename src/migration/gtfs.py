from src.utils.utils import print_info, create_table_dump, restore_table_dump
from src.core.config import settings
from src.db.db import Database
from src.config.config import Config
from src.core.enums import DumpType

def migrate_gtfs(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    db_goat = Database(settings.GOAT_DATABASE_URI)

    config = Config("gtfs", region)
    schema = config.preparation["target_schema"]
    
    # Migrate table by table
    tables = ["stops"] #, "stop_times_optimized", "shape_dist_region"]
    for table in tables:
        # Create and restore schema
        create_table_dump(db.db_config, schema, table, dump_type=DumpType.schema)
        db_goat.perform(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
        restore_table_dump(db_goat.db_config, {schema}, table, dump_type=DumpType.schema)
        
        # Make table distributed
        db_goat.perform(f"SELECT create_distributed_table('{schema}.{table}', 'h3_3');")
        
        # Insert data
        create_table_dump(db.db_config, schema, table, dump_type=DumpType.data)
        restore_table_dump(db_goat.db_config, {schema}, table, dump_type=DumpType.data)
        
        print_info(f"Table {table} migrated to GOAT database.")

