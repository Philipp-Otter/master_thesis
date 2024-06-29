from src.db.db import Database
from src.core.config import settings
from src.utils.utils import print_info
import os 

class PrepareDB:
    def __init__(self, db):
        self.db = db

    def create_db_functions(self):
        """This function prepares the database to run the function produce_population_points()."""
        print_info("Checking database functions exists...")
        path = "src/db/functions/"
        
        #Create db functions
        for file_name in os.listdir(path):
            if file_name.endswith(".sql"):
                self.db.perform(query = open(os.path.join(path, file_name), 'r', encoding = 'utf8').read())
        
    def create_db_extensions(self):
        print_info("Checking database extensions exists...")
        # Create db extensions
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS intarray;")
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS postgis;")
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS postgis_raster;")
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS hstore;")

    def create_db_schemas(self):
        # Create db schemas
        print_info("Checking database schemas exists...")
        self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS basic;")
        self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS extra;")
        self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS temporal;")    

def main():
    db = Database(settings.LOCAL_DATABASE_URI)
    prepare_db = PrepareDB(db)
    prepare_db.create_db_extensions()
    prepare_db.create_db_schemas()
    prepare_db.create_db_functions()

if __name__ == "__main__":
    main()
    