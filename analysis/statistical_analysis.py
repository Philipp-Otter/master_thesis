import time

from src.core.config import settings
from src.db.db import Database
from src.utils.utils import timing
from analysis.master_thesis_philipp_config import poi_dicts


class Statistical_Analysis:
    def __init__(self, db, db_rd=None):
        self.db = db
        self.db_rd = db_rd

    def table_exists(self, table_name):
        """
        Check if a table exists in the database.
        """
        query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name='{table_name}'
            );
        """
        return self.db.select(query)[0][0]

    def ensure_h3_db_functions_exist(self):
        """
        Check if the necessary H3 functions exist, and create them if they don't.
        """
        print("Initializing H3 helper functions...")

        function_creation_sql = {
            9: """
                CREATE OR REPLACE FUNCTION basic.to_short_h3_9(bigint) RETURNS bigint
                AS $$ select ($1 & 'x00ffffffffff0000'::bit(64)::bigint>>16)::bit(64)::bigint;$$
                LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;
            """,
            10: """
                CREATE OR REPLACE FUNCTION basic.to_short_h3_10(bigint) RETURNS bigint
                AS $$ select ($1 & 'x00fffffffffff000'::bit(64)::bigint>>12)::bit(64)::bigint;$$
                LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;
            """
        }

        for resolution, sql_create_function in function_creation_sql.items():
            sql_check_function = f"""
            SELECT EXISTS (
                SELECT 1
                FROM pg_proc
                WHERE proname = 'to_short_h3_{resolution}'
                  AND pronamespace = 'basic'::regnamespace
            );
            """

            function_exists = self.db.select(sql_check_function)[0][0]

            if not function_exists:
                print(f"Creating basic.to_short_h3_{resolution} function...")
                self.db.perform(sql_create_function)
                print(f"basic.to_short_h3_{resolution} function created successfully.")
            else:
                print(f"basic.to_short_h3_{resolution} function already exists.")

        print("H3 functions initialization completed.")

    @timing
    def generate_h3_grid_table_for_germany(self, resolution, replace=False):
        """
        Generate H3 grid for the buffered Germany geometry using a dynamic resolution,
        and insert it into the database if replace is True or if table doesn't exist.
        """

        # Ensure that the H3 functions exist
        self.ensure_h3_db_functions_exist()

        table_name = f"h3_{resolution}_grid"

        # Check if the table already exists
        if not self.table_exists(table_name) or replace:
            print(f"Generating H3 grid for resolution {resolution}...")

            sql_generate_grid = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                h3_index TEXT PRIMARY KEY,
                h3_short BIGINT,
                h3_boundary GEOMETRY(LINESTRING, 4326),
                geom GEOMETRY(POLYGON, 4326)
            );

            WITH border_points AS
            (
                SELECT ((ST_DUMPPOINTS(geom)).geom)::point AS geom
                FROM (SELECT ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 10000), 4326) AS geom
                      FROM germany_border) AS buffered_geom
            ),
            polygons AS
            (
                SELECT ((ST_DUMP(geom)).geom)::polygon AS geom
                FROM (SELECT ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 10000), 4326) AS geom
                      FROM germany_border) AS buffered_geom
            ),
            h3_ids AS
            (
                SELECT h3_lat_lng_to_cell(b.geom, {resolution}) h3_index
                FROM border_points b
                UNION ALL
                SELECT h3_polygon_to_cells(p.geom, ARRAY[]::polygon[], {resolution}) h3_index
                FROM polygons p
            )
            INSERT INTO {table_name} (h3_index, h3_short, h3_boundary, geom)
            SELECT sub.h3_index::text,
                   basic.to_short_h3_{resolution}(sub.h3_index::bigint) AS h3_short,
                   ST_ExteriorRing(ST_SetSRID(geometry(h3_cell_to_boundary(sub.h3_index)), 4326)) AS h3_boundary,
                   ST_SetSRID(geometry(h3_cell_to_boundary(sub.h3_index)), 4326) AS geom
            FROM h3_ids sub
            GROUP BY sub.h3_index;
            """
            self.db.perform(sql_generate_grid)
            print(f"H3 grid generated and inserted into {table_name}.")

            self.db.perform(f"CREATE INDEX ON {table_name} USING GIST (geom);")
            print(f"GIST index created for {table_name}.")

        else:
            print(f"Table {table_name} already exists. Skipping H3 grid generation.")

    @timing
    def generate_h3_additional_data_table(self, resolution, replace=False, batch_size=100000):
        """
        Combined process to update H3 table with Zensus, Regiostar, and Landuse data in one go,
        prioritizing specific objart values for landuse using keyset pagination.
        """
        table_name = f"h3_{resolution}_grid"
        result_table = f"h3_{resolution}_additional_data"

        # Step 1: Check if the additional data table exists
        if not self.table_exists(result_table) or replace:
            print(f"Creating and adding additional data to {result_table}...")

            sql_create_result_table = f"""
                DROP TABLE IF EXISTS {result_table};
                CREATE TABLE {result_table} (
                    h3_index TEXT PRIMARY KEY,
                    population INT DEFAULT 0,
                    rent_sqm NUMERIC(10, 2) DEFAULT 0.0,
                    regiostar_17 INT DEFAULT NULL,
                    objart INT DEFAULT NULL,
                    objart_txt TEXT DEFAULT NULL,
                    average_household_size FLOAT8 DEFAULT NULL,
                    average_age FLOAT8 DEFAULT NULL,
                    ownership_rate FLOAT8 DEFAULT NULL,
                    geom GEOMETRY(Polygon, 4326)
                );
            """
            self.db.perform(sql_create_result_table)
            print(f"Table {result_table} created successfully.")

            total_rows = self.db.select(f"SELECT COUNT(*) FROM {table_name}")[0][0]
            total_batches = (total_rows // batch_size) + 1
            last_h3_index = ''
            batch_num = 0

            while True:
                batch_start_time = time.time()

                print(f"{result_table}: Processing batch {batch_num + 1} out of {total_batches}...")

                sql_insert_batch = f"""
                    WITH cte AS (
                        SELECT h3_index, geom
                        FROM {table_name}
                        WHERE h3_index > '{last_h3_index}'
                        ORDER BY h3_index
                        LIMIT {batch_size}
                    ),
                    zensus_stats AS (
                        SELECT cte.h3_index,
                            SUM(COALESCE(zensus."Einwohner"::int, 0)) AS total_population,
                            ROUND(AVG(zensus.durchschnmieteqm::numeric) FILTER (WHERE zensus.durchschnmieteqm IS NOT NULL), 2) AS average_rent,
                            ROUND(AVG(zensus.durchschnhhgroesse::numeric) FILTER (WHERE zensus.durchschnhhgroesse IS NOT NULL), 2) AS average_household_size,
                            ROUND(AVG(zensus.durchschnittsalter::numeric) FILTER (WHERE zensus.durchschnittsalter IS NOT NULL), 2) AS average_age,
                            ROUND(AVG(zensus.eigentuemerquote::numeric) FILTER (WHERE zensus.eigentuemerquote IS NOT NULL), 2) AS ownership_rate
                        FROM cte
                        LEFT JOIN temporal.zensus2022_bevoelkerungszahl_100m_gitter AS zensus
                        ON zensus.h3_index_{resolution} = cte.h3_index  -- Use H3 index join instead of ST_Intersects
                        GROUP BY cte.h3_index
                    ),
                    regiostar_data AS (
                        SELECT cte.h3_index, regiostar_2022.regiostar_17
                        FROM cte
                        LEFT JOIN regiostar_2022
                        ON ST_Intersects(ST_Centroid(cte.geom), regiostar_2022.geom)
                    ),
                    landuse_data AS (
                        SELECT DISTINCT ON (cte.h3_index)
                            cte.h3_index,
                            landuse_atkis.objart::INTEGER AS objart,
                            landuse_atkis.objart_txt
                        FROM cte
                        LEFT JOIN landuse_atkis
                        ON ST_Intersects(ST_Centroid(cte.geom), landuse_atkis.geom)
                        ORDER BY cte.h3_index
                    )
                    INSERT INTO {result_table} (h3_index, population, rent_sqm, regiostar_17, objart, objart_txt, average_household_size, average_age, ownership_rate, geom)
                    SELECT cte.h3_index,
                        COALESCE(zensus_stats.total_population, 0),
                        zensus_stats.average_rent,
                        regiostar_data.regiostar_17,
                        landuse_data.objart,
                        landuse_data.objart_txt,
                        zensus_stats.average_household_size,
                        zensus_stats.average_age,
                        zensus_stats.ownership_rate,
                        cte.geom
                    FROM cte
                    LEFT JOIN zensus_stats ON cte.h3_index = zensus_stats.h3_index
                    LEFT JOIN regiostar_data ON cte.h3_index = regiostar_data.h3_index
                    LEFT JOIN landuse_data ON cte.h3_index = landuse_data.h3_index;
                """
                self.db.perform(sql_insert_batch)

                last_h3_index_result = self.db.select(f"SELECT MAX(h3_index) FROM {result_table}")[0][0]
                batch_num += 1

                batch_end_time = time.time()
                time_taken = batch_end_time - batch_start_time
                print(f"Batch {batch_num} of {total_batches} completed in {time_taken:.2f} seconds.")

                if last_h3_index_result is None or last_h3_index_result == last_h3_index:
                    break

                last_h3_index = last_h3_index_result

            self.db.perform(f"CREATE INDEX ON {result_table} USING GIST (geom);")
            print(f"Additional data added to {result_table}.")
        else:
            print(f"Table {result_table} already exists. Skipping additional data processing.")

    # @timing
    # def generate_h3_additional_data_table(self, resolution, replace=False, batch_size=10):
    #     """
    #     Combined process to update H3 table with Zensus, Regiostar, and Landuse data in one go,
    #     prioritizing specific objart values for landuse using keyset pagination.
    #     """
    #     table_name = f"h3_{resolution}_grid"
    #     result_table = f"h3_{resolution}_additional_data"

    #     # Step 1: Check if the additional data table exists
    #     if not self.table_exists(result_table) or replace:
    #         print(f"Creating and adding additional data to {result_table}...")

    #         sql_create_result_table = f"""
    #             DROP TABLE IF EXISTS {result_table};
    #             CREATE TABLE {result_table} (
    #                 h3_index TEXT PRIMARY KEY,
    #                 population INT DEFAULT 0,
    #                 rent_sqm NUMERIC(10, 2) DEFAULT 0.0,
    #                 regiostar_17 INT DEFAULT NULL,
    #                 objart INT DEFAULT NULL,
    #                 objart_txt TEXT DEFAULT NULL,
    #                 average_household_size FLOAT8 DEFAULT NULL,
    #                 average_age FLOAT8 DEFAULT NULL,
    #                 ownership_rate FLOAT8 DEFAULT NULL,
    #                 geom GEOMETRY(Polygon, 4326)
    #             );
    #         """
    #         self.db.perform(sql_create_result_table)
    #         print(f"Table {result_table} created successfully.")

    #         total_rows = self.db.select(f"SELECT COUNT(*) FROM {table_name}")[0][0]
    #         total_batches = (total_rows // batch_size) + 1
    #         last_h3_index = ''
    #         batch_num = 0

    #         while True:
    #             batch_start_time = time.time()

    #             print(f"{result_table}: Processing batch {batch_num + 1} out of {total_batches}...")

    #             sql_insert_batch = f"""
    #                 WITH cte AS (
    #                     SELECT h3_index, geom
    #                     FROM {table_name}
    #                     WHERE h3_index > '{last_h3_index}'
    #                     ORDER BY h3_index
    #                     LIMIT {batch_size}
    #                 ),
    #                 zensus_stats AS (
    #                     SELECT cte.h3_index,
    #                         SUM(COALESCE(zensus."Einwohner"::int, 0)) AS total_population,
    #                         ROUND(AVG(zensus.durchschnmieteqm::numeric), 2) AS average_rent,
    #                         ROUND(AVG(zensus.durchschnhhgroesse::numeric), 2) AS average_household_size,
    #                         ROUND(AVG(zensus.durchschnittsalter::numeric), 2) AS average_age,
    #                         ROUND(AVG(zensus.eigentuemerquote::numeric), 2) AS ownership_rate
    #                     FROM cte
    #                     LEFT JOIN temporal.zensus2022_bevoelkerungszahl_100m_gitter AS zensus
    #                     ON ST_Intersects(ST_Centroid(zensus.geom), cte.geom)
    #                     GROUP BY cte.h3_index
    #                 ),
    #                 regiostar_data AS (
    #                     SELECT cte.h3_index, regiostar_2022.regiostar_17
    #                     FROM cte
    #                     LEFT JOIN regiostar_2022
    #                     ON ST_Intersects(ST_Centroid(cte.geom), regiostar_2022.geom)
    #                 ),
    #                 landuse_data AS (
    #                     SELECT DISTINCT ON (cte.h3_index)
    #                         cte.h3_index,
    #                         landuse_atkis.objart::INTEGER AS objart,
    #                         landuse_atkis.objart_txt
    #                     FROM cte
    #                     LEFT JOIN landuse_atkis
    #                     ON ST_Intersects(ST_Centroid(cte.geom), landuse_atkis.geom)
    #                     ORDER BY cte.h3_index
    #                 )
    #                 INSERT INTO {result_table} (h3_index, population, rent_sqm, regiostar_17, objart, objart_txt, average_household_size, average_age, ownership_rate, geom)
    #                 SELECT cte.h3_index,
    #                     COALESCE(zensus_stats.total_population, 0),
    #                     zensus_stats.average_rent,
    #                     regiostar_data.regiostar_17,
    #                     landuse_data.objart,
    #                     landuse_data.objart_txt,
    #                     zensus_stats.average_household_size,
    #                     zensus_stats.average_age,
    #                     zensus_stats.ownership_rate,
    #                     cte.geom
    #                 FROM cte
    #                 LEFT JOIN zensus_stats ON cte.h3_index = zensus_stats.h3_index
    #                 LEFT JOIN regiostar_data ON cte.h3_index = regiostar_data.h3_index
    #                 LEFT JOIN landuse_data ON cte.h3_index = landuse_data.h3_index;
    #             """
    #             self.db.perform(sql_insert_batch)

    #             last_h3_index_result = self.db.select(f"SELECT MAX(h3_index) FROM {result_table}")[0][0]
    #             batch_num += 1

    #             batch_end_time = time.time()
    #             time_taken = batch_end_time - batch_start_time
    #             print(f"Batch {batch_num} of {total_batches} completed in {time_taken:.2f} seconds.")

    #             if last_h3_index_result is None or last_h3_index_result == last_h3_index:
    #                 break

    #             last_h3_index = last_h3_index_result

    #         self.db.perform(f"CREATE INDEX ON {result_table} USING GIST (geom);")
    #         print(f"Additional data added to {result_table}.")
    #     else:
    #         print(f"Table {result_table} already exists. Skipping additional data processing.")

    @timing
    def combine_additional_data_and_heatmaps(self, poi_category, replace=False):
        """
        Combines additional data and heatmaps for a given point of interest (POI) category.
        """
        for mode, resolution in [("bicycle", 9), ("walking", 10)]:
            result_table = f"{poi_category}_{mode}"

            if not self.table_exists(result_table) or replace:
                print(f"Combining additional data and heatmaps for {poi_category} ({mode})...")

                combine_sql = f"""
                DROP TABLE IF EXISTS {result_table};
                CREATE TABLE {result_table} AS
                WITH ranked_data AS (
                    SELECT
                        had.*,
                        hbcap.accessibility AS accessibility_closest_average,
                        hbccgp.accessibility AS accessibility_combined_gaussian,
                        hbgp.accessibility AS accessibility_gaussian,
                        ROW_NUMBER() OVER (PARTITION BY had.h3_index ORDER BY had.h3_index) AS rn
                    FROM
                        h3_{resolution}_additional_data had
                    LEFT JOIN
                        heatmap_{mode}_closest_average_{poi_category} hbcap
                        ON had.h3_index = hbcap.h3_index
                    LEFT JOIN
                        heatmap_{mode}_combined_gaussian_{poi_category} hbccgp
                        ON had.h3_index = hbccgp.h3_index
                    LEFT JOIN
                        heatmap_{mode}_gaussian_{poi_category} hbgp
                        ON had.h3_index = hbgp.h3_index
                    WHERE
                        (had.population > 0
                        OR had.objart IN (41001, 41002, 41006, 41007))
                )
                SELECT * FROM ranked_data
                WHERE rn = 1;

                ALTER TABLE {result_table} DROP COLUMN rn;

                CREATE INDEX ON {result_table} USING GIST(geom);
                ALTER TABLE {result_table} ADD CONSTRAINT {result_table}_pkey PRIMARY KEY (h3_index);
                """
                self.db.perform(combine_sql)
                print(f"Table {result_table} created and combined with heatmap data for {poi_category} ({mode}).")
            else:
                print(f"Table {result_table} already exists. Skipping combination for {poi_category} ({mode}).")

    # @timing
    # def combine_data_and_heatmaps_by_mode_long(self, mode, replace=False):
    #     """
    #     Combines data and heatmaps for each mode into a long table format.
    #     Introduces a serial id and excludes the geom column.
    #     """
    #     combined_table = f"{mode}_long"

    #     if not self.table_exists(combined_table) or replace:
    #         print(f"Combining data and heatmaps for mode: {mode}...")

    #         # Fetch columns and their data types from one of the tables
    #         sample_table = f"{list(poi_dicts.keys())[0]}_{mode}"
    #         columns_sql = f"""
    #         SELECT column_name, data_type
    #         FROM information_schema.columns
    #         WHERE table_name = '{sample_table}'
    #         AND column_name NOT IN ('geom', 'population', 'rent_sqm', 'objart', 'objart_txt')
    #         """
    #         columns_result = self.db.select(columns_sql)

    #         # Adjust the data types for optimization
    #         columns = []
    #         column_names = []
    #         for row in columns_result:
    #             column_name = row[0]
    #             data_type = row[1]

    #             # Optimize data types: SMALLINT for limited integers, FLOAT4 for lower precision
    #             if column_name == 'regiostar_17':
    #                 data_type = 'SMALLINT'  # Smaller than INTEGER
    #             elif 'accessibility' in column_name:
    #                 data_type = 'FLOAT4'  # Lower precision float for memory optimization

    #             columns.append(f"{column_name} {data_type}")
    #             column_names.append(column_name)

    #         # Create the combined table with a serial id, optimized types, and excluding geom
    #         create_table_sql = f"""
    #         DROP TABLE IF EXISTS {combined_table};
    #         CREATE TABLE {combined_table} (
    #             id SERIAL PRIMARY KEY,
    #             {', '.join(columns)}
    #         );
    #         """
    #         self.db.perform(create_table_sql)

    #         # Insert data from all POI categories for this mode
    #         for poi_category in poi_dicts:
    #             insert_data_sql = f"""
    #             INSERT INTO {combined_table} ({', '.join(column_names)})
    #             SELECT {', '.join(column_names)}
    #             FROM {poi_category}_{mode};
    #             """
    #             self.db.perform(insert_data_sql)
    #             print(f"Inserted data for {poi_category} in mode: {mode}")

    #         print(f"Data successfully combined into {combined_table}.")

    #     else:
    #         print(f"Table {combined_table} already exists. Skipping combination.")

    @timing
    def combine_data_and_heatmaps_by_mode_long(self, mode, poi_categories=None, poi_sensitivity_category=None, replace=False):
        """
        Combines data and heatmaps for each mode into a long table format.
        Introduces a serial id and excludes the geom column.
        """
        # Determine the combined table name based on poi_sensitivity_category
        if poi_sensitivity_category:
            combined_table = f"{mode}_{poi_sensitivity_category}_long"
        else:
            combined_table = f"{mode}_long"

        if not self.table_exists(combined_table) or replace:
            if poi_sensitivity_category:
                print(f"Combining data and heatmaps for mode: {mode} and POI sensitivity category: {poi_sensitivity_category}...")
            else:
                print(f"Combining data and heatmaps for mode: {mode}...")

            # Fetch columns and their data types from one of the tables
            sample_table = f"{list(poi_dicts.keys())[0]}_{mode}"
            columns_sql = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{sample_table}'
            AND column_name NOT IN ('geom', 'population', 'rent_sqm', 'objart', 'objart_txt', 'average_household_size', 'average_age', 'ownership_rate')
            """
            columns_result = self.db.select(columns_sql)

            # Adjust the data types for optimization
            columns = []
            column_names = []
            for row in columns_result:
                column_name = row[0]
                data_type = row[1]

                # Optimize data types: SMALLINT for limited integers, FLOAT4 for lower precision
                if column_name == 'regiostar_17':
                    data_type = 'SMALLINT'  # Smaller than INTEGER
                elif 'accessibility' in column_name:
                    data_type = 'FLOAT4'  # Lower precision float for memory optimization

                columns.append(f"{column_name} {data_type}")
                column_names.append(column_name)

            # Create the combined table with a serial id, optimized types, and excluding geom
            create_table_sql = f"""
            DROP TABLE IF EXISTS {combined_table};
            CREATE TABLE {combined_table} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)}
            );
            """
            self.db.perform(create_table_sql)

            # Determine the POI categories to use
            if poi_categories is None:
                poi_categories = poi_dicts.keys()

            # Insert data from all POI categories for this mode
            for poi_category in poi_categories:
                insert_data_sql = f"""
                INSERT INTO {combined_table} ({', '.join(column_names)})
                SELECT {', '.join(column_names)}
                FROM {poi_category}_{mode};
                """
                self.db.perform(insert_data_sql)
                if poi_sensitivity_category:
                    print(f"Inserted data for {poi_category} in mode: {mode} and POI sensitivity category: {poi_sensitivity_category}")
                else:
                    print(f"Inserted data for {poi_category} in mode: {mode}")

            if poi_sensitivity_category:
                print(f"Data successfully combined into {combined_table} for mode: {mode} and POI sensitivity category: {poi_sensitivity_category}.")
            else:
                print(f"Data successfully combined into {combined_table} for mode: {mode}.")

        else:
            if poi_sensitivity_category:
                print(f"Table {combined_table} for mode: {mode} and POI sensitivity category: {poi_sensitivity_category} already exists. Skipping combination.")
            else:
                print(f"Table {combined_table} for mode: {mode} already exists. Skipping combination.")

    ##### work in progress in case ever needed
    # @timing
    # def combine_data_and_heatmaps_by_mode_wide(self, mode, replace=False):
    #     """
    #     Combines data and heatmaps for each mode into a wide table format.
    #     Keeps geom and changes accessibility column names to include poi_category.
    #     Uses outer join to ensure all h3_index values are included.
    #     """
    #     combined_table = f"{mode}_wide"

    #     if not self.table_exists(combined_table) or replace:
    #         print(f"Combining data and heatmaps for mode: {mode} into wide format...")

    #         # Fetch columns from one of the tables (including geom for wide format)
    #         sample_table = f"{list(poi_dicts.keys())[0]}_{mode}"
    #         columns_sql = f"""
    #         SELECT column_name, data_type
    #         FROM information_schema.columns
    #         WHERE table_name = '{sample_table}'
    #         AND column_name NOT IN ('geom', 'accessibility_closest_average', 'accessibility_combined_gaussian', 'accessibility_gaussian')
    #         """
    #         columns_result = self.db.select(columns_sql)
    #         columns = [f"{row[0]} {row[1]}" for row in columns_result]  # Fetching columns and types
    #         column_names = [row[0] for row in columns_result]  # Only the column names for later queries

    #         # Add geom and h3_index to columns for table creation
    #         columns = [f"h3_index TEXT PRIMARY KEY", "geom GEOMETRY(Polygon, 4326)"] + columns

    #         # Create the wide combined table with the necessary columns, including geom
    #         create_table_sql = f"""
    #         DROP TABLE IF EXISTS {combined_table};
    #         CREATE TABLE {combined_table} (
    #             {', '.join(columns)}
    #         );
    #         """
    #         self.db.perform(create_table_sql)

    #         # Insert the initial data for h3_index, geom, and other columns from the first poi_category
    #         first_poi_category = list(poi_dicts.keys())[0]
    #         insert_initial_sql = f"""
    #         INSERT INTO {combined_table} (h3_index, geom, {', '.join(column_names)})
    #         SELECT h3_index, geom, {', '.join(column_names)}
    #         FROM {first_poi_category}_{mode};
    #         """
    #         self.db.perform(insert_initial_sql)
    #         print(f"Initial data inserted for {first_poi_category} in wide format for mode: {mode}")

    #         # Loop through the remaining POI categories and perform outer joins
    #         for poi_category in poi_dicts:
    #             if poi_category != first_poi_category:  # Skip the first one as it's already inserted
    #                 # Dynamically add new columns for this poi_category
    #                 add_columns_sql = f"""
    #                 ALTER TABLE {combined_table}
    #                 ADD COLUMN {poi_category}_accessibility_closest_average NUMERIC,
    #                 ADD COLUMN {poi_category}_accessibility_combined_gaussian NUMERIC,
    #                 ADD COLUMN {poi_category}_accessibility_gaussian NUMERIC;
    #                 """
    #                 self.db.perform(add_columns_sql)

    #                 # Perform an outer join to include this poi_category data
    #                 outer_join_sql = f"""
    #                 UPDATE {combined_table}
    #                 SET
    #                     {poi_category}_accessibility_closest_average = src.accessibility_closest_average,
    #                     {poi_category}_accessibility_combined_gaussian = src.accessibility_combined_gaussian,
    #                     {poi_category}_accessibility_gaussian = src.accessibility_gaussian
    #                 FROM (
    #                     SELECT h3_index, accessibility_closest_average, accessibility_combined_gaussian, accessibility_gaussian
    #                     FROM {poi_category}_{mode}
    #                 ) AS src
    #                 WHERE {combined_table}.h3_index = src.h3_index;
    #                 """
    #                 self.db.perform(outer_join_sql)
    #                 print(f"Outer joined data for {poi_category} in wide format for mode: {mode}")

    #         print(f"Data successfully combined into wide format table: {combined_table}.")
    #     else:
    #         print(f"Table {combined_table} already exists. Skipping combination.")

if __name__ == "__main__":
    try:
        analysis = Statistical_Analysis(db=Database(settings.LOCAL_DATABASE_URI), db_rd=Database(settings.RAW_DATABASE_URI))

        # Process H3 grid for resolution 9
        analysis.generate_h3_grid_table_for_germany(resolution=9, replace=False)

        # Process H3 grid for resolution 10
        analysis.generate_h3_grid_table_for_germany(resolution=10, replace=False)

        # Process additional data for resolution 9
        analysis.generate_h3_additional_data_table(resolution=9, replace=False) #

        # Process additional data for resolution 10
        analysis.generate_h3_additional_data_table(resolution=10, replace=True) #

        # combine heatmaps and additional data for each poi_category -> loop over POI categories from confing -> insert into new function
        for poi_category in poi_dicts:
            analysis.combine_additional_data_and_heatmaps(poi_category, replace=True) #

        # combine heatmaps and additonal data for each transport mode (walking, bicycle) -> long form without geom
        # introduce serial id
        for mode in ['walking', 'bicycle']:
            analysis.combine_data_and_heatmaps_by_mode_long(mode=mode, replace=False)

        ##### work in progress in case ever needed
        # combine heatmaps and additonal data for each transport mode (walking, bicycle) -> wide form with geom
        # keep h3_index as primary key -> outer join? change name of accessibility columns -> need to add poi_category to name
        # for mode in ['walking', 'bicycle']:
        #     analysis.combine_data_and_heatmaps_by_mode_wide(mode=mode, replace=False)
        #####

        # combine heatmaps and additional data for each POI-Sensitivity-Category -> long form without geom
        poi_sensitivity_categories = {
            "immediate": ["bus_stop_gtfs", "childcare", "grocery_store", "school_isced_level_1"],
            "close": ["pharmacy", "rail_station_gtfs"],
            "district_wide": ["general_practitioner", "restaurant"],
            "citywide": ["museum", "population"]
        }

        for poi_sensitivity_category, poi_categories in poi_sensitivity_categories.items():
            for mode in ['walking', 'bicycle']:
                analysis.combine_data_and_heatmaps_by_mode_long(mode=mode, poi_categories=poi_categories, poi_sensitivity_category=poi_sensitivity_category, replace=False)

        # muss ich meine daten cleanen?

        # exploratory data analysis
        # df.info()
        # df.describe()
        # df.null().sum()
        # df.nunique()
        # different plots: scatter, bar, box, hist, heatmap, pairplot, correlation matrix


    except Exception as e:
        print(f"An error occurred: {e}")
