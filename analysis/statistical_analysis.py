import json
import time

import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist
from scipy.stats import skew

from analysis.master_thesis_philipp_config import poi_dicts
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import timing


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

    def fetch_data_in_chunks(self, table_name, chunksize=100000):
        """Fetch data from the database in chunks to avoid memory overload."""
        full_table_name = f"{table_name}"
        sql_query = f"SELECT * FROM {full_table_name}"
        try:
            chunk_iter = pd.read_sql(sql_query, self.db.return_sqlalchemy_engine(), chunksize=chunksize)
            df_list = []
            for chunk in chunk_iter:
                optimized_chunk = self.optimize_dataframe(chunk)
                df_list.append(optimized_chunk)
            df = pd.concat(df_list, ignore_index=True)  # Combine all chunks into a single DataFrame
            return df
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error


    def optimize_dataframe(self, df, columns_to_exclude=None):
        """Optimize the dataframe for memory efficiency and handle specific column types."""
        if columns_to_exclude is None:
            columns_to_exclude = []

        # Exclude unwanted columns upfront
        df = df.drop(columns=columns_to_exclude, errors='ignore')

        # Convert specific columns to 'category' type if they exist
        if 'regiostar_17' in df.columns:
            df['regiostar_17'] = df['regiostar_17'].astype('category')
        if 'objart' in df.columns:
            df['objart'] = df['objart'].astype('category')

        # Optimize other object columns by converting them to 'category'
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype('category')

        # Apply rounding and conversion to specific float columns
        for col in df.select_dtypes(include=['float']).columns:
            if 'closest_average' in col.lower():
                df[col].round(0).astype(pd.Int64Dtype()) # Round and then convert to int            
            if 'gaussian' in col.lower():
                df[col] = pd.to_numeric(df[col], downcast='float')  # Downcast all other floats

        return df

    def generate_statistical_summary(self, poi_dicts, regiostar_levels, summary_table_name, replace=True):
        """
        Generate and store a comprehensive statistical summary for each POI category and specified RegioStaR levels in PostgreSQL.
        """
        # Check if the table exists
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{summary_table_name}'
            );
        """
        table_exists = self.db.select(table_exists_query)[0][0]

        if not replace and table_exists:
            print(f"Table {summary_table_name} already exists and replace is set to False. Skipping summary generation.")
            return

        # Step 1: Pre-create the summary table with the required schema
        create_table_sql = f"""
            DROP TABLE IF EXISTS {summary_table_name};
            CREATE TABLE {summary_table_name} (
                id SERIAL PRIMARY KEY,
                poi_category TEXT,
                transport_mode TEXT,
                regiostar_type TEXT,
                regiostar_subtype TEXT,
                spatial_accessibility_indicator TEXT,
                count BIGINT,
                mean NUMERIC,
                std_dev NUMERIC,
                mad NUMERIC,
                min NUMERIC,
                max NUMERIC,
                range NUMERIC,
                q25 NUMERIC,
                q50 NUMERIC,
                q75 NUMERIC,
                null_count BIGINT,
                null_percentage NUMERIC,
                unique_count BIGINT,
                skewness NUMERIC,
                kurtosis NUMERIC,
                outlier_count BIGINT,
                quintiles NUMERIC[]
            );
        """
        self.db.perform(create_table_sql)
        print(f"Table {summary_table_name} created successfully.")

        # Step 2: Loop over each POI category and RegioStaR level
        for poi_category in poi_dicts:

            for transport_mode in ['walking', 'bicycle']:
                table_name = f"{poi_category}_{transport_mode}"  # Adjust table naming convention as needed

                # Fetch and optimize data
                df = self.fetch_data_in_chunks(table_name)
                df = self.optimize_dataframe(df)

                # RegioStaR mappings
                regiostar_mappings = {
                    '2': {
                        111: "1 - Stadtregion", 112: "1 - Stadtregion", 113: "1 - Stadtregion", 114: "1 - Stadtregion", 115: "1 - Stadtregion",
                        121: "1 - Stadtregion", 123: "1 - Stadtregion", 124: "1 - Stadtregion", 125: "1 - Stadtregion",
                        211: "2 - Ländliche Region", 213: "2 - Ländliche Region", 214: "2 - Ländliche Region", 215: "2 - Ländliche Region",
                        221: "2 - Ländliche Region", 223: "2 - Ländliche Region", 224: "2 - Ländliche Region", 225: "2 - Ländliche Region"
                        },
                    '4': {
                        111: "11 - Metropolitane Stadtregion", 112: "11 - Metropolitane Stadtregion", 113: "11 - Metropolitane Stadtregion",
                        114: "11 - Metropolitane Stadtregion", 115: "11 - Metropolitane Stadtregion", 121: "12 - Regiopolitane Stadtregion",
                        123: "12 - Regiopolitane Stadtregion", 124: "12 - Regiopolitane Stadtregion", 125: "12 - Regiopolitane Stadtregion",
                        211: "21 - Stadtregionsnahe ländliche Region", 213: "21 - Stadtregionsnahe ländliche Region",
                        214: "21 - Stadtregionsnahe ländliche Region", 215: "21 - Stadtregionsnahe ländliche Region",
                        221: "22 - Periphere ländliche Region", 223: "22 - Periphere ländliche Region",
                        224: "22 - Periphere ländliche Region", 225: "22 - Periphere ländliche Region"
                        },
                    '7': {
                        111: "71 - Metropolen", 112: "72 - Regiopolen und Großstädte", 113: "73 - Mittelstädte und städtischer Raum",
                        114: "73 - Mittelstädte und städtischer Raum", 115: "74 - Kleinstädtischer Raum einer Stadtregion",
                        121: "72 - Regiopolen und Großstädte", 123: "73 - Mittelstädte und städtischer Raum",
                        124: "73 - Mittelstädte und städtischer Raum", 125: "74 - Kleinstädtischer Raum einer Stadtregion",
                        211: "75 - Zentrale Städte einer Ländlichen Region", 213: "76 - Mittelstädte, städtischer Raum",
                        214: "76 - Mittelstädte, städtischer Raum", 215: "77 - Kleinstädtischer Raum einer Ländlichen Region",
                        221: "75 - Zentrale Städte einer Ländlichen Region", 223: "76 - Mittelstädte, städtischer Raum",
                        224: "76 - Mittelstädte, städtischer Raum", 225: "77 - Kleinstädtischer Raum einer Ländlichen Region"
                        },
                    '17': {
                        111: "111 - Metropole", 112: "112 - Großstadt einer Metropolitanen Stadtregion", 
                        113: "113 - Mittelstadt einer Metropolitanen Stadtregion", 114: "114 - Städtischer Raum einer Metropolitanen Stadtregion",
                        115: "115 - Kleinstädtischer, dörflicher Raum einer Metropolitanen Stadtregion", 121: "121 - Regiopole", 
                        123: "123 - Mittelstadt einer Regiopolitanen Stadtregion", 124: "124 - Städtischer Raum einer Regiopolitanen Stadtregion", 
                        125: "125 - Kleinstädtischer, dörflicher Raum einer Regiopolitanen Stadtregion", 211: "211 - Zentrale Stadt einer Stadtregionsnahen ländlichen Region",
                        213: "213 - Mittelstadt einer Stadtregionsnahen ländlichen Region", 214: "214 - Städtischer Raum einer Stadtregionsnahen ländlichen Region", 
                        215: "215 - Kleinstädtischer, dörflicher Raum einer Stadtregionsnahen ländlichen Region", 221: "221 - Zentrale Stadt einer Peripheren ländlichen Region", 
                        223: "223 - Mittelstadt einer Peripheren ländlichen Region", 224: "224 - Städtischer Raum einer Peripheren ländlichen Region", 
                        225: "225 - Kleinstädtischer, dörflicher Raum einer Peripheren ländlichen Region"
                        },
                    'Gem7': {
                        111: "1 - Metropole", 112: "3 - Großstadt", 113: "5 - Mittelstadt", 114: "6 - Städtischer Raum", 115: "7 - Kleinstädtischer/dörflicher Raum",
                        121: "2 - Regiopole", 123: "5 - Mittelstadt", 124: "6 - Städtischer Raum", 125: "7 - Kleinstädtischer/dörflicher Raum",
                        211: "4 - Zentrale Stadt", 213: "5 - Mittelstadt", 214: "6 - Städtischer Raum", 215: "7 - Kleinstädtischer/dörflicher Raum",
                        221: "4 - Zentrale Stadt", 223: "5 - Mittelstadt", 224: "6 - Städtischer Raum", 225: "7 - Kleinstädtischer/dörflicher Raum"
                        },
                    'Gem5': {
                        111: "1 - Metropole", 112: "2 - Regiopole/Großstadt", 113: "3 - Zentrale Stadt/Mittelstadt",
                        114: "4 - Städtischer Raum", 115: "5 - Kleinstädtischer/dörflicher Raum", 121: "2 - Regiopole/Großstadt",
                        123: "3 - Zentrale Stadt/Mittelstadt", 124: "4 - Städtischer Raum", 125: "5 - Kleinstädtischer/dörflicher Raum",
                        211: "3 - Zentrale Stadt/Mittelstadt", 213: "3 - Zentrale Stadt/Mittelstadt", 214: "4 - Städtischer Raum",
                        215: "5 - Kleinstädtischer/dörflicher Raum", 221: "3 - Zentrale Stadt/Mittelstadt", 223: "3 - Zentrale Stadt/Mittelstadt",
                        224: "4 - Städtischer Raum", 225: "5 - Kleinstädtischer/dörflicher Raum"
                        }
                    }

                for level in regiostar_levels:
                    if level in regiostar_mappings:
                        df[f'regiostar_{level}_group'] = df['regiostar_17'].map(regiostar_mappings[level])

                        for column in ['accessibility_closest_average', 'accessibility_combined_gaussian', 'accessibility_gaussian']:
                            if column in df.columns:
                                df_level = df[[f'regiostar_{level}_group', column]]

                                for regiostar_subtype, group_data in df_level.groupby(f'regiostar_{level}_group'):
                                    # Entfernen Sie `dropna()` hier, um sicherzustellen, dass `NULL`-Werte nicht entfernt werden
                                    group_data = group_data.dropna(subset=[column])

                                    q25, q50, q75 = group_data[column].quantile([0.25, 0.5, 0.75])
                                    iqr = q75 - q25
                                    median = group_data[column].median()
                                    mad = np.median(np.abs(group_data[column] - median))

                                    # Filter out zero values for quintile calculation
                                    non_zero_data = group_data[column][group_data[column] > 0]

                                    # Calculate quintiles array
                                    if not non_zero_data.empty:
                                        quintiles = [0] + list(np.percentile(non_zero_data, [20, 40, 60, 80, 100]))
                                    else:
                                        quintiles = [0] * 6

                                    stats = {
                                        'count': int(group_data[column].count()),
                                        'mean': float(group_data[column].mean()),
                                        'std_dev': float(group_data[column].std()),
                                        'mad': float(mad),
                                        'min': float(group_data[column].min()),
                                        'max': float(group_data[column].max()),
                                        'range': float(group_data[column].max() - group_data[column].min()),
                                        'q25': float(q25),
                                        'q50': float(q50),
                                        'q75': float(q75),
                                        'null_count': int(group_data[column].isnull().sum()),
                                        'null_percentage': float(group_data[column].isnull().mean() * 100),
                                        'unique_count': int(group_data[column].nunique()),
                                        'skewness': float(group_data[column].skew()),
                                        'kurtosis': float(group_data[column].kurt()),
                                        'outlier_count': int(group_data[(group_data[column] < (q25 - 1.5 * iqr)) | (group_data[column] > (q75 + 1.5 * iqr))][column].count()),
                                        'quintiles': quintiles
                                    }

                                    # Insert statistics into summary table
                                    insert_sql = f"""
                                        INSERT INTO {summary_table_name} (
                                            poi_category, transport_mode, regiostar_type, regiostar_subtype, spatial_accessibility_indicator, count, mean, std_dev, mad, min, max, range, q25, q50, q75, null_count, null_percentage, unique_count, skewness, kurtosis, outlier_count, quintiles
                                        ) VALUES (
                                            '{poi_category}', '{transport_mode}', '{level}', '{regiostar_subtype}', '{column}', {stats['count']}, {stats['mean']}, {stats['std_dev']}, {stats['mad']}, {stats['min']}, {stats['max']}, {stats['range']}, {stats['q25']}, {stats['q50']}, {stats['q75']}, {stats['null_count']}, {stats['null_percentage']}, {stats['unique_count']}, {stats['skewness']}, {stats['kurtosis']}, {stats['outlier_count']}, ARRAY{stats['quintiles']}
                                        )
                                    """
                                    self.db.perform(insert_sql)

                        print(f"Statistical summary for {poi_category} and {transport_mode} at level {level} inserted into {summary_table_name}.")
        print("Statistical summary generation completed.")

    def assign_similarity_groups(self, summary_table_name):
        """
        Task 1 (Steps 1–4): Identify similar subtypes and assign group_id to subtype groups.
        Task 2 (Steps 5–6): Identify similar POIs and assign group_id to POI groups.
        """

        # Step 1: Ensure the similar_group column exists, add it if missing
        check_column_sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{summary_table_name}' AND column_name = 'similar_group';
        """
        column_check = self.db.select(check_column_sql)
        if not column_check:
            print(f"Column `similar_group` not found in {summary_table_name}. Adding column...")
            add_column_sql = f"ALTER TABLE {summary_table_name} ADD COLUMN similar_group TEXT[];"
            self.db.perform(add_column_sql)
            print("Column `similar_group` added successfully.")

        # Step 2: Clear existing values in the similar_group column
        clear_column_sql = f"UPDATE {summary_table_name} SET similar_group = NULL;"
        self.db.perform(clear_column_sql)
        print("Cleared existing values in `similar_group` column.")

        # Step 3: Fetch unique combinations of RegioStaR type, mode, and indicator
        fetch_combinations_sql = f"""
            SELECT DISTINCT regiostar_type, transport_mode, spatial_accessibility_indicator
            FROM {summary_table_name};
        """
        unique_combinations = self.db.select(fetch_combinations_sql)

        if not unique_combinations:
            print(f"No data found in {summary_table_name}.")
            return

        unique_combinations = [
            {'regiostar_type': row[0], 'mode': row[1], 'spatial_accessibility_indicator': row[2]}
            for row in unique_combinations
        ]

        grouped_table_values = []
        group_id = 1

        # Step 4: Identify Similar Subtypes (Task 1)
        for combination in unique_combinations:
            regiostar_type = combination['regiostar_type']
            mode = combination['mode']
            indicator = combination['spatial_accessibility_indicator']
            print(f"Processing RegioStaR: {regiostar_type}, Mode: {mode}, Indicator: {indicator}...")

            # Fetch data for the current combination
            fetch_data_sql = f"""
                SELECT id, poi_category, regiostar_subtype, quintiles
                FROM {summary_table_name}
                WHERE regiostar_type = '{regiostar_type}'
                AND transport_mode = '{mode}'
                AND spatial_accessibility_indicator = '{indicator}';
            """
            data = pd.read_sql(fetch_data_sql, self.db.return_sqlalchemy_engine())

            if data.empty:
                print(f"No data found for RegioStaR {regiostar_type}, Mode {mode}, and indicator {indicator}.")
                continue

            # Parse quintiles
            data['quintiles'] = data['quintiles'].apply(lambda q: np.array(q, dtype=float))

            # Compute pairwise distance matrix for the group
            distance_matrix = pdist(data['quintiles'].tolist(), metric='euclidean')

            # # Calculate threshold once per group
            # threshold = np.percentile(distance_matrix, 25)
            # print(f"Calculated threshold for RegioStaR {regiostar_type}, Mode {mode}, Indicator {indicator}: {threshold}")

            # Berechne die Skewness
            skewness = skew(distance_matrix)
            print(f"Skewness for RegioStaR {regiostar_type}, Mode {mode}, Indicator {indicator}: {skewness}")

            if abs(skewness) < 0.5:
                # Symmetrische Verteilung: Nutze Mittelwert-Ansatz
                mean_distance = np.mean(distance_matrix)
                std_distance = np.std(distance_matrix)
                threshold = mean_distance - 0.5 * std_distance
                method = "Mean - 0.5*Std"
            else:
                # Asymmetrische Verteilung: Nutze Perzentil-Ansatz
                threshold = np.percentile(distance_matrix, 25)
                method = "25th Percentile"

            print(f"Calculated threshold for RegioStaR {regiostar_type}, Mode {mode}, Indicator {indicator} ({method}): {threshold}")


            # Group subtypes based on the threshold
            subtype_matched = set()
            for i, row_i in data.iterrows():
                if row_i['poi_category'] in subtype_matched:
                    continue

                current_group = [row_i['regiostar_subtype']]
                current_quintiles = [row_i['quintiles']]
                original_rows = [row_i.to_dict()]

                for j, row_j in data.iterrows():
                    if row_j['poi_category'] in subtype_matched or row_i['poi_category'] == row_j['poi_category']:
                        continue

                    # Compare subtype quintiles using global threshold
                    distance = np.linalg.norm(row_i['quintiles'] - row_j['quintiles'])
                    if distance <= threshold:
                        current_group.append(row_j['regiostar_subtype'])
                        current_quintiles.append(row_j['quintiles'])
                        original_rows.append(row_j.to_dict())
                        subtype_matched.add(row_j['poi_category'])

                if len(current_group) > 1:
                    grouped_table_values.append({
                        'group_id': group_id,
                        'regiostar_type': regiostar_type,
                        'mode': mode,
                        'indicator': indicator,
                        'subtypes': current_group,
                        'group_quintiles': np.mean(current_quintiles, axis=0).tolist(),
                        'original_rows': original_rows
                    })
                    group_id += 1



        # Step 5: Create and Insert Grouped Table
        # Create a new table with all necessary columns
        new_table_name = f"{summary_table_name}_grouped"
        create_table_sql = f"""
            DROP TABLE IF EXISTS {new_table_name};
            CREATE TABLE {new_table_name} (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL,
                poi_category TEXT NOT NULL,
                regiostar_type TEXT NOT NULL,
                regiostar_subtype TEXT NOT NULL,
                mode TEXT NOT NULL,
                spatial_accessibility_indicator TEXT NOT NULL,
                original_quintiles DOUBLE PRECISION[],
                group_quintiles DOUBLE PRECISION[]
            );
        """
        self.db.perform(create_table_sql)
        print(f"Table `{new_table_name}` created.")


        for group in grouped_table_values:
            for original_row in group['original_rows']:
                # Escape single quotes for PostgreSQL compatibility
                poi_category = original_row['poi_category'].replace("'", "''")

                # Format arrays as PostgreSQL-compatible strings
                original_quintiles_array = "'{" + ", ".join(map(str, original_row['quintiles'])) + "}'"
                group_quintiles_array = "'{" + ", ".join(map(str, group['group_quintiles'])) + "}'"

                # Insert each row into the grouped table
                insert_sql = f"""
                    INSERT INTO {new_table_name} (
                        group_id, poi_category, regiostar_type, regiostar_subtype,
                        mode, spatial_accessibility_indicator, original_quintiles, group_quintiles
                    ) VALUES (
                        {group['group_id']}, '{poi_category}', 
                        '{group['regiostar_type']}', '{original_row['regiostar_subtype']}',
                        '{group['mode']}', '{group['indicator']}', 
                        {original_quintiles_array}, {group_quintiles_array}
                    );
                """
                self.db.perform(insert_sql)


        print(f"Inserted all rows into `{new_table_name}`.")

        # Task 2: Identify Similar POI Categories Within Each Subtype
        poi_groups = []
        poi_group_id = group_id  # Continue group IDs from where Task 1 left off

        for combination in unique_combinations:
            regiostar_type = combination['regiostar_type']
            mode = combination['mode']
            indicator = combination['spatial_accessibility_indicator']

            # Fetch data for the current combination
            fetch_data_sql = f"""
                SELECT id, poi_category, regiostar_subtype, quintiles
                FROM {summary_table_name}
                WHERE regiostar_type = '{regiostar_type}'
                AND transport_mode = '{mode}'
                AND spatial_accessibility_indicator = '{indicator}';
            """
            data = pd.read_sql(fetch_data_sql, self.db.return_sqlalchemy_engine())

            if data.empty:
                continue

            # Parse quintiles
            data['quintiles'] = data['quintiles'].apply(lambda q: np.array(q, dtype=float))

            # Group by RegioStaR Subtype
            for subtype in data['regiostar_subtype'].unique():
                subtype_data = data[data['regiostar_subtype'] == subtype]
                poi_matched = set()

                for i, row_i in subtype_data.iterrows():
                    if row_i['poi_category'] in poi_matched:
                        continue

                    current_group = [row_i['poi_category']]
                    current_quintiles = [row_i['quintiles']]

                    for j, row_j in subtype_data.iterrows():
                        if row_j['poi_category'] in poi_matched or row_i['poi_category'] == row_j['poi_category']:
                            continue

                        # Compare POI quintiles within the same subtype
                        distance = np.linalg.norm(row_i['quintiles'] - row_j['quintiles'])

                        # Dynamic threshold calculation for each pair
                        distance_matrix = pdist(np.vstack([row_i['quintiles'], row_j['quintiles']]), metric='euclidean')
                        skewness = skew(distance_matrix)
                        if abs(skewness) < 0.5:
                            mean_distance = np.mean(distance_matrix)
                            std_distance = np.std(distance_matrix)
                            threshold = mean_distance - 0.5 * std_distance
                            method = "Mean - 0.5*Std"
                        else:
                            threshold = np.percentile(distance_matrix, 25)
                            method = "25th Percentile"

                        print(f"Threshold ({method}): {threshold}")

                        if distance <= threshold:
                            current_group.append(row_j['poi_category'])
                            current_quintiles.append(row_j['quintiles'])
                            poi_matched.add(row_j['poi_category'])

                    if len(current_group) > 1:
                        poi_groups.append({
                            'group_id': poi_group_id,
                            'regiostar_type': regiostar_type,
                            'mode': mode,
                            'indicator': indicator,
                            'subtype': subtype,
                            'pois': current_group,
                            'group_quintiles': np.mean(current_quintiles, axis=0).tolist()
                        })
                        poi_group_id += 1

        # Print Final Refined Groups (POIs)
        print("\nFinal Refined Groups (POIs Within Each Subtype):")
        for group in poi_groups:
            print(
                f"Group {group['group_id']}: RegioStaR: {group['regiostar_type']}, Mode: {group['mode']}, "
                f"Indicator: {group['indicator']}, Subtype: {group['subtype']}, "
                f"POIs: {group['pois']}, Group Quintiles: {group['group_quintiles']}"
            )

        # Task 3: Identify POI Categories Similar Across All Subcategories
        poi_groups = []
        poi_group_id = group_id  # Start group IDs where the previous tasks left off

        for combination in unique_combinations:
            regiostar_type = combination['regiostar_type']
            mode = combination['mode']
            indicator = combination['spatial_accessibility_indicator']

            # Fetch data for the current combination
            fetch_data_sql = f"""
                SELECT poi_category, regiostar_subtype, quintiles
                FROM {summary_table_name}
                WHERE regiostar_type = '{regiostar_type}'
                AND transport_mode = '{mode}'
                AND spatial_accessibility_indicator = '{indicator}';
            """
            data = pd.read_sql(fetch_data_sql, self.db.return_sqlalchemy_engine())

            if data.empty:
                continue

            # Parse quintiles into NumPy arrays
            data['quintiles'] = data['quintiles'].apply(lambda q: np.array(q, dtype=float))

            # Prepare a dictionary to store quintiles grouped by POI category
            poi_quintiles = {
                poi_category: poi_data['quintiles'].tolist()
                for poi_category, poi_data in data.groupby('poi_category')
            }

            # Compare POI categories for similarity
            poi_list = list(poi_quintiles.keys())
            for i, poi_1 in enumerate(poi_list):
                for poi_2 in poi_list[i + 1:]:
                    quintiles_1 = poi_quintiles[poi_1]
                    quintiles_2 = poi_quintiles[poi_2]

                    # Ensure both POIs have data for all subtypes
                    if len(quintiles_1) < 2 or len(quintiles_2) < 2:
                        continue

                    # Compute pairwise distances between their quintiles
                    combined_quintiles = quintiles_1 + quintiles_2
                    distance_matrix = pdist(combined_quintiles, metric='euclidean')

                    # Define a dynamic threshold
                    threshold = np.percentile(distance_matrix, 25)
                    if np.std(distance_matrix) > 0.0:  # Adjust based on skewness if applicable
                        skewness = skew(distance_matrix)
                        if abs(skewness) < 0.5:
                            threshold = np.mean(distance_matrix) - 0.5 * np.std(distance_matrix)

                    # Check if all distances between the two POIs are below the threshold
                    all_similar = np.all(distance_matrix <= threshold)

                    if all_similar:
                        # Check if the group already exists
                        existing_group = None
                        for group in poi_groups:
                            if poi_1 in group['poi_categories'] or poi_2 in group['poi_categories']:
                                existing_group = group
                                break

                        if existing_group:
                            # Add to existing group
                            existing_group['poi_categories'].add(poi_1)
                            existing_group['poi_categories'].add(poi_2)
                        else:
                            # Create a new group
                            poi_groups.append({
                                'group_id': poi_group_id,
                                'regiostar_type': regiostar_type,
                                'mode': mode,
                                'indicator': indicator,
                                'poi_categories': {poi_1, poi_2},
                            })
                            poi_group_id += 1

        # Print Final Groups
        print("\nFinal Groups of POIs with Similar Quintiles Across Subtypes:")
        for group in poi_groups:
            print(f"Group {group['group_id']}:")
            print(f"  RegioStaR: {group['regiostar_type']}")
            print(f"  Mode: {group['mode']}")
            print(f"  Indicator: {group['indicator']}")
            print(f"  POI Categories: {', '.join(group['poi_categories'])}")






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
        analysis.generate_h3_additional_data_table(resolution=10, replace=False) #

        # combine heatmaps and additional data for each poi_category -> loop over POI categories from confing -> insert into new function
        for poi_category in poi_dicts:
            analysis.combine_additional_data_and_heatmaps(poi_category, replace=False) #
        print("For every POI category, the additional data and heatmaps have been combined.")

        regiostar_levels = ['2','7','17'] # ['2', '4', '7', 'Gem7', 'Gem5', '17']
        summary_table_name = "poi_statistical_summary"

        analysis.generate_statistical_summary(poi_dicts, regiostar_levels, summary_table_name, replace = False)

        analysis.assign_similarity_groups(summary_table_name)


        # # combine heatmaps and additonal data for each transport mode (walking, bicycle) -> long form without geom
        # # introduce serial id
        # for mode in ['walking', 'bicycle']:
        #     analysis.combine_data_and_heatmaps_by_mode_long(mode=mode, replace=False)

        ##### work in progress in case ever needed
        # combine heatmaps and additonal data for each transport mode (walking, bicycle) -> wide form with geom
        # keep h3_index as primary key -> outer join? change name of accessibility columns -> need to add poi_category to name
        # for mode in ['walking', 'bicycle']:
        #     analysis.combine_data_and_heatmaps_by_mode_wide(mode=mode, replace=False)
        #####

        #### probably not needed anymore
        # combine heatmaps and additional data for each POI-Sensitivity-Category -> long form without geom
        # poi_sensitivity_categories = {
        #     "immediate": ["bus_stop_gtfs", "childcare", "grocery_store", "school_isced_level_1"],
        #     "close": ["pharmacy", "rail_station_gtfs"],
        #     "district_wide": ["general_practitioner", "restaurant"],
        #     "citywide": ["museum", "population"]
        # }

        # for poi_sensitivity_category, poi_categories in poi_sensitivity_categories.items():
        #     for mode in ['walking', 'bicycle']:
        #         analysis.combine_data_and_heatmaps_by_mode_long(mode=mode, poi_categories=poi_categories, poi_sensitivity_category=poi_sensitivity_category, replace=False)
        #####

        # muss ich meine daten cleanen?

        # create statistical table

        # df.info()
        # df.describe()
        # df.null().sum()
        # df.nunique()
        # different plots: scatter, bar, box, hist, heatmap, pairplot, correlation matrix


    except Exception as e:
        print(f"An error occurred: {e}")
