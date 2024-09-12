import os
import re
import time
import requests
import geopandas as gpd
from shapely import wkb
import pandas as pd
import numpy as np
from collections import OrderedDict
import zipfile
import subprocess
import shutil

from src.core.config import settings
from src.db.db import Database
from src.utils.utils import create_table_dump, restore_table_dump, timing
from analysis.master_thesis_philipp_config import poi_dicts


class GoatAPIClient:
    def __init__(self, db: Database, db_rd: Database):
        self.db = db
        self.db_rd = db_rd

    def make_request(self, method, url, **kwargs):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = headers

        response = requests.request(method, url, **kwargs)

        return response

    def delete_folder(self, folder_id):
        url = f'http://goat_core:8000/api/v2/folder/{folder_id}'
        response = self.make_request('DELETE', url, headers={'accept': '*/*'})
        if response.status_code == 204:
            print(f"Folder {folder_id} deleted successfully.")
        else:
            raise Exception(f"Failed to delete folder: {response.status_code} - {response.text}")

    def create_folder(self, folder_name, replace=False):
        search_url = f'http://goat_core:8000/api/v2/folder?search={folder_name}&order_by=created_at&order=descendent'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        search_response = self.make_request('GET', search_url, headers=headers)

        if search_response.status_code == 200:
            folders = search_response.json()
            if folders:
                existing_folder = folders[0]
                print(f"Folder '{folder_name}' already exists.")
                if replace:
                    self.delete_folder(existing_folder["id"])
                    print("Replacing the existing folder.")
                    # After deletion, proceed to create a new folder
                else:
                    return (False, existing_folder)  # Indicate no new folder was created
            else:
                print(f"No existing folder named '{folder_name}' found. Creating new folder.")
        else:
            raise Exception(f'Failed to search for folder: {search_response.status_code} - {search_response.text}')

        create_url = 'http://goat_core:8000/api/v2/folder'
        create_response = self.make_request('POST', create_url, headers=headers, json={'name': folder_name})

        if create_response.status_code == 201:
            print(create_response.json())
            return (True, create_response.json())  # Indicate a new folder was created
        else:
            raise Exception(f'Folder creation failed: {create_response.status_code} - {create_response.text}')

    def create_project_folder(self, payload, replace=False):
        # Ensure metadata table exists
        self.ensure_metadata_table_exists()

        folder_name = payload["name"]
        was_created, folder = self.create_folder(folder_name, replace)

        if was_created or replace:
            sql_add_comment_to_metadata_table = f"""
                COMMENT ON TABLE master_thesis_metadata IS 'project_folder_id:{folder['id']}';
            """
            self.db.perform(sql_add_comment_to_metadata_table)

        return folder

    def create_project(self, payload, replace=False):
        # Ensure metadata table exists
        self.ensure_metadata_table_exists()

        # Get the project folder ID from the metadata table comment
        sql_get_comment = "SELECT obj_description('master_thesis_metadata'::regclass);"
        comment = self.db.select(sql_get_comment)
        folder_id = re.search('project_folder_id:([^,]*)', comment[0][0]).group(1)

        # Update the payload with the correct folder ID
        payload['folder_id'] = folder_id

        project_name = payload["name"]
        search_url = f'http://goat_core:8000/api/v2/project?search={project_name}&order_by=created_at&order=descendent'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        search_response = self.make_request('GET', search_url, headers=headers)

        if search_response.status_code == 200:
            response_data = search_response.json()
            projects = response_data['items']
            if projects:
                existing_project = projects[0]
                print(f"Project '{project_name}' already exists.")
                if replace:
                    self.delete_project(existing_project["id"])
                    print("Replacing the existing project.")
                else:
                    return existing_project  # Return the existing project details
            else:
                print(f"No existing project named '{project_name}' found. Creating new project.")
        else:
            raise Exception(f'Failed to search for project: {search_response.status_code} - {search_response.text}')

        create_url = 'http://goat_core:8000/api/v2/project'
        create_response = self.make_request('POST', create_url, headers=headers, json=payload)

        if create_response.status_code == 201:
            print(create_response.json())

            sql_update_comment = f"""
                COMMENT ON TABLE master_thesis_metadata IS 'project_id:{create_response.json()['id']}, project_folder_id:{folder_id}';
            """
            self.db.perform(sql_update_comment)

            return create_response.json()
        else:
            raise Exception(f'Project creation failed: {create_response.status_code} - {create_response.text}')

    def delete_project(self, project_id):
        url = f'http://goat_core:8000/api/v2/project/{project_id}'
        response = self.make_request('DELETE', url, headers={'accept': '*/*'})
        if response.status_code == 204:
            print(f"Project {project_id} deleted successfully.")
        else:
            raise Exception(f"Failed to delete project: {response.status_code} - {response.text}")

    def delete_layer(self, layer_id: str):
        url = f'http://goat_core:8000/api/v2/layer/{layer_id}'
        headers = {
            'accept': '*/*'
        }

        response = self.make_request('DELETE', url, headers=headers)
        if response.status_code == 204:
            print(f"Layer with ID {layer_id} deleted successfully.")
        else:
            print(f"Failed to delete layer: {response.status_code} - {response.text}")

    def get_layer_ids_of_folder(self, folder_id):
        base_url = 'http://goat_core:8000/api/v2/layer'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        data = {
            "folder_id": folder_id,
            "order_by": "created_at",
            "order": "descendent"
        }
        page = 1
        size = 50
        all_layer_ids = []

        while True:
            # Construct the URL with query parameters for pagination
            url = f"{base_url}?order_by=created_at&order=descendent&page={page}&size={size}"

            response = self.make_request('POST', url, headers=headers, json=data)

            if response.status_code == 200:
                response_data = response.json()
                layer_ids = [item['id'] for item in response_data['items']]
                all_layer_ids.extend(layer_ids)

                if len(response_data['items']) < size:
                    break

                page += 1
            else:
                raise Exception(f"Failed to retrieve layers: {response.status_code} - {response.text}")

        return all_layer_ids

    def ensure_metadata_table_exists(self):
        sql_create_metadata_table = """
            CREATE TABLE IF NOT EXISTS master_thesis_metadata (
                layer_id TEXT PRIMARY KEY,
                reference_clipped_area TEXT NOT NULL,
                poi_category TEXT NOT NULL,
                layer_project_id TEXT,
                heatmap_layer_id TEXT
            )
        """
        self.db.perform(sql_create_metadata_table)
        print("Ensured that metadata table exists.")

    def check_if_file_in_metadata_table(self, reference_clipped_area, poi_category):
        sql_check_file_in_metadata_table = f"""
            SELECT * FROM master_thesis_metadata
            WHERE reference_clipped_area = '{reference_clipped_area}' AND poi_category = '{poi_category}';
        """
        result = self.db.select(sql_check_file_in_metadata_table)
        if result:
            return True
        else:
            return False

    def delete_file_from_metadata_table(self, reference_clipped_area: str, poi_category: str):
        sql_delete_file_from_metadata_table = f"""
            DELETE FROM master_thesis_metadata
            WHERE reference_clipped_area = '{reference_clipped_area}' AND poi_category = '{poi_category}';
        """
        self.db.perform(sql_delete_file_from_metadata_table)
        print(f"Deleted metadata for reference_clipped_area {reference_clipped_area} and poi_category {poi_category}.")

    def validate_poi_category(self, poi_category):
        if poi_category in ['population', 'grocery_store', 'childcare']:
            return True

        sql_validate_poi_category = f"SELECT * FROM kart_pois.poi_categories WHERE category = '{poi_category}'"
        result = self.db.select(sql_validate_poi_category)
        if result:
            return True
        else:
            raise ValueError(f"Invalid POI category: {poi_category}")

    def get_poi_table_name(self, poi_category):
        try:
            # Normalize poi_category by removing known suffixes
            original_category = poi_category
            if poi_category.endswith('_osm') or poi_category.endswith('_gtfs'):
                poi_category = poi_category.rsplit('_', 1)[0]

            # find correct poi table name within kart
            sql_kart_poi_table_name = f"""
                SELECT table_name
                FROM kart_pois.poi_categories
                WHERE category = '{poi_category}';
            """
            table_name = self.db.select(sql_kart_poi_table_name)[0][0]

            # Append the original suffix back to the table name
            if original_category != poi_category:
                table_name += original_category[len(poi_category):]

            return table_name
        except IndexError:
            print(f"No table name found for category '{original_category}'")
            return None
        except Exception as e:
            print(f"An error occurred during getting POI table name: {e}")
            return None

    def get_poi_query_components(self, poi_category):
        if poi_category == 'population':
            from_clause = "temporal.zensus2022_bevoelkerungszahl_100m_gitter_4326_centroids"
            where_clause = ""
        elif poi_category in ['nursery', 'kindergarten', 'school_isced_level_1']:
            from_clause = f"poi.{self.get_poi_table_name(poi_category)}"
            where_clause = f"{poi_category} = True"
        elif poi_category == 'grocery_store':
            from_clause = "poi.poi_shopping"
            where_clause = "category in ('supermarket', 'discount_supermarket', 'hypermarket')"
        elif poi_category == 'childcare':
            from_clause = "poi.poi_childcare"
            where_clause = "after_school IS False"
        elif poi_category.endswith('_osm') or poi_category.endswith('_gtfs'):
            from_clause = f"poi.{self.get_poi_table_name(poi_category)}"
            where_clause = f"category = '{poi_category.rsplit('_', 1)[0]}'"  # Use the suffix as a value for the source column
        else:
            from_clause = f"poi.{self.get_poi_table_name(poi_category)}"
            where_clause = f"category = '{poi_category}'"

        return from_clause, where_clause

    # loop over the geoms of grid table, buffer grid cells (10km), intersect with POI table and store resulting poi file in project folder
    def clip_and_upload_pois(self, poi_category, replace=False):
        """
        Clips and uploads points of interest (POIs) for a given category.

        Args:
        poi_category (str): The category of the POIs to be clipped and uploaded.
        replace (bool): Flag to indicate whether to replace existing data. Default is False.
        """
        # Normalize poi_category by removing known suffixes
        original_category = poi_category
        if poi_category.endswith('_osm') or poi_category.endswith('_gtfs'):
            poi_category = poi_category.rsplit('_', 1)[0]

        self.validate_poi_category(poi_category)
        poi_category = original_category

        # Determine the table based on poi_category
        # if poi_category == 'population':
        #     table_name = "germany_grid_25km_4326"
        # else:
        #     table_name = "germany_grid_50km_4326"
        table_name = "germany_grid_50km_4326"

        # Construct the SQL query using the determined table_name
        sql_select_id_geom = f"""
                    SELECT id, geom
                    FROM temporal.{table_name}
                """

        cur = self.db_rd.conn.cursor()

        try:
            cur.execute(sql_select_id_geom)
            grids = cur.fetchall()
        except Exception as e:
            print(f"An error occurred while fetching grid data: {e}")
            self.db_rd.conn.rollback()
            cur.close()
            return

        for grid_id, grid_geom in grids:
            layer_name = f"{poi_category}_{grid_id}"
            layer_id = self.get_layer_id_by_name(name=layer_name)
            file_in_metadata = self.check_if_file_in_metadata_table(reference_clipped_area=grid_id, poi_category=poi_category)

            # Skip processing if layer already exists and replace is False
            if layer_id and file_in_metadata and not replace:
                print(f"{layer_name} already in GOAT and replace is False, skipping...")
                continue

            try:
                # Prepare POI query components
                from_clause, where_clause = self.get_poi_query_components(poi_category)

                sql_create_poi_upload_table = f"""
                    DROP TABLE IF EXISTS poi_upload;
                    CREATE TEMP TABLE poi_upload AS (
                        SELECT *
                        FROM {from_clause}
                        WHERE 1=0
                    );
                """
                cur.execute(sql_create_poi_upload_table)

                # Get column names
                if poi_category == 'population':
                    table_name = 'zensus2022_bevoelkerungszahl_100m_gitter_4326_centroids'
                    schema = 'temporal'
                elif poi_category == 'grocery_store':
                    table_name = 'poi_shopping'
                    schema = 'poi'
                elif poi_category == 'childcare':
                    table_name = 'poi_childcare'
                    schema = 'poi'
                else:
                    table_name = self.get_poi_table_name(poi_category)
                    schema = 'poi'

                sql_get_columns = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{schema}' AND table_name = '{table_name}';"
                cur.execute(sql_get_columns)
                columns = cur.fetchall()
                column_names = ', '.join([column[0] for column in columns])

                sql_clip_poi_data = f"""
                    INSERT INTO poi_upload({column_names})
                    WITH region AS (
                        SELECT ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_GeomFromText(ST_AsText('{grid_geom}')), 4326), 3857), 10000), 4326) AS geom
                    )
                    SELECT DISTINCT ON (p.id) p.*
                    FROM {from_clause} p
                    JOIN region r ON ST_Intersects(p.geom, r.geom)
                    {'WHERE ' + where_clause if where_clause else ''}
                """.strip()
                cur.execute(sql_clip_poi_data)
                self.db_rd.conn.commit()

                # Fetch data into a DataFrame
                cur.execute("SELECT * FROM poi_upload;")
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                dataframe = pd.DataFrame(rows, columns=columns)

                if dataframe.empty:
                    print(f"No data for {poi_category}_{grid_id}, skipping...")
                    continue

                # Process DataFrame
                dataframe['geom'] = dataframe['geom'].apply(wkb.loads)
                geodataframe = gpd.GeoDataFrame(dataframe, geometry='geom', crs='EPSG:4326')

                # Convert list columns to strings
                for col in geodataframe.columns:
                    if not geodataframe[col].empty and isinstance(geodataframe[col].iloc[0], list):
                        geodataframe[col] = geodataframe[col].apply(str)

                if 'other_categories' in geodataframe.columns:
                    geodataframe['other_categories'] = geodataframe['other_categories'].apply(str)

                geodataframe = geodataframe.replace({None: np.nan, 'None': np.nan})

                # Export GeoDataFrame to a GeoPackage
                output_path = f"/app/src/data/output/{poi_category}_{grid_id}.gpkg"
                geodataframe.to_file(output_path, driver="GPKG")

                # Handle deletion of similar layers immediately after obtaining layer_id
                if isinstance(layer_id, list):
                    for similar_layer_id in layer_id:
                        self.delete_layer(similar_layer_id)
                        print(f"Deleted similar layer {similar_layer_id}.")
                    layer_id = None  # Reset layer_id since similar layers have been deleted

                # Handle existing layers and metadata
                if layer_id and file_in_metadata:
                    self.delete_layer(layer_id)
                    self.delete_file_from_metadata_table(reference_clipped_area=grid_id, poi_category=poi_category)
                elif layer_id:
                    self.delete_layer(layer_id)
                elif file_in_metadata:
                    self.delete_file_from_metadata_table(reference_clipped_area=grid_id, poi_category=poi_category)

                # Upload the GeoPackage to the server
                self.upload_file(output_path, grid_id, poi_category)

            except Exception as e:
                print(f"An error occurred while clipping and uploding POIs: {e}")
                self.db_rd.conn.rollback()

        cur.close()

    def upload_file(self, file_path, grid_id, poi_category, max_retries=3):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"File path is invalid or file does not exist: {file_path}")

        url = 'http://goat_core:8000/api/v2/layer/file-upload'
        headers = {
            'accept': 'application/json'
        }
        sql_get_comment = "SELECT obj_description('master_thesis_metadata'::regclass);"
        comment = self.db.select(sql_get_comment)
        folder_id_match = re.search('project_folder_id:([^,]*)', comment[0][0])
        if folder_id_match:
            folder_id = folder_id_match.group(1)
        else:
            raise ValueError("Could not find project_folder_id in comment")

        for _ in range(max_retries):
            with open(file_path, 'rb') as file:
                files = {'file': file}
                response = self.make_request('POST', url, headers=headers, files=files)

            if response.status_code == 201:
                print(f"File {file_path} uploaded successfully to folder {folder_id}.")
                name = os.path.splitext(os.path.basename(file_path))[0]
                dataset_id = response.json()['dataset_id']
                self.process_file(folder_id, name, dataset_id)
                layer_id = self.get_layer_id_by_name(name = poi_category + '_' + str(grid_id))

                sql_insert_file_metadata =f"""
                    INSERT INTO master_thesis_metadata (layer_id, reference_clipped_area, poi_category)
                    VALUES ('{layer_id}', '{grid_id}', '{poi_category}');
                """
                self.db.perform(sql_insert_file_metadata)
                break  # Break the loop if upload is successful

            elif response.status_code == 422:
                print(f"Failed to upload file, retrying: {response.status_code} - {response.text}")
            else:
                raise Exception(f"Failed to upload file: {response.status_code} - {response.text}")

    def process_file(self, folder_id, name, dataset_id):
        url = 'http://goat_core:8000/api/v2/layer/internal'
        headers = {
            'accept': 'application/json'
        }
        data = {
            'folder_id': folder_id,
            'name': name,
            'dataset_id': dataset_id
        }
        response = self.make_request('POST', url, headers=headers, json=data)

        if response.status_code == 201:
            # wait until job is completed
            while self.check_job_status(response.json()['job_id'])['status_simple'] != 'finished':
                time.sleep(2)
            print(f"File with dataset_id {dataset_id} processed successfully.")
        else:
            raise Exception(f"Failed to process file: {response.status_code} - {response.text}")

    def get_layer_id_by_name(self, name: str):
        page = 1
        similar_layers = []
        pattern = re.compile(re.escape(name) + r' \([0-9]+\)$')

        while True:
            url = f'http://goat_core:8000/api/v2/layer?order_by=created_at&order=descendent&page={page}&size=50'
            headers = {'accept': 'application/json'}
            data = {"search": name}

            response = self.make_request('POST', url, headers=headers, json=data)
            if response.status_code == 200:
                response_data = response.json()
                layers = response_data.get("items", [])
                for layer in layers:
                    layer_name_lower = layer['name'].lower()
                    if layer_name_lower == name.lower():
                        return layer['id']  # Return exact match
                    elif pattern.search(layer_name_lower):  # Use regex pattern to find similar names
                        similar_layers.append(layer['name'])  # Collect similar names

                if not layers or len(layers) < 50:
                    break  # Exit loop if no more pages or less than 50 items in the last page
                page += 1  # Increment page number to fetch next page
            else:
                print(f"Failed to get layer: {response.status_code} - {response.text}")
                break  # Exit loop on request failure

        return similar_layers if similar_layers else None  # Return similar names list or None

    def add_poi_layers_to_project(self, poi_category, replace=False):

        project_id = self.get_project_id()

        if replace:
            sql_get_layer_ids_of_poi_category_to_add = f"""
                SELECT layer_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}';
            """
            layer_ids_to_add = self.db.select(sql_get_layer_ids_of_poi_category_to_add)
            layer_ids_to_add = [layer_id[0] for layer_id in layer_ids_to_add]
            print(f"Layers to add: {layer_ids_to_add}")

            sql_get_layer_project_ids_of_poi_category_to_delete = f"""
                SELECT layer_project_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL;
            """
            layer_project_ids_to_delete = self.db.select(sql_get_layer_project_ids_of_poi_category_to_delete)
            layer_project_ids_to_delete = [project_layer_id[0] for project_layer_id in layer_project_ids_to_delete]
            print(f"Project layers to delete: {layer_project_ids_to_delete}")

            # Delete existing layers from the project
            self.delete_layers_from_project(project_id, layer_project_ids_to_delete)
        else:
            sql_get_layer_ids_of_poi_category_to_add = f"""
                SELECT layer_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NULL;
            """
            layer_ids_to_add = self.db.select(sql_get_layer_ids_of_poi_category_to_add)
            layer_ids_to_add = [layer_id[0] for layer_id in layer_ids_to_add]
            print(f"Layers to add: {layer_ids_to_add}")

        # If layer_ids_to_add is empty, skip the rest
        if not layer_ids_to_add:
            print("No layers to add, skipping...")
            return

        # Add new layers to the project
        self.add_layers_to_project(project_id, layer_ids_to_add)

        print(f"Layers added to project {project_id} successfully.")
        return project_id

    def delete_layers_from_project(self, project_id, layer_project_ids):
        for layer_project_id in layer_project_ids:
            delete_url = f'http://goat_core:8000/api/v2/project/{project_id}/layer'
            delete_params = f'layer_project_id={layer_project_id}'
            delete_full_url = f"{delete_url}?{delete_params}"
            print(f"Deleting layer with project ID {layer_project_id} from project {project_id}")
            delete_response = self.make_request('DELETE', delete_full_url, headers={'accept': '*/*'})

            if delete_response.status_code != 204:
                raise Exception(f"Failed to delete layer {layer_project_id} from project: {delete_response.status_code} - {delete_response.text}")

    def get_layer_project_ids(self, project_id):
        url = f'http://goat_core:8000/api/v2/project/{project_id}/layer'
        response = self.make_request('GET', url, headers={'accept': 'application/json'})

        if response.status_code == 200:
            layers = response.json()
            layer_project_ids = [layer['id'] for layer in layers]
            return layer_project_ids
        else:
            raise Exception(f'Failed to get layers: {response.status_code} - {response.text}')

    def add_layers_to_project(self, project_id, layer_ids):
        url = f'http://goat_core:8000/api/v2/project/{project_id}/layer'
        params = '&'.join([f'layer_ids={layer_id}' for layer_id in layer_ids])
        full_url = f"{url}?{params}"
        print(f"Adding layers to project {project_id} with URL: {full_url}")

        response = self.make_request('POST', full_url, headers={'accept': 'application/json'}, data='')

        if response.status_code == 200:
            layers = response.json()
            layers_dict = {layer['layer_id']: layer['id'] for layer in layers}
            print(f"Layer project IDs received: {layers_dict}")

            for layer_id, layer_project_id in layers_dict.items():
                sql_update_layer_project_id = f"""
                    UPDATE master_thesis_metadata
                    SET layer_project_id = '{layer_project_id}'
                    WHERE layer_id = '{layer_id}';
                """
                self.db.perform(sql_update_layer_project_id)
                print(f"Added layer project id {layer_id} to metadata")
        else:
            raise Exception(f"Failed to add layers to project: {response.status_code} - {response.text}")

    def check_job_status(self, job_id):
        url = f'http://goat_core:8000/api/v2/job/{job_id}'
        response = self.make_request('GET', url, headers={'accept': 'application/json'})
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get job status: {response.status_code} - {response.text}")

    def create_heatmap_gravity(self, project_id, layer_project_id, heatmap_attributes):
        url = f'http://goat_core:8000/api/v2/active-mobility/heatmap-gravity?project_id={project_id}'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        for opportunity in heatmap_attributes["opportunities"]:
            opportunity["opportunity_layer_project_id"] = int(layer_project_id)

        payload = OrderedDict()
        payload["impedance_function"] = heatmap_attributes["impedance_function"]
        payload["opportunities"] = heatmap_attributes["opportunities"]
        payload["routing_type"] = heatmap_attributes["routing_type"]

        response = self.make_request('POST', url, headers=headers, json=payload)

        if response.status_code == 201:
            job_id = response.json()['job_id']
            print(f"Heatmap gravity request successful. Job ID: {job_id} and layer project ID: {layer_project_id}")

            # wait until job is completed
            while True:
                status = self.check_job_status(job_id)['status_simple']
                print("Heatmap job status:", status)
                if status == 'finished':
                    heatmap_layer_id = self.check_job_status(job_id)['layer_ids'][0]
                    sql_update_layer_project_id = f"""
                        UPDATE master_thesis_metadata
                        SET heatmap_layer_id = '{heatmap_layer_id}'
                        WHERE layer_project_id = '{layer_project_id}';
                    """
                    self.db.perform(sql_update_layer_project_id)
                    print(f"Added heatmap layer id {heatmap_layer_id} to metadata")
                    print("Heatmap job completed successfully.")
                    break
                elif status == 'failed':
                    error = self.check_job_status(job_id)['msg_simple']
                    sql_update_layer_id = f"""
                        UPDATE master_thesis_metadata
                        SET heatmap_layer_id = '{error}'
                        WHERE layer_project_id = '{layer_project_id}';
                    """
                    # self.db.perform(sql_update_layer_id)
                    print(f"Added heatmap creation error {error} to metadata")
                    print("Heatmap job failed.")
                    break
                else: #TODO should be status == 'pending' and else something went wrong
                    print("Heatmap job still in progress...")
                    time.sleep(20)  # Wait for 10 seconds before checking again
            return job_id
        else:
            raise Exception(f"Heatmap gravity request failed: {response.status_code} - {response.text}")

    def create_heatmap_closest_average(self, project_id, layer_project_id, heatmap_attributes):
        url = f'http://goat_core:8000/api/v2/active-mobility/heatmap-closest-average?project_id={project_id}'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        for opportunity in heatmap_attributes["opportunities"]:
            opportunity["opportunity_layer_project_id"] = int(layer_project_id)

        payload = OrderedDict()
        payload["opportunities"] = heatmap_attributes["opportunities"]
        payload["routing_type"] = heatmap_attributes["routing_type"]

        response = self.make_request('POST', url, headers=headers, json=payload)

        if response.status_code == 201:
            job_id = response.json()['job_id']
            print(f"Heatmap closest average request successful. Job ID: {job_id} and layer project ID: {layer_project_id}")

            # wait until job is completed
            while True:
                status = self.check_job_status(job_id)['status_simple']
                print("Heatmap job status:", status)
                if status == 'finished':
                    heatmap_layer_id = self.check_job_status(job_id)['layer_ids'][0]
                    sql_update_layer_project_id = f"""
                        UPDATE master_thesis_metadata
                        SET heatmap_layer_id = '{heatmap_layer_id}'
                        WHERE layer_project_id = '{layer_project_id}';
                    """
                    self.db.perform(sql_update_layer_project_id)
                    print(f"Added heatmap layer id {heatmap_layer_id} to metadata")
                    print("Heatmap job completed successfully.")
                    break
                elif status == 'failed':
                    error = self.check_job_status(job_id)['msg_simple']
                    sql_update_layer_id = f"""
                        UPDATE master_thesis_metadata
                        SET heatmap_layer_id = '{error}'
                        WHERE layer_project_id = '{layer_project_id}';
                    """
                    # self.db.perform(sql_update_layer_id)
                    print(f"Added heatmap creation error {error} to metadata")
                    print("Heatmap job failed.")
                    break
                else:  # should be status == 'pending' and else something went wrong
                    print("Heatmap job still in progress...")
                    time.sleep(20)  # Wait for 20 seconds before checking again
            return job_id
        else:
            raise Exception(f"Heatmap closest average request failed: {response.status_code} - {response.text}")

    @timing
    def calculate_poi_heatmaps(self, poi_category, heatmap_attributes, heatmap_description, replace=False):

        project_id = self.get_project_id()

        if replace:
            sql_get_layer_project_ids_of_poi_category_to_calculate_heatmap = f"""
                SELECT layer_project_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL;
            """
            layer_project_ids_to_calculate_heatmap = self.db.select(sql_get_layer_project_ids_of_poi_category_to_calculate_heatmap)
            layer_project_ids_to_calculate_heatmap = [layer_project_id[0] for layer_project_id in layer_project_ids_to_calculate_heatmap]
            print(f"Project layers to calculate heatmap: {layer_project_ids_to_calculate_heatmap}")

            sql_get_heatmap_layer_ids_of_poi_category_to_delete = f"""
                SELECT heatmap_layer_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL AND heatmap_layer_id IS NOT NULL;
            """
            heatmap_layer_ids_to_delete = self.db.select(sql_get_heatmap_layer_ids_of_poi_category_to_delete)
            heatmap_layer_ids_to_delete = [heatmap_project_layer_id[0] for heatmap_project_layer_id in heatmap_layer_ids_to_delete]
            print(f"Heatmap project layers to delete: {heatmap_layer_ids_to_delete}")

            # Delete existing layers from the project
            self.delete_layers_from_project(project_id, heatmap_layer_ids_to_delete)
        else:
            sql_get_layer_project_ids_of_poi_category_to_add = f"""
                SELECT layer_project_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL AND heatmap_layer_id IS NULL;
            """
            layer_project_ids_to_calculate_heatmap = self.db.select(sql_get_layer_project_ids_of_poi_category_to_add)
            layer_project_ids_to_calculate_heatmap = [layer_project_id[0] for layer_project_id in layer_project_ids_to_calculate_heatmap]
            print(f"Project layers to calculate heatmap: {layer_project_ids_to_calculate_heatmap}")

        # If layer_project_ids_to_calculate_heatmap is empty, skip the rest
        if not layer_project_ids_to_calculate_heatmap:
            print("No layers to add, skipping...")
            return

        # Add new layers to the project
        if 'gaussian' in heatmap_description or 'cumulative_gaussian' in heatmap_description:
            for counter, layer_project_id_to_calculate_heatmap in enumerate(layer_project_ids_to_calculate_heatmap, start=1):
                self.create_heatmap_gravity(project_id, layer_project_id_to_calculate_heatmap, heatmap_attributes)
                print(f"{heatmap_description} for poi {poi_category} have been calculated in project {project_id} successfully. Iteration {counter} of {len(layer_project_ids_to_calculate_heatmap)}")
        elif 'closest_average' in heatmap_description:
            for counter, layer_project_id_to_calculate_heatmap in enumerate(layer_project_ids_to_calculate_heatmap, start=1):
                self.create_heatmap_closest_average(project_id, layer_project_id_to_calculate_heatmap, heatmap_attributes)
                print(f"{heatmap_description} for poi {poi_category} have been calculated in project {project_id} successfully. Iteration {counter} of {len(layer_project_ids_to_calculate_heatmap)}")
        else:
            print(f"Invalid heatmap description: {heatmap_description}")

        return project_id

    def get_project_id(self):
        # Get the project ID from the metadata table comment
        sql_get_comment = "SELECT obj_description('master_thesis_metadata'::regclass);"
        comment = self.db.select(sql_get_comment)
        project_id = re.search('project_id:([^,]*)', comment[0][0]).group(1)
        print(f"Retrieved project ID: {project_id}")
        return project_id

    def download_and_unzip_layer(self, layer_id, file_name):

        url = f'http://goat_core:8000/api/v2/layer/internal/{layer_id}/export'
        headers = {
            'accept': '*/*',
            'Content-Type': 'application/json'
        }
        payload = {
            'id': layer_id,
            'file_type': 'gpkg',
            'file_name': file_name,
            'crs': 'EPSG:4326'
        }
        response = self.make_request('POST', url, headers=headers, json=payload)

        if response.status_code == 200:
            output_path = f"/app/src/data/output/{file_name}.zip"
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Layer {layer_id} exported successfully and saved to {output_path}.")

            # Unzip the file
            extract_path = f"/app/src/data/output/{file_name}"
            with zipfile.ZipFile(output_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            print(f"Layer {layer_id} unzipped successfully to {extract_path}.")

        else:
            raise Exception(f"Failed to export layer: {response.status_code} - {response.text}")

    def upload_to_local_db(self, file_name):
        file_path = f"/app/src/data/output/{file_name}/{file_name}/{file_name}.gpkg"

        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        db_host = os.getenv("POSTGRES_HOST")
        db_port = os.getenv("POSTGRES_PORT")
        db_name = os.getenv("POSTGRES_DB")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")

        db_connection_string = f'PG:host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}'

        # Use the '-nlt PROMOTE_TO_MULTI' option to handle complex geometries
        command = [
            'ogr2ogr',
            '-f', 'PostgreSQL',
            db_connection_string,
            file_path,
            '-nlt', 'PROMOTE_TO_MULTI',
            '-nln', file_name,
            '-lco', 'SCHEMA=temporal',
            '-lco', 'OVERWRITE=YES'
        ]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            print(f"File {file_path} uploaded to database successfully.")
        else:
            print(f"Failed to upload {file_path} to database. Error: {result.stderr.decode()}")
            raise Exception(f"ogr2ogr error: {result.stderr.decode()}")

    @timing
    def migrate_heatmaps_to_local_db(self, poi_category, heatmap_description, replace=False):

        sql_get_heatmap_layer_ids_of_poi_category = f"""
            SELECT heatmap_layer_id
            FROM master_thesis_metadata
            WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL AND heatmap_layer_id IS NOT NULL;
        """
        heatmap_layer_ids = self.db.select(sql_get_heatmap_layer_ids_of_poi_category)
        heatmap_layer_ids = [heatmap_layer_id[0] for heatmap_layer_id in heatmap_layer_ids]

        for counter, heatmap_layer_id in enumerate(heatmap_layer_ids, start=1):
            sql_get_reference_clipped_area = f"""
                SELECT reference_clipped_area
                FROM master_thesis_metadata
                WHERE heatmap_layer_id = '{heatmap_layer_id}';
            """
            reference_clipped_area = self.db.select(sql_get_reference_clipped_area)[0][0]

            file_name = f'{heatmap_description}_{poi_category}_{reference_clipped_area}'

            # Check if the file name exists in the temporal schema of the database
            sql_check_table = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'temporal'
                    AND table_name = '{file_name}'
                );
            """
            exists_in_db = self.db.select(sql_check_table)[0][0]

            # Perform checks before downloading and uploading
            file_path = f"/app/src/data/output/{file_name}/{file_name}/{file_name}.gpkg"
            if replace or not os.path.isfile(file_path) or not exists_in_db:
                if replace:
                    print(f"Iteration {counter} of {len(heatmap_layer_ids)}: Replace is set to True. Downloading from GOAT and migrating {file_name} to local db.")
                elif not os.path.isfile(file_path):
                    print(f"Iteration {counter} of {len(heatmap_layer_ids)}: File {file_path} does not exist. Downloading from GOAT and migrating {file_name} to local db.")
                else:
                    print(f"Iteration {counter} of {len(heatmap_layer_ids)}: File {file_name} not found in database. Downloading from GOAT and migrating {file_name} to local db.")

                self.download_and_unzip_layer(heatmap_layer_id, file_name)
                self.upload_to_local_db(file_name)
            else:
                print(f"Iteration {counter} of {len(heatmap_layer_ids)}: Skipping download and upload for {file_name} as all checks passed.")

    def clip_and_combine_heatmaps(self, poi_category, heatmap_description):
        print(f"Starting to clip and combine {heatmap_description} for POI category: {poi_category}")

        # if poi_category == 'population':
        #     table_name = "germany_grid_25km_4326"
        # else:
        #     table_name = "germany_grid_50km_4326"
        table_name = "germany_grid_50km_4326"

        sql_get_reference_clipped_areas_of_poi_category = f"""
            SELECT reference_clipped_area
            FROM master_thesis_metadata
            WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL AND heatmap_layer_id IS NOT NULL;
        """
        reference_clipped_areas = self.db.select(sql_get_reference_clipped_areas_of_poi_category)
        reference_clipped_areas = [reference_clipped_area[0] for reference_clipped_area in reference_clipped_areas]

        # If heatmap_layer_ids is empty, skip the rest
        if not reference_clipped_areas:
            print("No heatmaps to combine, skipping...")
            return

        print(f"{len(reference_clipped_areas)} reference clipped areas to process.")

        # migrate reference table from Geonode to local db
        print("Migrating reference grid from Geonode to local database.")
        create_table_dump(
            db_config=self.db_rd.db_config,
            schema='temporal',
            table_name=table_name
        )

        self.db.perform(f"DROP TABLE IF EXISTS temporal.{table_name};")

        restore_table_dump(
            db_config=self.db.db_config,
            schema='temporal',
            table_name=table_name
        )

        # create result_table
        sql_create_result_table = f"""
            DROP TABLE IF EXISTS {heatmap_description}_{poi_category};
            CREATE TABLE {heatmap_description}_{poi_category} (
                id TEXT PRIMARY KEY,
                h3_index TEXT,
                accessibility FLOAT,
                geom GEOMETRY(MULTIPOLYGON, 4326)
            );
        """
        self.db.perform(sql_create_result_table)

        # Combine heatmaps
        for reference_clipped_area in reference_clipped_areas:
            print(f"Processing reference clipped area: {reference_clipped_area}")
            # insert the clipped data into the result table
            sql_clip_and_combine_heatmaps = f"""
                INSERT INTO {heatmap_description}_{poi_category} (id, h3_index, accessibility, geom)
                WITH grid_cell AS (
                    SELECT *
                    FROM temporal.{table_name}
                    WHERE id = '{reference_clipped_area}'
                )
                SELECT h.id, h.h3_index, h.accessibility, h.geom
                FROM temporal.{heatmap_description}_{poi_category}_{reference_clipped_area} h
                JOIN grid_cell g ON ST_Intersects(h.geom, g.geom);
            """
            self.db.perform(sql_clip_and_combine_heatmaps)

        sql_create_index = f"""
            CREATE INDEX ON {heatmap_description}_{poi_category} USING GIST (geom);
        """
        self.db.perform(sql_create_index)

        print(f"Finished clipping and combining {heatmap_description} for POI category: {poi_category}")

        sql_clear_heatmap_layer_id_column = f"""
            UPDATE master_thesis_metadata
            SET heatmap_layer_id = NULL
            WHERE poi_category = '{poi_category}';
        """
        self.db.perform(sql_clear_heatmap_layer_id_column)
        print("Cleared heatmap_layer_id column in metadata table.")

        # clear temporal schema
        sql_drop_temporal_heatmap_tables = """
            DO $$
            DECLARE
                table_list TEXT[];
                command TEXT;
            BEGIN
                SELECT ARRAY_AGG('DROP TABLE ' || table_schema || '.' || table_name || ' CASCADE')
                INTO table_list
                FROM information_schema.tables
                WHERE table_schema = 'temporal'
                AND table_name LIKE 'heatmap_%';

                IF array_length(table_list, 1) IS NOT NULL THEN
                    FOREACH command IN ARRAY table_list
                    LOOP
                        EXECUTE command;
                    END LOOP;
                END IF;
            END $$;
        """
        self.db.perform(sql_drop_temporal_heatmap_tables)
        print("Cleared temporal schema from heatmap tables.")

        # clear output folder
        directory = "/app/src/data/output"
        prefix = "heatmap_"

        for filename in os.listdir(directory):
            if filename.startswith(prefix):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        print(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        print(f"Deleted folder: {file_path}")
                except Exception as e:
                    print(f"Failed to delete {file_path}. Reason: {e}")

        # delete heatmap layers from project
        sql_get_layer_project_ids_not_to_delete = f"""
            SELECT layer_project_id
            FROM master_thesis_metadata
            WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL;
        """
        layer_project_ids_not_to_delete = self.db.select(sql_get_layer_project_ids_not_to_delete)
        layer_project_ids_not_to_delete = [str(layer_project_id[0]) for layer_project_id in layer_project_ids_not_to_delete]

        all_project_layer_ids = self.get_layer_project_ids(project_id=client.get_project_id())
        layer_project_ids_to_delete = [id for id in all_project_layer_ids if str(id) not in layer_project_ids_not_to_delete]

        self.delete_layers_from_project(self.get_project_id(), layer_project_ids_to_delete)
        print("Deleted heatmap layers from project.")

if __name__ == "__main__":
    client = GoatAPIClient(db=Database(settings.LOCAL_DATABASE_URI), db_rd=Database(settings.RAW_DATABASE_URI))

    # create project folder
    folder_payload = {
        "name": "Master Thesis"
    }
    try:
        client.create_project_folder(payload=folder_payload, replace=False)

        project_payload = {
            "name": "Master Thesis",
            "description": "Master Thesis Project description",
            "tags": ["tag1", "tag2"],
            "initial_view_state": {
                "latitude": 48.1502132,
                "longitude": 11.5696284,
                "zoom": 12,
                "min_zoom": 0,
                "max_zoom": 20,
                "bearing": 0,
                "pitch": 0
            }
        }
        # create project
        client.create_project(payload=project_payload, replace=False)

        # dicts with heatmap_attributes for every_poicategory
        # 6 heatmaps for every poi category walking: gussian, cumulative_gaussian, closest_abverage
        for poi_category, heatmap_configs in poi_dicts.items():
            print(f"Processing POI category: {poi_category}")
            processed_new_heatmap = False  # Initialize flag

            for heatmap_description, heatmap_attributes in heatmap_configs.items():
                # skip POI category if all heatmaps calculated
                if Database(settings.LOCAL_DATABASE_URI).select(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{heatmap_description}_{poi_category}');")[0][0]:
                    print(f"Heatmap {heatmap_description}_{poi_category} already exists")
                    continue

                # loop over the geoms of grid table, buffer grid cells (10km), intersect with POI table and store resulting poi file in project data folder (not project)
                client.clip_and_upload_pois(poi_category=poi_category, replace=False)

                # add clipped poi layers to project
                client.add_poi_layers_to_project(poi_category=poi_category, replace=False)
                break

            for heatmap_description, heatmap_attributes in heatmap_configs.items():
                if Database(settings.LOCAL_DATABASE_URI).select(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{heatmap_description}_{poi_category}');")[0][0]:
                    print(f"Heatmap {heatmap_description}_{poi_category} already exists")
                    continue

                print(f"Processing heatmap description: {heatmap_description}")

                client.calculate_poi_heatmaps(poi_category=poi_category, heatmap_attributes=heatmap_attributes, heatmap_description=heatmap_description, replace=False)

                # download all heatmaps -> unzip -> upload to local db
                client.migrate_heatmaps_to_local_db(poi_category=poi_category, heatmap_description=heatmap_description, replace=False)

                # clip and combine heatmaps
                client.clip_and_combine_heatmaps(poi_category=poi_category, heatmap_description=heatmap_description)

                # Set flag when a new heatmap is processed
                processed_new_heatmap = True

            if processed_new_heatmap:  # Only run deletion and cleanup if a new heatmap was processed

                # delete all project layers
                client.delete_layers_from_project(project_id=client.get_project_id(), layer_project_ids=client.get_layer_project_ids(project_id=client.get_project_id()))

                # delete all files from project folder
                sql_get_comment = "SELECT obj_description('master_thesis_metadata'::regclass);"
                comment = Database(settings.LOCAL_DATABASE_URI).select(sql_get_comment)
                folder_id = re.search('project_folder_id:([^,]*)', comment[0][0]).group(1)

                layer_ids_project_folder = client.get_layer_ids_of_folder(folder_id)
                for layer_id in layer_ids_project_folder:
                    client.delete_layer(layer_id)
                print("Deleted all layers from project folder.")

                # clear output folder?
                # Specify the directory and prefix
                directory = "/app/src/data/output"
                prefix = f"{poi_category}_"

                for filename in os.listdir(directory):
                    if filename.startswith(prefix):
                        file_path = os.path.join(directory, filename)
                        try:
                            if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                                print(f"Deleted file: {file_path}")
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                                print(f"Deleted folder: {file_path}")
                        except Exception as e:
                            print(f"Failed to delete {file_path}. Reason: {e}")


            # #TODO: add additional data like regiostar or population

            # print(f"Finished processing POI category: {poi_category}")

    except Exception as e:
        print(f"An error occurred: {e}")
