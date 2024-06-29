import os
import re
import time
import requests
import geopandas as gpd
from shapely import wkb
import pandas as pd
import numpy as np
from collections import OrderedDict

from src.core.config import settings
from src.db.db import Database


class GoatAPIClient:
    def __init__(self, db: Database, db_rd: Database):
        self.user = os.getenv("DEV_GOAT_V2_USER")
        self.password = os.getenv("DEV_GOAT_V2_PASSWORD")
        self.client_id = os.getenv("DEV_GOAT_V2_CLIENT_ID")
        self.client_secret = os.getenv("DEV_GOAT_V2_CLIENT_SECRET")
        self.keycloak_token_url = "https://auth.dev.plan4better.de/realms/p4b/protocol/openid-connect/token"
        self.access_token = None
        self.db = db
        self.db_rd = db_rd

    def get_access_token(self):
        if self.access_token is None:
            self.renew_access_token()
        return self.access_token

    def renew_access_token(self):
        payload = {
            'client_id': self.client_id,
            'username': self.user,
            'password': self.password,
            'grant_type': 'password',
            'client_secret': self.client_secret
        }
        response = requests.post(self.keycloak_token_url, data=payload)
        if response.status_code == 200:
            token_info = response.json()
            self.access_token = token_info['access_token']
            print("Access token renewed successfully.")
        else:
            raise Exception(f"Failed to get token: {response.status_code} - {response.text}")

    def make_request(self, method, url, **kwargs):
        headers = kwargs.get('headers', {})
        headers['Authorization'] = f'Bearer {self.get_access_token()}'
        kwargs['headers'] = headers

        response = requests.request(method, url, **kwargs)

        # Check for token expiration and renew if needed
        if response.status_code == 401:  # Unauthorized
            print("Access token expired. Renewing token...")
            self.renew_access_token()
            headers['Authorization'] = f'Bearer {self.access_token}'
            response = requests.request(method, url, **kwargs)

        return response

    def delete_folder(self, folder_id):
        url = f'https://api.goat.dev.plan4better.de/api/v2/folder/{folder_id}'
        response = self.make_request('DELETE', url, headers={'accept': '*/*'})
        if response.status_code == 204:
            print(f"Folder {folder_id} deleted successfully.")
        else:
            raise Exception(f"Failed to delete folder: {response.status_code} - {response.text}")

    def create_folder(self, folder_name, replace=False):
        search_url = f'https://api.goat.dev.plan4better.de/api/v2/folder?search={folder_name}&order_by=created_at&order=descendent'
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
                else:
                    return existing_folder  # Return the existing folder details
            else:
                print(f"No existing folder named '{folder_name}' found. Creating new folder.")
        else:
            raise Exception(f'Failed to search for folder: {search_response.status_code} - {search_response.text}')

        create_url = 'https://api.goat.dev.plan4better.de/api/v2/folder'
        create_response = self.make_request('POST', create_url, headers=headers, json={'name': folder_name})

        if create_response.status_code == 201:
            print(create_response.json())
            return create_response.json()
        else:
            raise Exception(f'Folder creation failed: {create_response.status_code} - {create_response.text}')

    def create_project_folder(self, payload, replace=False):
        # Ensure metadata table exists
        self.ensure_metadata_table_exists()

        folder_name = payload["name"]
        folder = self.create_folder(folder_name, replace)

        if replace is True:

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
        search_url = f'https://api.goat.dev.plan4better.de/api/v2/project?search={project_name}&order_by=created_at&order=descendent'
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

        create_url = 'https://api.goat.dev.plan4better.de/api/v2/project'
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
        url = f'https://api.goat.dev.plan4better.de/api/v2/project/{project_id}'
        response = self.make_request('DELETE', url, headers={'accept': '*/*'})
        if response.status_code == 204:
            print(f"Project {project_id} deleted successfully.")
        else:
            raise Exception(f"Failed to delete project: {response.status_code} - {response.text}")

    def delete_layer(self, layer_id: str):
        url = f'https://api.goat.dev.plan4better.de/api/v2/layer/{layer_id}'
        headers = {
            'accept': '*/*'
        }

        response = self.make_request('DELETE', url, headers=headers)
        if response.status_code == 204:
            print(f"Layer with ID {layer_id} deleted successfully.")
        else:
            print(f"Failed to delete layer: {response.status_code} - {response.text}")


    def ensure_metadata_table_exists(self):
        sql_create_metadata_table = """
            CREATE TABLE IF NOT EXISTS master_thesis_metadata (
                layer_id TEXT PRIMARY KEY,
                reference_clipped_area TEXT NOT NULL,
                poi_category TEXT NOT NULL,
                layer_project_id TEXT,
                heatmap_layer_project_id TEXT
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
        sql_validate_poi_category = f"SELECT * FROM kart_pois.poi_categories WHERE category = '{poi_category}'"
        result = self.db.select(sql_validate_poi_category)
        if result:
            return True
        else:
            raise ValueError(f"Invalid POI category: {poi_category}")

    def get_poi_table_name(self, poi_category):
        try:
            # find correct poi table name within kart
            sql_kart_poi_table_name = f"""
                SELECT table_name
                FROM kart_pois.poi_categories
                WHERE category = '{poi_category}';
            """
            return self.db.select(sql_kart_poi_table_name)[0][0]
        except IndexError:
            print(f"No table name found for category '{poi_category}'")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    # loop over the geoms of grid table, buffer grid cells (10km), intersect with POI table and store resulting poi file in project folder
    def clip_and_upload_pois(self, poi_category, replace = False):
        """
        Clips and uploads points of interest (POIs) for a given category.

        Args:
        poi_category (str): The category of the POIs to be clipped and uploaded.
        replace (bool): Flag to indicate whether to replace existing data. Default is False.
        """

        self.validate_poi_category(poi_category)

        sql_select_id_geom = """
            SELECT id, geom
            FROM temporal.germany_geogitter_inspire_100km_4326_clipped
        """

        cur = self.db_rd.conn.cursor()

        try:
            cur.execute(sql_select_id_geom)
            for grid_id, grid_geom in cur.fetchall():

                sql_create_poi_upload_table = f"""
                    DROP TABLE IF EXISTS poi_upload;
                    CREATE TEMP TABLE poi_upload AS (
                        SELECT *
                        FROM poi.{self.get_poi_table_name(poi_category)}
                        WHERE 1=0
                    );
                """
                cur.execute(sql_create_poi_upload_table)

                # Get column names
                sql_get_columns = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'poi' AND table_name = '{self.get_poi_table_name(poi_category)}';"
                cur.execute(sql_get_columns)
                columns = cur.fetchall()
                column_names = ', '.join([column[0] for column in columns])

                clip_poi_data = f"""
                    INSERT INTO poi_upload({column_names})
                    WITH region AS (
                        SELECT ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_GeomFromText(ST_AsText('{grid_geom}')), 4326), 3857), 10000), 4326) AS geom
                    )
                    SELECT DISTINCT ON (p.id) p.*
                    FROM poi.{self.get_poi_table_name(poi_category)} p
                    JOIN region r ON ST_Intersects(p.geom, r.geom)
                    WHERE p.category = '{poi_category}'
                """
                cur.execute(clip_poi_data)
                self.db_rd.conn.commit()

                # Fetch data from PostGIS into a pandas DataFrame
                cur.execute("SELECT * FROM poi_upload;")
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                dataframe = pd.DataFrame(rows, columns=columns)

                # If dataframe is empty, skip the rest of the loop
                if dataframe.empty:
                    print(f"No data for {poi_category}_{grid_id}, skipping...")
                    continue

                # Convert the WKB geometry data into a format that GeoPandas can understand
                dataframe['geom'] = dataframe['geom'].apply(wkb.loads)

                # Convert the pandas DataFrame into a GeoDataFrame
                geodataframe = gpd.GeoDataFrame(dataframe, geometry='geom')
                geodataframe = geodataframe.set_crs(epsg=4326)

                # Convert list columns to strings
                for col in geodataframe.columns:
                    if not geodataframe[col].empty and isinstance(geodataframe[col].iloc[0], list):
                        geodataframe[col] = geodataframe[col].apply(str)

                # Convert lists in 'other_categories' column to strings
                if 'other_categories' in geodataframe.columns:
                    geodataframe['other_categories'] = geodataframe['other_categories'].apply(str)


                # Replace 'None' strings with np.nan
                geodataframe = geodataframe.replace({None: np.nan})
                geodataframe = geodataframe.replace({'None': np.nan})

                # Export GeoDataFrame to a GeoPackage
                output_path = f"/app/src/data/output/{poi_category}_{grid_id}.gpkg"
                geodataframe.to_file(output_path, driver="GPKG")

                # check if file already in application (only then returns layer_id) and metadata table
                # if in both and replace = False skip
                # if in one or none or replace = True -> delete existing from application and table -> upload
                layer_name = f"{poi_category}_{grid_id}"
                layer_id = self.get_layer_id_by_name(name=layer_name)
                file_in_metadata = self.check_if_file_in_metadata_table(reference_clipped_area=grid_id, poi_category=poi_category)

                if layer_id and file_in_metadata:
                    if not replace:
                        # Skip
                        print(f"{layer_name} already in GOAT and replace is False, skipping...")
                        pass
                    else:
                        # Delete existing file from application and table
                        self.delete_layer(layer_id)
                        self.delete_file_from_metadata_table(reference_clipped_area=grid_id, poi_category=poi_category)
                        # Upload the GeoPackage to the server
                        self.upload_file(output_path, grid_id, poi_category)
                elif layer_id or file_in_metadata:
                    # Delete existing file from application and table
                    if layer_id:
                        self.delete_layer(layer_id)
                    if file_in_metadata:
                        self.delete_file_from_metadata_table(reference_clipped_area=grid_id, poi_category=poi_category)
                    # Upload the GeoPackage to the server
                    self.upload_file(output_path, grid_id, poi_category)
                else:
                    # Upload the GeoPackage to the server
                    self.upload_file(output_path, grid_id, poi_category)

        except Exception as e:
            print(f"An error occurred: {e}")
            self.db_rd.conn.rollback()

        finally:
            cur.close()

    def upload_file(self, file_path, grid_id, poi_category, max_retries=3):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"File path is invalid or file does not exist: {file_path}")

        url = 'https://api.goat.dev.plan4better.de/api/v2/layer/file-upload'
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
                layer_id = self.get_layer_id_by_name(name = str(poi_category + '_' + grid_id))

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
        url = 'https://api.goat.dev.plan4better.de/api/v2/layer/internal'
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
        url = 'https://api.goat.dev.plan4better.de/api/v2/layer?order_by=created_at&order=descendent&page=1&size=50'
        headers = {
            'accept': 'application/json'
        }
        data = {
            "search": name
        }

        response = self.make_request('POST', url, headers=headers, json=data)
        if response.status_code == 200:
            response_data = response.json()
            layers = response_data.get("items", [])
            layer_found = False
            for layer in layers:
                if layer['name'].lower() == name.lower():
                    layer_found = True
                    return layer['id']
            # if not layer_found:
            #     print("Layer not found.")
        else:
            print(f"Failed to get layer: {response.status_code} - {response.text}")
        return None

    def add_poi_layers_to_project(self, poi_category, replace=False):
        # Get the project ID from the metadata table comment
        sql_get_comment = "SELECT obj_description('master_thesis_metadata'::regclass);"
        comment = self.db.select(sql_get_comment)
        project_id = re.search('project_id:([^,]*)', comment[0][0]).group(1)
        print(f"Retrieved project ID: {project_id}")

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
            delete_url = f'https://api.goat.dev.plan4better.de/api/v2/project/{project_id}/layer'
            delete_params = f'layer_project_id={layer_project_id}'
            delete_full_url = f"{delete_url}?{delete_params}"
            print(f"Deleting layer with project ID {layer_project_id} from project {project_id}")
            delete_response = self.make_request('DELETE', delete_full_url, headers={'accept': '*/*'})

            if delete_response.status_code != 204:
                raise Exception(f"Failed to delete layer {layer_project_id} from project: {delete_response.status_code} - {delete_response.text}")

    def add_layers_to_project(self, project_id, layer_ids):
        url = f'https://api.goat.dev.plan4better.de/api/v2/project/{project_id}/layer'
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
        url = f'https://api.goat.dev.plan4better.de/api/v2/job/{job_id}'
        response = self.make_request('GET', url, headers={'accept': 'application/json'})
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get job status: {response.status_code} - {response.text}")

    def create_heatmap_gravity(self, project_id, layer_project_id, heatmap_attributes):
        url = f'https://api.goat.dev.plan4better.de/api/v2/active-mobility/heatmap-gravity?project_id={project_id}'
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
            print("Heatmap gravity request successful. Job ID:", job_id)

            # wait until job is completed
            while True:
                status = self.check_job_status(job_id)['status_simple']
                print("Heatmap job status:", status)
                if status == 'finished':
                    # find a way to get the heatmap_layer_project_id -> i think neither name, layer_id nor project_layer_id are returned
                    heatmap_layer_project_id = self.check_job_status(job_id)['result']['project_layer_id']
                    layer_id = 'test'
                    sql_update_layer_project_id = f"""
                        UPDATE master_thesis_metadata
                        SET heatmap_layer_project_id = '{heatmap_layer_project_id}'
                        WHERE layer_id = '{layer_id}';
                    """
                    self.db.perform(sql_update_layer_project_id)
                    print(f"Added heatmap layer project id {layer_id} to metadata")
                    print("Heatmap job completed successfully.")
                    break
                elif status.get('status') == 'failed':
                    raise Exception("Heatmap job failed.")
                else:
                    print("Heatmap job still in progress...")
                    time.sleep(10)  # Wait for 10 seconds before checking again
            return job_id
        else:
            raise Exception(f"Heatmap gravity request failed: {response.status_code} - {response.text}")

    def calculate_poi_heatmaps(self, poi_category, heatmap_attributes, replace=False):
        # Get the project ID from the metadata table comment
        sql_get_comment = "SELECT obj_description('master_thesis_metadata'::regclass);"
        comment = self.db.select(sql_get_comment)
        project_id = re.search('project_id:([^,]*)', comment[0][0]).group(1)
        print(f"Retrieved project ID: {project_id}")

        if replace:
            sql_get_layer_project_ids_of_poi_category_to_calculate_heatmap = f"""
                SELECT layer_project_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL;
            """
            layer_project_ids_to_calculate_heatmap = self.db.select(sql_get_layer_project_ids_of_poi_category_to_calculate_heatmap)
            layer_project_ids_to_calculate_heatmap = [layer_project_id[0] for layer_project_id in layer_project_ids_to_calculate_heatmap]
            print(f"Project layers to calculate heatmap: {layer_project_ids_to_calculate_heatmap}")

            sql_get_heatmap_layer_project_ids_of_poi_category_to_delete = f"""
                SELECT heatmap_layer_project_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL AND heatmap_layer_project_id IS NOT NULL;
            """
            heatmap_layer_project_ids_to_delete = self.db.select(sql_get_heatmap_layer_project_ids_of_poi_category_to_delete)
            heatmap_layer_project_ids_to_delete = [heatmap_project_layer_id[0] for heatmap_project_layer_id in heatmap_layer_project_ids_to_delete]
            print(f"Heatmap project layers to delete: {heatmap_layer_project_ids_to_delete}")

            # Delete existing layers from the project
            self.delete_layers_from_project(project_id, heatmap_layer_project_ids_to_delete)
        else:
            sql_get_layer_project_ids_of_poi_category_to_add = f"""
                SELECT layer_project_id
                FROM master_thesis_metadata
                WHERE poi_category = '{poi_category}' AND layer_project_id IS NOT NULL AND heatmap_layer_project_id IS NULL;
            """
            layer_project_ids_to_calculate_heatmap = self.db.select(sql_get_layer_project_ids_of_poi_category_to_add)
            layer_project_ids_to_calculate_heatmap = [layer_project_id[0] for layer_project_id in layer_project_ids_to_calculate_heatmap]
            print(f"Project layers to calculate heatmap: {layer_project_ids_to_calculate_heatmap}")

        # If layer_project_ids_to_calculate_heatmap is empty, skip the rest
        if not layer_project_ids_to_calculate_heatmap:
            print("No layers to add, skipping...")
            return

        # Add new layers to the project
        for layer_project_id_to_calculate_heatmap in layer_project_ids_to_calculate_heatmap:
            self.create_heatmap_gravity(project_id, layer_project_id_to_calculate_heatmap, heatmap_attributes)
            print(f"Heatmaps for poi {poi_category} have been calculated in project {project_id} successfully.")

        return project_id



# Example usage
if __name__ == "__main__":
    client = GoatAPIClient(db = Database(settings.LOCAL_DATABASE_URI), db_rd = Database(settings.RAW_DATABASE_URI))

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


    # loop over the geoms of grid table, buffer grid cells (10km), intersect with POI table and store resulting poi file in project data folder (not project)
        client.clip_and_upload_pois(poi_category = 'cinema', replace=False)

    # add clipped poi layers to project
        client.add_poi_layers_to_project(poi_category = 'cinema', replace=False)

    # calculate heatmaps
        heatmap_attributes = {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        }
        client.calculate_poi_heatmaps(poi_category = 'cinema', heatmap_attributes=heatmap_attributes, replace=False)


    # if i use local db for export -> dump and restore grid

    # clip and export heatmaps to local db


    except Exception as e:
        print(e)



    # Payload for the heatmap gravity request



    except Exception as e:
        print(e)
