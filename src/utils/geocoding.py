import argparse
import csv
import json
import os
import psycopg2
import requests
import pandas as pd
from src.core.config import settings
from src.core.enums import (
    SaveGeocodedDataMethodType,
    GeocoderOriginFormatType,
    GeocoderResultSchema,
)
from src.db.db import Database
from src.utils.utils import print_info


class Geocoder:
    def __init__(self, db: Database):
        self.db = db
        self.geocoders = {
            "nominatim": self.geocode_nominatim,
            "openrouteservice": self.geocode_openrouteservice,
            "geoapify": self.geocode_geoapify,
            "google": self.geocode_google,
        }
        self.engine = self.db.return_sqlalchemy_engine()

    def geocode(self, address_columns, address):
        
        # Create a string from the address columns remove if empty
        address_parts = [str(address.get(column, "")) for column in address_columns if address.get(column, "") != ""]
        address_str = ", ".join(address_parts)
        results = {}
        for geocoder in self.geocoders:
            location = self.geocoders[geocoder](address_str)
            if location is None:
                continue
            else:
                results[geocoder] = location

        return results

    def geocode_google(self, address):
        api_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"address": address, "key": settings.GOOGLE_API_KEY}
        response = requests.get(api_url, params=params)
        
        if response.status_code != 200:
            return None
        
        data = json.loads(response.text)
        if data["status"] == "OK":
            location = data["results"][0]["geometry"]["location"]
            return (location["lat"], location["lng"])
        else:
            return None

    def geocode_nominatim(self, address):
        api_url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json"}
        response = requests.get(api_url, params=params)
        
        # Check status code of the response is 200
        if response.status_code != 200:
            return None
        
        data = json.loads(response.text)
        if len(data) > 0:
            location = data[0]
            return (location["lat"], location["lon"])
        else:
            return None

    def geocode_openrouteservice(self, address):
        
        headers = {
            "Accept": "application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8",
        }
        response = requests.get(f"""https://api.openrouteservice.org/geocode/search?api_key={settings.OPENROUTESERVICE_API_KEY}&text={address}""", headers=headers)
        
        if response.status_code != 200:
            return None
        
        data = json.loads(response.text)
        if len(data["features"]) > 0:
            location = data["features"][0]["geometry"]["coordinates"]
            return (location[1], location[0])
        else:
            return None

    def geocode_geoapify(self, address):
        
        response = requests.get(f"https://api.geoapify.com/v1/geocode/search?text={address}&format=json&apiKey={settings.GEOAPIFY_API_KEY}")
        data = response.json()
        data = json.loads(response.text)
        if len(data["results"]) > 0:
            location = (data["results"][0]["lat"], data["results"][0]["lon"])
            return location
        else:
            return None

    # Read Table in the formats JSON, GeoJSON, CSV
    def read_table(
        self, location: str, format: GeocoderOriginFormatType
    ) -> pd.DataFrame:

        if format == GeocoderOriginFormatType.sql.value:
            df = pd.read_sql_table(
                table=table_name,
                engine=self.engine,
                schema=GeocoderResultSchema.temporal.value,
            )

        elif format == GeocoderOriginFormatType.csv.value:
            df = pd.read_csv(location)
            table_name = location.split("/")[-1].split(".")[0]
        else:
            raise ValueError(
                "Please specify either table_name in the PostgreSQL DB or a file directory to a CSV file."
            )
        return {"data": df, "format": format, "table_name": table_name}

    def create_or_update_table(
        self, data: dict, table_name: str, method: SaveGeocodedDataMethodType = "create"
    ):

        if method != SaveGeocodedDataMethodType.create.value:
            print_info("Appending data to table. No table will be created.")
        elif (
            method == SaveGeocodedDataMethodType.create.value
            and data["format"] == "csv"
        ):
            print_info("Creating a new table with the same schema as the input table.")
            data["data"].to_sql(
                table_name,
                self.engine,
                if_exists="replace",
                index=False,
                schema="temporal",
            )
        elif method == SaveGeocodedDataMethodType.create and data["format"] == "sql":
            print_info("Geometry columns will be added to the exists table.")

        if method != SaveGeocodedDataMethodType.append.value:
            # Add columns for geometries to the table
            for geocoder in self.geocoders:
                self.db.perform(
                    f"ALTER TABLE temporal.{table_name} ADD COLUMN {geocoder}_geom geometry(Point, 4326);"
                )

    def save_geocoded_table_to_postgis(self, data: pd.DataFrame, table_name: str):
        # Save the geocoded table to the temporal schema in the DB
        data.to_sql(
            table_name,
            self.engine,
            if_exists="replace",
            index=False,
            schema="temporal",
        )
        
        # Create a geometry column for each geocoder
        for geocoder in self.geocoders:
            
            # Create a geometry column
            self.db.perform(
                f"ALTER TABLE temporal.{table_name} ADD COLUMN {geocoder}_geom geometry(Point, 4326);"
            )
            
            # Update the geometry column
            self.db.perform(
                f"""UPDATE temporal.{table_name} 
                SET {geocoder}_geom = 
                CASE WHEN {geocoder}_lon IS NOT NULL 
                AND {geocoder}_lat IS NOT NULL 
                THEN ST_SETSRID(ST_MAKEPoint({geocoder}_lon::float, {geocoder}_lat::float), 4326)
                ELSE NULL END;"""
            )

    def geocode_table(self, data: dict, address_columns: list[str]):

        # Loop through the rows and geocode them
        locations = []

        df = data["data"]
        table_name = data["table_name"]
        # Append new columns to the table
        for geocoder in self.geocoders:
            df = df.assign(**{f"{geocoder}_lat": None})
            df = df.assign(**{f"{geocoder}_lon": None})

        # Save the header to a file
        df.head(0).to_csv(
            f"/app/src/data/{table_name}_geocoded.csv", index=False, header=True
        )

        cnt = 0 
        for index, row in df.iterrows():
            cnt += 1
            locations = self.geocode(address_columns, row)
            
            # Add the geocode locations to the row in the df 
            for geocoder in locations:
                df.loc[index, f"{geocoder}_lat"] = locations[geocoder][0]
                df.loc[index, f"{geocoder}_lon"] = locations[geocoder][1]

            # Save to file if the number of rows is a multiple of 100
            if cnt % 5 == 0:
                # Save the 100 rows to a file
                df.iloc[cnt-5:cnt].to_csv(
                    f"/app/src/data/{table_name}_geocoded.csv",
                    index=False,
                    header=False,
                    mode="a",
                )
                print_info(f"Processed {cnt} out of {len(df.index)} rows and saved to file.")
        
        return df 
                

def main():
    db = Database(settings.RAW_DATABASE_URI)
    geocode = Geocoder(db)
    address_columns = ["addr:street", "addr:city", "addr:postcode",	"country"]
    
    print_info("Reading table from CSV file.")
    data = geocode.read_table(
        location="/app/src/data/dentist_test.csv", format="csv"
    )
    
    print_info("Creating or updating table.")
    geocode.create_or_update_table(
        data=data, table_name=data["table_name"], method="create"
    )
    
    print_info("Geocoding table.")
    df = geocode.geocode_table(data, address_columns)
    
    print_info("Saving geocoded table to PostGIS.")
    geocode.save_geocoded_table_to_postgis(df, data["table_name"])    

if __name__ == "__main__":
    main()
