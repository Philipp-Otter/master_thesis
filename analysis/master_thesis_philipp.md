Workflow 

1. Create Postgres table to track everything

    Table structure: master_thesis_metadata -> add comment with project_id: ... and project_folder_id: ... 

    layer id (pkey) | reference clipped area | POI type
    -------- | -------- | -------- | --------
    test | test | test 

2. Create project folder if not exist
3. Create project within project folder if my project does not exist
4. clip GeoGitter 100km using the border of Germany (before adjusting projection and geometry type of both tables)

    SELECT ST_SRID(geom)
    FROM public.germany_geogitter_inspire_100km
    LIMIT 1;

    DROP TABLE IF EXISTS temporal.germany_geogitter_inspire_100km_4326;
    CREATE TABLE temporal.germany_geogitter_inspire_100km_4326 AS
    SELECT id, ST_Multi(ST_Transform(geom, 4326)) AS geom
    FROM public.germany_geogitter_inspire_100km;
    CREATE INDEX ON temporal.germany_geogitter_inspire_100km_4326 USING GIST(geom);

    CREATE TABLE temporal.germany_geogitter_inspire_100km_4326_clipped AS
    SELECT 
        g.id, 
        ST_Intersection(g.geom, b.the_geom) AS geom
    FROM 
        temporal.germany_geogitter_inspire_100km_4326 g, 
        public."DEU_adm0" b
    WHERE 
        ST_Intersects(g.geom, b.the_geom);

5. Use GeoGitter 100km -> check if max amount of POIs of specifc category < 20-30k
    decision based on e.g.:

    SELECT 
        g.id AS grid_id,
        COUNT(p.*) AS point_count
    FROM 
        temporal.geogitter_germany_100km g
    LEFT JOIN 
        poi.poi_tourism_leisure p
        WHERE p.category = 'cinema'
    ON 
        ST_Intersects(g.geom, p.geom)
    GROUP BY 
        g.id
    ORDER BY 
        point_count DESC;

6. input/ part of the payload should be the poi_category (validation use kart_pois.poi_categories from local db) -> store folder id + descriptive Name (poi_category) in local db
7. loop the geom column of our grid table (temporal.germany_geogitter_inspire_100km_4326_clipped from db_rd)
    7.1 clip POI layer to buffered parts of Germany 
        -> buffer the geoms by 10km (geom is in epsg 4326) and then clip the poi table (schema is always poi, but the table_name we need to get from kart_pois.poi_categories column table_name)
        -> geom to clip POIS = GeoGitter cell with a buffer of 10km as the cycling heatmap uses a cycling speed of 15km/h and the max travel time of the sensitivity curve is 30 min -> 15km/h * 30 min = 7,5km -> use 10 km
    7.2 create file e.g. geopackage to upload it to GOAT project folder + naming logic + store layer id -> check if combination of POI and area was already uploaded -> update or replace file
    7.3 document in ids table -> information of the new layer in the master_thesis_metadata table (id of the geogitter is the reference_clipped_area)
8. for specific folder -> loop over specific layer ids
