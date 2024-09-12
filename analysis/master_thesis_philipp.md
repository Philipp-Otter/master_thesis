Workflow 

1. Create Postgres table to track everything

    Table structure: master_thesis_metadata -> add comment with project_id: ... and project_folder_id: ... 

    layer id (pkey) | reference clipped area | POI type
    -------- | -------- | -------- | --------
    test | test | test 

2. Create project folder if not exist
3. Create project within project folder if my project does not exist
4. clip GeoGitter 50km using the border of Germany (before adjusting projection and geometry type of both tables)
    
    *new:*

    DROP TABLE IF EXISTS temporal.germany_grid_50km_4326;
    CREATE TABLE temporal.germany_grid_50km_4326 (
        id SERIAL PRIMARY KEY,
        geom geometry(MULTIPOLYGON, 4326)
    );

    WITH 
    bounds AS (
        SELECT ST_Extent(geom) AS geom FROM poi.geom_ref WHERE id = 'DE'
    ),
    merged_geometries AS (
        SELECT ST_Union(geom) AS geom
        FROM poi.geom_ref
        WHERE id = 'DE'
    ),
    grid AS (
        SELECT ST_MakeEnvelope(
            x::numeric, 
            y::numeric, 
            (x + 0.45)::numeric, 
            (y + 0.45)::numeric, 
            4326
        ) AS geom,
        row_number() OVER () as grid_id
        FROM generate_series(floor(ST_XMin((SELECT geom FROM bounds)))::numeric, ceiling(ST_XMax((SELECT geom FROM bounds)))::numeric, 0.45) AS x
        CROSS JOIN generate_series(floor(ST_YMin((SELECT geom FROM bounds)))::numeric, ceiling(ST_YMax((SELECT geom FROM bounds)))::numeric, 0.45) AS y
    ),
    intersections AS (
        SELECT 
            g.grid_id,
            (ST_Dump(ST_Intersection(m.geom, ST_Buffer(g.geom, -0.00001)))).geom AS geom
        FROM merged_geometries m
        JOIN grid g ON ST_Intersects(m.geom, g.geom)
    ),
    filtered_intersections AS (
        SELECT grid_id, geom
        FROM intersections
        WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
    )
    INSERT INTO temporal.germany_grid_50km_4326 (geom)
    SELECT ST_Multi(ST_Union(geom)) AS geom
    FROM filtered_intersections
    GROUP BY grid_id;

    CREATE INDEX ON temporal.germany_grid_50km_4326 USING gist(geom);


    *old:*
    DROP TABLE IF EXISTS temporal.germany_grid_50km_4326;
    CREATE TABLE temporal.germany_grid_50km_4326 (
        id SERIAL PRIMARY KEY,
        geom geometry(MULTIPOLYGON, 4326)
    );

    WITH 
    bounds AS (
        SELECT ST_Extent(geom) AS geom FROM poi.geom_ref WHERE id = 'DE'
    ),
    merged_geometries AS (
        SELECT ST_Union(geom) AS geom
        FROM poi.geom_ref
        WHERE id = 'DE'
    ),
    grid AS (
        SELECT ST_MakeEnvelope(
            x::numeric, 
            y::numeric, 
            (x + 0.45)::numeric, 
            (y + 0.45)::numeric, 
            4326
        ) AS geom
        FROM generate_series(floor(ST_XMin((SELECT geom FROM bounds)))::numeric, ceiling(ST_XMax((SELECT geom FROM bounds)))::numeric, 0.45) AS x
        CROSS JOIN generate_series(floor(ST_YMin((SELECT geom FROM bounds)))::numeric, ceiling(ST_YMax((SELECT geom FROM bounds)))::numeric, 0.45) AS y
    ),
    intersections AS (
        SELECT 
            (ST_Dump(ST_Intersection(m.geom, ST_Buffer(g.geom, -0.00001)))).geom AS geom
        FROM merged_geometries m
        JOIN grid g ON ST_Intersects(m.geom, g.geom)
    )
    INSERT INTO temporal.germany_grid_50km_4326 (geom)
    SELECT ST_Multi(ST_Union(geom)) AS geom
    FROM intersections 
    WHERE ST_GeometryType(geom) = 'ST_Polygon'
    GROUP BY ST_SnapToGrid(geom, 0.45);

    CREATE INDEX ON temporal.germany_grid_50km_4326 USING gist(geom);

    *really old:*
    SELECT ST_SRID(geom)
    FROM public.germany_geogitter_inspire_10km
    LIMIT 1;

    DROP TABLE IF EXISTS temporal.germany_geogitter_inspire_10km_4326;
    CREATE TABLE temporal.germany_geogitter_inspire_10km_4326 AS
    SELECT id, ST_Multi(ST_Transform(geom, 4326)) AS geom
    FROM public.germany_geogitter_inspire_10km;
    CREATE INDEX ON temporal.germany_geogitter_inspire_10km_4326 USING GIST(geom);

    CREATE TABLE temporal.germany_geogitter_inspire_10km_4326_clipped AS
    SELECT 
        g.id, 
        ST_Intersection(g.geom, b.the_geom) AS geom
    FROM 
        temporal.germany_geogitter_inspire_10km_4326 g, 
        public."DEU_adm0" b
    WHERE 
        ST_Intersects(g.geom, b.the_geom);

5. Use GeoGitter 10km -> check if max amount of POIs of specifc category < 20-30k
    decision based on e.g.:

    SELECT 
        g.id AS grid_id,
        COUNT(p.*) AS point_count
    FROM 
        temporal.geogitter_germany_10km g
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
7. loop the geom column of our grid table (temporal.germany_geogitter_inspire_10km_4326_clipped from db_rd)
    7.1 clip POI layer to buffered parts of Germany 
        -> buffer the geoms by 10km (geom is in epsg 4326) and then clip the poi table (schema is always poi, but the table_name we need to get from kart_pois.poi_categories column table_name)
        -> geom to clip POIS = GeoGitter cell with a buffer of 10km as the cycling heatmap uses a cycling speed of 15km/h and the max travel time of the sensitivity curve is 30 min -> 15km/h * 30 min = 7,5km -> use 10 km
    7.2 create file e.g. geopackage to upload it to GOAT project folder + naming logic + store layer id -> check if combination of POI and area was already uploaded -> update or replace file
    7.3 document in ids table -> information of the new layer in the master_thesis_metadata table (id of the geogitter is the reference_clipped_area)
8. for specific folder -> loop over specific layer ids
