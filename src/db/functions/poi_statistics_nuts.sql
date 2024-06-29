DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'poi_data_type') THEN
        CREATE TYPE poi_data_type AS (
            nuts_id TEXT,
            poi_count INTEGER,
            total_population INTEGER,
            geom TEXT,
            area_km2 NUMERIC,
            density NUMERIC,
            per_population NUMERIC
        );
    END IF;
END $$;
CREATE OR REPLACE FUNCTION poi_statistics_nuts(poi_category TEXT)
RETURNS SETOF poi_data_type AS $$
DECLARE
    rec RECORD;
    area_km2 NUMERIC;
    density NUMERIC;
    per_population NUMERIC;
    r poi_data_type;
BEGIN
    FOR rec IN (
        SELECT 
            n.nuts_id, 
            COUNT(p.*) as poi_count, 
            n.total_population,
            ST_AsGeoJSON(n.geom) as geom
        FROM public.nuts_population_germany n
        LEFT JOIN kart_poi_goat.poi p ON ST_Intersects(p.geom, n.geom) AND p.category = poi_category
        WHERE n.levl_code = 3 AND n.total_population IS NOT NULL AND n.total_population > 0
        GROUP BY nuts_id, n.total_population, n.geom
    )
    LOOP
        IF rec.total_population IS NOT NULL AND rec.total_population > 0 THEN
            area_km2 := ST_Area(ST_Transform(ST_SetSRID(rec.geom::geometry, 4326), 3857)) / 1000000;
            density := ROUND(rec.poi_count / area_km2, 3);
            per_population := ROUND(rec.poi_count / rec.total_population * 10000, 6);
           --per_population calculation does not properly work for nuts
            r := (rec.nuts_id, rec.poi_count, rec.total_population, rec.geom, area_km2, density, per_population);
            RETURN NEXT r;
        END IF;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;


--how to use
--SELECT poi_statistics_nuts('childcare');
