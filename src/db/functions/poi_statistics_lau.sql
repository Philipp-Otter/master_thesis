DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'poi_data_type') THEN
        CREATE TYPE poi_data_type AS (
            lau_id TEXT,
            poi_count INTEGER,
            pop_2021 INTEGER,
            geom TEXT,
            area_km2 NUMERIC,
            density NUMERIC,
            per_population NUMERIC
        );
    END IF;
END $$;
CREATE OR REPLACE FUNCTION poi_statistics_lau(poi_category TEXT)
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
            n.lau_id, 
            COUNT(p.*) as poi_count, 
            n.pop_2021,
            ST_AsGeoJSON(n.geom) as geom
        FROM public.lau_germany n
        LEFT JOIN kart_poi_goat.poi p ON ST_Intersects(p.geom, n.geom) AND p.category = poi_category
        WHERE n.pop_2021 IS NOT NULL AND n.pop_2021 > 0
        GROUP BY lau_id, n.pop_2021, n.geom
    )
    LOOP
        IF rec.pop_2021 IS NOT NULL AND rec.pop_2021 > 0 THEN
            area_km2 := ST_Area(ST_Transform(ST_SetSRID(rec.geom::geometry, 4326), 3857)) / 1000000;
            density := ROUND(rec.poi_count / area_km2, 3);
            per_population := ROUND(rec.poi_count / rec.pop_2021 * 10000, 6);

            r := (rec.lau_id, rec.poi_count, rec.pop_2021, rec.geom, area_km2, density, per_population);
            RETURN NEXT r;
        END IF;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
;


--how to use
--SELECT poi_statistics_lau('childcare')