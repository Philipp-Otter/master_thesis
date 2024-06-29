-- Function that snaps a polygon to a reference border consisting of other polygons
DROP FUNCTION IF EXISTS basic.polygon_snap_to_border;
CREATE OR REPLACE FUNCTION basic.polygon_snap_to_border(geom_to_expand geometry, geoms_border geometry[], snapping_distance integer) 
RETURNS geometry
AS $$
DECLARE 
	union_border geometry; 
	snapped_geom geometry;
BEGIN 
    
	-- Union border geoms 
	union_border = (SELECT ST_UNION(geom) FROM UNNEST(geoms_border) geom); 	

	-- Snap geom to border by expanding by snapping distance
	snapped_geom = ST_DIFFERENCE(ST_BUFFER(ST_Difference(geom_to_expand, union_border)::geography, snapping_distance)::geometry, union_border); 
	
	-- Remove very small remaining polygons  < 1 m²
	snapped_geom = (WITH dumped AS 
	(
		SELECT (ST_DUMP(snapped_geom)).geom
	)
	SELECT ST_COLLECT(geom)
	FROM dumped
	WHERE ST_AREA(geom::geography) > 1);  
	
    -- Return result
    RETURN snapped_geom;

END; 
$$ LANGUAGE plpgsql;

/* Example for study areas
WITH to_snap AS 
(
	SELECT s.id, geom  
	FROM basic.sub_study_area s
	WHERE s.id = 4097
),
selected_polygons AS 
(
	SELECT f.id, f.geom, ARRAY_AGG(s.geom) geoms_border
	FROM basic.sub_study_area s, to_snap f
	WHERE ST_Intersects(s.geom, ST_BUFFER(f.geom::geography, 200)::geometry)
	AND f.id <> s.id
	GROUP BY f.geom, f.id 
)
SELECT b.id, basic.polygon_snap_to_border(geom, geoms_border, 200) 
FROM selected_polygons b 
 */