DROP FUNCTION IF EXISTS create_equal_area_split_polygon;
CREATE OR REPLACE FUNCTION create_equal_area_split_polygon(input_geom geometry, area_size integer)
 RETURNS SETOF geometry
AS $$
DECLARE
	area_geom float := ST_AREA(input_geom::geography)::float / 100000000;
	count_clusters integer := area_geom / area_size::float;
BEGIN

	RETURN query 
	WITH voronois AS 
	(
		SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(c.geom)))).geom
		FROM 
		(
			SELECT ST_Centroid(ST_collect(g.geom)) AS geom 
			FROM 
			(	
				SELECT geom, ST_ClusterKMeans(p.geom, count_clusters) OVER() AS classes
				FROM (SELECT (ST_Dump(ST_GeneratePoints(input_geom, area_geom::integer * 20))).geom AS geom) p 
			) g 
			GROUP BY classes
		) c
	)
	SELECT ST_Intersection(input_geom, v.geom)
	FROM voronois v; 
	
END
$$ LANGUAGE plpgsql;

/*SELECT ST_MULTI(create_equal_area_split_polygon(b.geom, 1))
FROM boundaries b*/