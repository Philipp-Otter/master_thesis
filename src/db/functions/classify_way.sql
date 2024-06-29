DROP FUNCTION IF EXISTS public.classify_way;
CREATE OR REPLACE FUNCTION public.classify_way(id integer, excluded_class_id_walking integer[], excluded_class_id_cycling integer[], 
categories_no_foot text[], categories_no_bicycle text[], cycling_surface jsonb)
 RETURNS VOID
AS $$
DECLARE
	rec_way record;
	rec_line record;
BEGIN
	--Select relevant way
	SELECT gid AS id, osm_id, name, tag_id AS class_id, SOURCE, target, 
	one_way, maxspeed_forward, maxspeed_backward, the_geom AS geom, 
	NULL::text AS foot, NULL::text AS bicycle, NULL::float AS sidewalk_both_width, NULL::float AS sidewalk_left_width, NULL::float AS sidewalk_right_width,
	NULL AS highway, NULL::text AS surface, NULL::text AS bicycle_road, NULL::text AS cycleway, NULL::text AS lit, NULL::text AS parking,
	NULL::text AS parking_lane_both, NULL::text parking_lane_right, NULL::text parking_lane_left, NULL::text AS segregated, NULL::text AS sidewalk,
	NULL::text AS smoothness, NULL::text AS wheelchair, NULL::float AS lanes, NULL::integer AS incline_percent, NULL::float AS length_m, NULL::float AS length_3857, 
	NULL::json AS coordinates_3857, NULL::SMALLINT AS crossing_delay_category, NULL::text width, NULL::float AS s_imp, NULL::float AS rs_imp,
	NULL::float AS impedance_surface
	INTO rec_way
	FROM ways 
	WHERE gid = id;
	
	--Select related line from all OSM data
	SELECT p.*
	INTO rec_line
	FROM planet_osm_line p 
	WHERE p.osm_id = rec_way.osm_id;

	--Assign foot and bicycle
	IF rec_line.highway NOT IN('bridleway','cycleway','footway') THEN 
		rec_way.foot = lower(rec_line.foot);		
	END IF;
	
	IF rec_line.width ~ '^[0-9.]*$' THEN 
		rec_way.width = rec_line.width::NUMERIC;
	END IF; 
	IF (rec_line.tags -> 'sidewalk:both:width') ~ '^[0-9.]*$' THEN 
		rec_way.sidewalk_both_width = (rec_line.tags -> 'sidewalk:both:width')::NUMERIC;  
	END IF; 
	IF (rec_line.tags -> 'sidewalk:both:width') ~ '^[0-9.]*$' THEN 
		rec_way.sidewalk_left_width = (rec_line.tags -> 'sidewalk:left:width')::NUMERIC;  
	END IF; 
	IF (rec_line.tags -> 'sidewalk:both:width') ~ '^[0-9.]*$' THEN 
		rec_way.sidewalk_right_width = (rec_line.tags -> 'sidewalk:right:width')::NUMERIC;  
	END IF; 
	rec_way.length_m = ST_LENGTH(rec_way.geom::geography);

	--Check if dem table exists and compute slope profile
	IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dem') THEN
		SELECT c.* 
		INTO rec_way.s_imp, rec_way.rs_imp, rec_way.incline_percent
		FROM get_slope_profile(rec_way.geom, rec_way.length_m, ST_LENGTH(rec_way.geom)) s, 
		LATERAL compute_impedances(s.elevs, s.linklength, s.lengthinterval) c;
	ELSE 
		rec_way.s_imp = NULL;
		rec_way.rs_imp = NULL;
		rec_way.incline_percent = NULL;
	END IF;

	rec_way.bicycle = lower(rec_line.bicycle);
	rec_way.highway = lower(rec_line.highway);
	rec_way.surface = lower(rec_line.surface);
	rec_way.impedance_surface = (cycling_surface ->> rec_way.surface)::float; 
	rec_way.bicycle_road = rec_line.tags -> 'bicycle_road';
	rec_way.cycleway = rec_line.tags -> 'cycleway';
	rec_way.lit = lower(rec_line.tags -> 'lit'); 
	rec_way.parking = rec_line.tags -> 'parking'; 
	rec_way.parking_lane_both = rec_line.tags -> 'parking:lane:both'; 
	rec_way.parking_lane_right = rec_line.tags -> 'parking:lane:right'; 
	rec_way.parking_lane_left = rec_line.tags -> 'parking:lane:left'; 
	rec_way.segregated = rec_line.tags -> 'segregated';
	rec_way.sidewalk = rec_line.tags -> 'sidewalk'; 
	rec_way.smoothness = rec_line.tags -> 'smoothness'; 
	rec_way.wheelchair = lower(rec_line.tags -> 'wheelchair');
	rec_way.lanes = (rec_line.tags -> 'lanes')::NUMERIC;
	rec_way.length_3857 = ST_LENGTH(ST_TRANSFORM(rec_way.geom, 3857));
	rec_way.coordinates_3857 = ST_ASGEOJSON(ST_TRANSFORM(rec_way.geom, 3857))::jsonb -> 'coordinates';

	IF (rec_way.highway = 'living_street' AND rec_way.maxspeed_forward > 7) THEN 
		rec_way.maxspeed_forward = 7;
		rec_way.maxspeed_backward = 7; 
	END IF; 

	IF rec_way.highway = 'stairs' THEN 
		rec_way.wheelchair = 'no';
	END IF; 
	
	IF abs(rec_way.incline_percent) > 6 THEN 
		rec_way.wheelchair = 'no';
	END IF;
	
	IF rec_line.highway = 'service' AND (rec_line.tags -> 'psv' IS NOT NULL OR rec_line.tags -> 'bus' = 'yes') THEN 
		rec_way.foot = 'no';
	END IF; 

	--Check street crossings and ways
	INSERT INTO basic.edge(id, osm_id, name, class_id, SOURCE, target, 
	one_way, maxspeed_forward, maxspeed_backward, geom, foot, bicycle, sidewalk_both_width, sidewalk_left_width, sidewalk_right_width, highway, 
	surface, bicycle_road, cycleway, lit, parking, parking_lane_both, parking_lane_right, parking_lane_left, segregated, sidewalk,
    smoothness, wheelchair, lanes, incline_percent, length_m, length_3857, coordinates_3857, crossing_delay_category, s_imp, rs_imp, impedance_surface)
    SELECT rec_way.id, rec_way.osm_id, rec_way.name, rec_way.class_id, rec_way.SOURCE, rec_way.target, 
	rec_way.one_way, rec_way.maxspeed_forward, rec_way.maxspeed_backward, rec_way.geom, rec_way.foot, rec_way.bicycle, rec_way.sidewalk_both_width, 
	rec_way.sidewalk_left_width, rec_way.sidewalk_right_width, rec_way.highway,	rec_way.surface, rec_way.bicycle_road, rec_way.cycleway, rec_way.lit, 
	rec_way.parking, rec_way.parking_lane_both, rec_way.parking_lane_right, rec_way.parking_lane_left, rec_way.segregated, rec_way.sidewalk,
    rec_way.smoothness, rec_way.wheelchair, rec_way.lanes, rec_way.incline_percent, rec_way.length_m, rec_way.length_3857, rec_way.coordinates_3857, 
    rec_way.crossing_delay_category, rec_way.s_imp, rec_way.rs_imp, rec_way.impedance_surface;
	
	--Insert nodes into node table
	IF (SELECT n.id FROM basic.node n WHERE n.id = rec_way.source) IS NULL THEN  
		INSERT INTO basic.node(id, geom)
		SELECT v.id, v.the_geom
		FROM ways_vertices_pgr v
		WHERE v.id = rec_way.source;
	END IF; 
	IF (SELECT n.id FROM basic.node n WHERE n.id = rec_way.target) IS NULL THEN  
		INSERT INTO basic.node(id, geom)
		SELECT v.id, v.the_geom
		FROM ways_vertices_pgr v
		WHERE v.id = rec_way.target;
	END IF; 

END
$$ LANGUAGE plpgsql;
/*
SELECT classify_way(gid::integer, ARRAY[0,101,102,103,104,105,106,107,501,502,503,504,701,801], 
ARRAY[0,101,102,103,104,105,106,107,501,502,503,504,701,801], ARRAY['use_sidepath','no'], 
ARRAY['use_sidepath','no'], 
'{"paving_stones": 0.2, "sett": 0.3, "unhewn_cobblestone": 0.3, "cobblestone": 0.3, "pebblestone": 0.3, "unpaved": 0.2, "compacted": 0.05, "fine_gravel": 0.05, "gravel": 0.3, "sand": 0.4, "grass": 0.25, "mud": 0.4}'::JSONB
)
FROM ways
LIMIT 10;

*/


