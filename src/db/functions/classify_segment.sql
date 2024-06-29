DROP TYPE IF EXISTS output_segment CASCADE;
CREATE TYPE output_segment AS (
	id text, length_m float8, length_3857 float8,
	osm_id int8, bicycle text, foot text,
    class_ text, impedance_slope float8,
	impedance_slope_reverse float8,
	impedance_surface float8,
    coordinates_3857 json, maxspeed_forward integer,
	maxspeed_backward integer, "source" text,
	source_index integer, target text,
	target_index integer, tags jsonb,
    geom public.geometry(linestring, 4326),
    h3_3 integer, h3_6 integer
);


DROP FUNCTION IF EXISTS public.classify_segment;
CREATE OR REPLACE FUNCTION public.classify_segment(
	segment_id TEXT,
	cycling_surfaces JSONB
)
RETURNS VOID
AS $$
DECLARE
	input_segment record;
	new_sub_segment output_segment;
	output_segment output_segment;
	split_geometry record;

	sub_segments output_segment[] = '{}';
	output_segments output_segment[] = '{}';
	
	source_conn_geom public.geometry(point, 4326);
	target_conn_geom public.geometry(point, 4326);
BEGIN
	-- Select relevant input segment
	SELECT id, subtype, connectors, geometry,
		road::jsonb->>'class' AS class,
		road::jsonb->'roadNames' AS roadNames,
		road::jsonb->'surface' AS surface,
		road::jsonb->'flags' AS flags,
		road::jsonb->'restrictions' AS restrictions,
		road::jsonb AS allData
	INTO input_segment
	FROM temporal.segments
	WHERE id = segment_id;

	-- Check if segment needs to be split into sub-segments
	IF array_length(input_segment.connectors, 1) > 2 THEN
		-- Split segment into sub-segments
		FOR i IN 2..array_length(input_segment.connectors, 1) LOOP
			-- Initialize sub-segment primary properties
			new_sub_segment.id = input_segment.id || '_sub_' || i-1;
			SELECT geometry INTO source_conn_geom FROM temporal.connectors WHERE id = input_segment.connectors[i-1];
			SELECT geometry INTO target_conn_geom FROM temporal.connectors WHERE id = input_segment.connectors[i];
			new_sub_segment.geom = ST_LineSubstring(
				input_segment.geometry,
				ST_LineLocatePoint(input_segment.geometry, source_conn_geom),
				ST_LineLocatePoint(input_segment.geometry, target_conn_geom)
			);
			new_sub_segment.source = input_segment.connectors[i-1];
			new_sub_segment.target = input_segment.connectors[i];

			-- TODO Handle linear split surface for sub-segment
			-- TODO Handle linear split speed limits for sub-segment
			-- TODO Handle linear split flags for sub-segment

			sub_segments = array_append(sub_segments, new_sub_segment);
		END LOOP;
	ELSE
		-- Initialize segment primary properties
		new_sub_segment.id = input_segment.id;
		new_sub_segment.geom = input_segment.geometry;
		new_sub_segment.source = input_segment.connectors[1];
		new_sub_segment.target = input_segment.connectors[2];

		-- TODO Handle linear split surface for segment
		-- TODO Handle linear split speed limits for segment
		-- TODO Handle linear split flags for segment

		sub_segments = array_append(sub_segments, new_sub_segment);
	END IF;
	
	-- Clip sub-segments to fit into h3_3 and h3_6 cells
	SELECT clip_segments(sub_segments, 6) INTO output_segments;
	SELECT clip_segments(output_segments, 3) INTO output_segments;

	-- Loop through final output segments
	FOREACH output_segment IN ARRAY output_segments LOOP
		-- Set remaining properties for every output segment, these are derived from primary properties
		output_segment.length_m = ST_Length(output_segment.geom::geography);
		output_segment.length_3857 = ST_Length(ST_Transform(output_segment.geom, 3857));
		output_segment.coordinates_3857 = (ST_AsGeoJson(ST_Transform(output_segment.geom, 3857))::jsonb)['coordinates'];
		output_segment.osm_id = NULL;
		output_segment.class_ = input_segment.class;
		output_segment.h3_3 = to_short_h3_3(h3_lat_lng_to_cell(ST_Centroid(output_segment.geom)::point, 3)::bigint);
		output_segment.h3_6 = to_short_h3_6(h3_lat_lng_to_cell(ST_Centroid(output_segment.geom)::point, 6)::bigint);

		-- Temporarily set the following properties here, but evetually handle linear split values above
		IF jsonb_typeof(input_segment.surface) != 'array' THEN
			output_segment.impedance_surface = (cycling_surfaces ->> (input_segment.allData ->> 'surface'))::float;
		END IF;
		output_segment.maxspeed_forward = ((input_segment.restrictions -> 'speedLimits') -> 'maxSpeed')[0];
		output_segment.tags = input_segment.flags;

		-- Check if digital elevation model (DEM) table exists and compute impedance values
		-- IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dem') THEN
		-- 	SELECT c.* 
		-- 	INTO output_segment.impedance_slope, output_segment.impedance_slope_reverse
		-- 	FROM get_slope_profile(output_segment.geom, output_segment.length_m, ST_LENGTH(output_segment.geom)) s, 
		-- 	LATERAL compute_impedances(s.elevs, s.linklength, s.lengthinterval) c;
		-- END IF;

		-- Insert processed output segment data into table
        INSERT INTO basic.segment (
                length_m, length_3857,
				osm_id, bicycle, foot,
                class_, impedance_slope, impedance_slope_reverse,
				impedance_surface, coordinates_3857, maxspeed_forward,
				maxspeed_backward, source, target,
				tags, geom, h3_3, h3_6
        )
        VALUES (
			output_segment.length_m, output_segment.length_3857,
			output_segment.osm_id, output_segment.bicycle, output_segment.foot,
			output_segment.class_, output_segment.impedance_slope, output_segment.impedance_slope_reverse,
			output_segment.impedance_surface, output_segment.coordinates_3857, output_segment.maxspeed_forward,
			output_segment.maxspeed_backward, output_segment.source_index, output_segment.target_index,
			output_segment.tags, output_segment.geom, output_segment.h3_3, output_segment.h3_6
        );
    END LOOP;
END
$$ LANGUAGE plpgsql;
