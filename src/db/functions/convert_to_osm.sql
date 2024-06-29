CREATE OR REPLACE FUNCTION basic.convert_way_to_osm(start_osm_id_node integer, osm_id_way integer, geom geometry, tags jsonb) 
RETURNS TABLE (max_node_id integer, nodes TEXT[], way TEXT)
AS $$
DECLARE 
    nodes_coords float[][2]; 
    nodes_length integer; 
    nodes_ids integer[];
    max_node_id integer;
    node_texts TEXT[];
    way_text TEXT;
    tags_keys TEXT[];
    tags_values TEXT[];
BEGIN 
    
    -- Get coordinates of nodes
    nodes_coords = ARRAY(
        SELECT ARRAY[(coords -> 1)::float, (coords -> 0)::float]        
        FROM jsonb_array_elements(st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates') coords
    );

    -- Get length of nodes array
    nodes_length = array_length(nodes_coords, 1);

    nodes_ids = (
        SELECT ARRAY_AGG(ids) 
        FROM generate_series(start_osm_id_node, start_osm_id_node + nodes_length - 1) ids
    );

    -- Get max node id
    max_node_id = nodes_ids[nodes_length];
    
    -- Convert nodes to text
    node_texts = ARRAY(
        SELECT format('<node id="%s" lat="%s" lon="%s"/>', nodes_ids[i], nodes_coords[i][1], nodes_coords[i][2]) 
        FROM generate_series(1, nodes_length) AS i
    );

    -- Convert way to text
    way_text = format('<way id="%s">%s', osm_id_way, (
        SELECT array_to_string(ARRAY(
            SELECT format('<nd ref="%s"/>', nodes_ids[i]) 
            FROM generate_series(1, nodes_length) AS i
        ), '')
    ));

    -- Get tags keys and values
    tags_keys = ARRAY(SELECT jsonb_object_keys(tags));
    tags_values = ARRAY(SELECT tags -> key FROM jsonb_object_keys(tags) key);

    -- Check if tags_keys is not empty
    IF array_length(tags_keys, 1) IS NOT NULL THEN
        -- Construct tags part
        FOR i IN 1..array_length(tags_keys, 1)
        LOOP
            way_text = way_text || format('<tag k="%s" v="%s"/>', tags_keys[i], replace(tags_values[i], '"', ''));
        END LOOP;
    END IF;
    
    way_text = way_text || '</way>';

    -- Return result
    RETURN QUERY SELECT start_osm_id_node + nodes_length AS max_node_id, node_texts, way_text;

END; 
$$ LANGUAGE plpgsql;