/*Function that duplicates POIs representing more then one catgory and which applies an offset to them*/
DROP FUNCTION IF EXISTS public.expand_grouped_pois;
CREATE OR REPLACE FUNCTION public.expand_grouped_pois(point geometry, target_categories TEXT[], distance_meters float)
 RETURNS TABLE (category TEXT, geom geometry)
 LANGUAGE plpgsql
AS $function$
DECLARE
	new_category TEXT;
	batch_radians float; 
	i integer;
	arr_new_geoms geometry[] := ARRAY[]::geometry[];
BEGIN
	
	--Order categories to increase the probability that the offset is done similiarly next time
	target_categories = (
		SELECT ARRAY_AGG(cat)
		FROM (
			SELECT cat 
			FROM UNNEST(target_categories) cat 
			ORDER BY cat
		) ordered
	);
		
	--Offset each point by its radians 
	batch_radians = 360 / array_length(target_categories, 1);
	FOR i IN array_lower(target_categories, 1) .. array_upper(target_categories, 1)
	LOOP
		i = i - 1;
		arr_new_geoms = array_append(arr_new_geoms, ST_Project(point::geometry, distance_meters, radians(i*batch_radians))); 
	END LOOP;
    RETURN QUERY SELECT UNNEST(target_categories) category, UNNEST(arr_new_geoms) geom; 
END;
$function$;

/*
SELECT e.*
FROM expand_grouped_pois(ST_MAKEPOINT(11.333, 48.123), ARRAY['test', 'test1', 'test3', 'test4']::TEXT[], 0.5) e  
*/