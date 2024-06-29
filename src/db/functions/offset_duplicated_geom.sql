DROP FUNCTION IF EXISTS public.offset_duplicated_geoms;
CREATE OR REPLACE FUNCTION public.offset_duplicated_geoms(uids TEXT[], points geometry[], distance_meters double precision)
 RETURNS TABLE(uid text, geom geometry)
 LANGUAGE plpgsql
AS $function$
DECLARE
	batch_radians float; 
	i integer;
	arr_new_geoms geometry[] := ARRAY[]::geometry[];
	point geometry; 
BEGIN
		
	--Offset each point by its radians 
	batch_radians = 360 / array_length(uids, 1);
	FOR i IN array_lower(uids, 1) .. array_upper(uids, 1)
	LOOP
		
		point = points[i];
		i = i - 1;
		arr_new_geoms = array_append(arr_new_geoms, ST_Project(point::geometry, distance_meters, radians(i*batch_radians))); 
		
	END LOOP;
    RETURN QUERY SELECT UNNEST(uids) uid, UNNEST(arr_new_geoms) geom; 
END;
$function$;

/*DROP TABLE temporal.test; 
CREATE TABLE temporal.test AS 
WITH x AS 
(
	SELECT count(*), ARRAY_AGG(uid) uids, ARRAY_AGG(geom) geoms 
	FROM kart_poi_goat.poi 
	GROUP BY geom 
	HAVING count(*) > 1
)
SELECT o.*
FROM x, LATERAL offset_duplicated_geoms(x.uids, x.geoms, 2) o; 
*/