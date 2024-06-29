/*From https://github.com/igor-suhorukov/openstreetmap_h3*/
DROP FUNCTION IF EXISTS to_short_h3_3;
CREATE FUNCTION to_short_h3_3(bigint) RETURNS integer
AS $$ select ($1 & 'x000ffff000000000'::bit(64)::bigint>>36)::integer;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;