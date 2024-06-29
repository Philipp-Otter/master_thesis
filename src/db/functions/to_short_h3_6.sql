/*Adaptation of https://github.com/igor-suhorukov/openstreetmap_h3*/
DROP FUNCTION IF EXISTS to_short_h3_6;
CREATE FUNCTION to_short_h3_6(bigint) RETURNS int
AS $$ select ($1 & 'x000fffffff000000'::bit(64)::bigint>>24)::bit(32)::int;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;