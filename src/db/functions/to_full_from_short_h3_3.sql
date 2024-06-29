/*From https://github.com/igor-suhorukov/openstreetmap_h3*/
DROP FUNCTION IF EXISTS to_full_from_short_h3_3;
CREATE FUNCTION to_full_from_short_h3_3(smallint) RETURNS bigint
AS $$ select (($1::bigint::bit(64) & 'x000000000000ffff'::bit(64))<<36 | 'x0830000fffffffff'::bit(64))::bigint;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;