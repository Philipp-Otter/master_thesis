DROP FUNCTION IF EXISTS get_id_for_max_val; 
CREATE OR REPLACE FUNCTION get_id_for_max_val(arr_val integer[], arr_id bigint[]) 
RETURNS bigint
AS $$
DECLARE 
    arr_index integer; 
BEGIN 
    arr_index = arr_val # (sort_desc(arr_val))[1];
    RETURN arr_id[arr_index];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

--SELECT get_id_for_max_val(ARRAY[300,10,30000],ARRAY[33,12,88])