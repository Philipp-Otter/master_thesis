DROP FUNCTION IF EXISTS public.split_street_housenumber;
CREATE OR REPLACE FUNCTION public.split_street_housenumber(address text)
 RETURNS text[]
 LANGUAGE plpgsql
AS $function$
DECLARE
	street text; 
    housenumber text;
    arr_length integer; 
   	splitted_street text[];
BEGIN
    -- remove any trailing whitespace from the address
    address = trim(both from address);
    -- use regex to find the first sequence of characters and numbers
    -- in the address and save it as the housenumber
    SELECT (regexp_split_to_array(address, '^[^0-9]*'))[2] INTO housenumber;
    SELECT replace(address, housenumber, '') INTO street; 
   
    IF (string_to_array(street, ' ')) IS NULL THEN 
    	splitted_street = string_to_array(street, ' '); 
    	arr_length = ARRAY_LENGTH(splitted_street, 1); 
    	SELECT ARRAY_TO_STRING(splitted_street[1:arr_length-1], ' ') INTO street; 
    END IF; 

    RETURN ARRAY[street, housenumber];
END;
$function$;
