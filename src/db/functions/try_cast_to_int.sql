DROP FUNCTION IF EXISTS try_cast_to_int;
CREATE OR REPLACE FUNCTION try_cast_to_int(input_value TEXT)
RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    BEGIN
        result := input_value::FLOAT::INTEGER;
        RETURN result;
    EXCEPTION WHEN invalid_text_representation THEN
        RETURN NULL;
    END;
END;
$$ LANGUAGE plpgsql;