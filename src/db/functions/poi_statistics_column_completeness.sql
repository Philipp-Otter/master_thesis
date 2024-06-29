CREATE OR REPLACE FUNCTION column_completeness(
    p_schema_name TEXT[],
    p_table_name TEXT[],
    p_column_name TEXT[],
    p_category_column_name TEXT[],
    OUT category TEXT,
    OUT column_completeness NUMERIC
)
RETURNS SETOF RECORD
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_sql TEXT;
    v_full_table_name TEXT;
    i INT;
BEGIN
    FOR i IN array_lower(p_schema_name, 1) .. array_upper(p_schema_name, 1)
    LOOP
        v_full_table_name := p_schema_name[i] || '.' || p_table_name[i];

        IF to_regclass(v_full_table_name) IS NULL THEN
            RAISE 'Table % does not exist.', v_full_table_name USING ERRCODE = 'undefined_table';
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = p_schema_name[i] AND table_name = p_table_name[i] AND column_name = p_column_name[i]) THEN
            RAISE 'Column % does not exist in table %.', p_column_name[i], v_full_table_name USING ERRCODE = 'undefined_column';
        END IF;

        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = p_schema_name[i] AND table_name = p_table_name[i] AND column_name = p_category_column_name[i]) THEN
            RAISE 'Category column % does not exist in table %.', p_category_column_name[i], v_full_table_name USING ERRCODE = 'undefined_column';
        END IF;
        
        v_sql := format(
            'SELECT %I AS category,
                    COALESCE(ROUND(100.0 * SUM(CASE WHEN %I IS NOT NULL AND %I <> '''' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2), 0) as non_empty_percentage
             FROM %I.%I
             GROUP BY %I',
            p_category_column_name[i],
            p_column_name[i],
            p_column_name[i],
            p_schema_name[i],
            p_table_name[i],
            p_category_column_name[i]
        );

        RETURN QUERY EXECUTE v_sql;
    END LOOP;
END;
$$;


--how to use
/*SELECT * FROM column_completeness(
    ARRAY['kart_poi_goat'],  -- p_schema_name
    ARRAY['poi'],           -- p_table_name
    ARRAY['operator'],      -- p_column_name
    ARRAY['category']       -- p_category_column_name
)
WHERE category IN ('nursery', 'childcare');*/;
