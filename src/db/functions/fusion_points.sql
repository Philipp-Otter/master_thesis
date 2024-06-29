DROP FUNCTION IF EXISTS fusion_points();
CREATE OR REPLACE FUNCTION fusion_points(
    name_table_1 text,
    name_table_2 text,
    fusion_radius float,
    similarity_threshold float,
    comparison_column_table_1 text,
    comparison_column_table_2 text,
    decision_table_1 text,
    decision_fusion text,
    decision_table_2 text,
    source_table_input_1 text
)
RETURNS void
AS $function$
BEGIN
    DROP TABLE IF EXISTS temporal.comparison_poi; 
    EXECUTE '
        CREATE TABLE temporal.comparison_poi AS 
        SELECT n.*, (ARRAY_AGG(matching_key_input_2))[1] AS matching_key_input_2, (ARRAY_AGG(similarity))[1] AS similarity
        FROM ' || name_table_1 || ' n
        LEFT JOIN LATERAL 
        (   
            SELECT y.matching_key_input_2, similarity(lower(y.' || comparison_column_table_2 || '), lower(n.' || comparison_column_table_1 || ')) AS similarity
            FROM 
            (
                SELECT p.matching_key_input_2, lower(' || comparison_column_table_2 || ') AS name 
                FROM ' || name_table_2 || ' p 
                WHERE ST_DWithin(n.geom, p.geom, ' || fusion_radius || ')
            ) y 
            WHERE similarity(y.name, n.' || comparison_column_table_1 || ') >= ' || similarity_threshold || '
            ORDER BY similarity(y.name, n.' || comparison_column_table_1 || ')
            DESC
            LIMIT 1
        ) j ON TRUE 
        GROUP BY n.id 
    ';

    CREATE INDEX ON temporal.comparison_poi (id);
    CREATE INDEX ON temporal.comparison_poi (matching_key_input_2); 

    ALTER TABLE temporal.comparison_poi 
    ADD COLUMN decision varchar;

    -- Use separate IF and ELSIF statements for different decisions
    IF decision_table_1 = 'keep' THEN
        EXECUTE '
            UPDATE temporal.comparison_poi 
            SET decision = ''keep''
            WHERE matching_key_input_2 IS NULL;
        ';
    ELSIF decision_table_1 = 'drop' THEN
        EXECUTE '
            DELETE FROM temporal.comparison_poi 
            WHERE matching_key_input_2 IS NULL;
        ';
    END IF;

    IF decision_fusion = 'keep' THEN
        EXECUTE '
            UPDATE temporal.comparison_poi 
            SET decision = ''keep''
            WHERE similarity >= ' || similarity_threshold || ';
        ';
    ELSIF decision_fusion = 'combine' THEN
        -- Inside the "combine" section
        -- Merge columns, handling JSONB or non-JSONB
        EXECUTE '
            UPDATE temporal.comparison_poi AS c
            SET 
                ' || (
                    SELECT string_agg(column_name || ' = COALESCE(t2.' || column_name || ', c.' || column_name || ')', ', ')
                    FROM information_schema.columns
                    WHERE table_name = name_table_1
                    AND column_name != 'id'
                ) || ',
                decision = ''combined''
            FROM ' || name_table_2 || ' AS t2
            WHERE c.matching_key_input_2 = t2.matching_key_input_2 
            AND similarity >= ' || similarity_threshold || ';
        ';
    ELSIF decision_fusion = 'replace' THEN
        -- Inside the "replace" section
        -- Replace columns, handling JSONB or non-JSONB
        EXECUTE '
            UPDATE temporal.comparison_poi AS c
            SET 
                ' || (
                    SELECT string_agg(column_name || ' = COALESCE(t2.' || column_name || ', c.' || column_name || ')', ', ')
                    FROM information_schema.columns
                    WHERE table_name = name_table_1
                    AND column_name != 'id'
                ) || ',
                decision = ''replaced''
            FROM ' || name_table_2 || ' AS t2
            WHERE c.matching_key_input_2 = t2.matching_key_input_2 
            AND similarity >= ' || similarity_threshold || ';
        ';
    END IF;

    IF decision_table_2 = 'add' THEN
        EXECUTE '
            INSERT INTO temporal.comparison_poi
            SELECT DISTINCT ON (a.id) a.*, matching_key_input_2, NULL AS similarity, ''add''::varchar AS decision
            FROM ' || name_table_2 || ' a
            WHERE NOT EXISTS (
                SELECT 1
                FROM ' || source_table_input_1 || ' s
                WHERE ST_DWithin(a.geom, s.geom, ' || fusion_radius || ')
                AND similarity(lower(s.' || comparison_column_table_1 || '), lower(a.' || comparison_column_table_2 || ')) >= ' || similarity_threshold || '
            )
            ORDER BY a.id
        ';

    ELSIF decision_table_2 = 'drop' THEN
        -- Do nothing or add any necessary logic
        NULL;
    END IF;

END;
$function$ LANGUAGE plpgsql;


-- /*TODO: add example*/


--- latest version, but needs more testing


-- DROP FUNCTION IF EXISTS fusion_points();
-- CREATE OR REPLACE FUNCTION fusion_points(
--     name_table_1 text,
--     name_table_2 text,
--     fusion_radius float,
--     similarity_threshold float,
--     comparison_column_table_1 text,
--     comparison_column_table_2 text,
--     decision_table_1 text,
--     decision_fusion text,
--     decision_table_2 text
-- )
-- RETURNS void
-- AS $function$
-- BEGIN
--     DROP TABLE IF EXISTS temporal.comparison_poi; 
--     EXECUTE '
--         CREATE TABLE temporal.comparison_poi AS 
--         SELECT n.*, (ARRAY_AGG(matching_key_input_2))[1] AS matching_key_input_2, (ARRAY_AGG(similarity))[1] AS similarity
--         FROM ' || name_table_1 || ' n
--         LEFT JOIN LATERAL 
--         (   
--             SELECT y.matching_key_input_2, similarity(lower(y.' || comparison_column_table_2 || '), lower(n.' || comparison_column_table_1 || ')) AS similarity
--             FROM 
--             (
--                 SELECT p.matching_key_input_2, lower(' || comparison_column_table_2 || ') AS name 
--                 FROM ' || name_table_2 || ' p 
--                 WHERE ST_DWithin(n.geom, p.geom, ' || fusion_radius || ')
--             ) y 
--             WHERE similarity(y.name, n.' || comparison_column_table_1 || ') >= ' || similarity_threshold || '
--             ORDER BY similarity(y.name, n.' || comparison_column_table_1 || ')
--             DESC
--             LIMIT 1
--         ) j ON TRUE 
--         GROUP BY n.id, n.category
--     ';

--     CREATE INDEX ON temporal.comparison_poi (id); 

--     ALTER TABLE temporal.comparison_poi 
--     ADD COLUMN decision varchar;

--     -- Use separate IF and ELSIF statements for different decisions
--     IF decision_table_1 = 'keep' THEN
--         EXECUTE '
--             UPDATE temporal.comparison_poi 
--             SET decision = ''keep''
--             WHERE matching_key_input_2 IS NULL;
--         ';
--     ELSIF decision_table_1 = 'drop' THEN
--         EXECUTE '
--             DELETE FROM temporal.comparison_poi 
--             WHERE matching_key_input_2 IS NULL;
--         ';
--     END IF;

--     IF decision_fusion = 'keep' THEN
--         EXECUTE '
--             UPDATE temporal.comparison_poi 
--             SET decision = ''keep''
--             WHERE similarity >= ' || similarity_threshold || ';
--         ';
--     ELSIF decision_fusion = 'combine' THEN
--         -- Inside the "combine" section
--         -- Merge columns, handling JSONB or non-JSONB
--         EXECUTE '
--             UPDATE temporal.comparison_poi AS c
--             SET 
--                 ' || (
--                     SELECT string_agg(
--                         CASE 
--                             WHEN column_name = 'source' THEN 'source = t2.source || ''_'' || c.source'
--                             ELSE column_name || ' = COALESCE(t2.' || column_name || ', c.' || column_name || ')'
--                         END, ', '
--                     )
--                     FROM information_schema.columns
--                     WHERE table_name = split_part(name_table_1, '.', 2)
--                     AND table_schema = split_part(name_table_1, '.', 1)
--                     AND column_name NOT IN ('id', 'matching_key_input_1', 'matching_key_input_2')
--                 ) || ',
--                 decision = ''combined''
--             FROM ' || name_table_2 || ' AS t2
--             WHERE c.matching_key_input_2 = t2.matching_key_input_2 
--             AND similarity >= ' || similarity_threshold || ';
--         ';
--     ELSIF decision_fusion = 'replace' THEN
--         -- Inside the "replace" section
--         -- Replace columns, handling JSONB or non-JSONB
--         EXECUTE '
--             UPDATE temporal.comparison_poi AS c
--             SET 
--                 ' || (
--                     SELECT string_agg(column_name || ' = COALESCE(t2.' || column_name || ', c.' || column_name || ')', ', ')
--                     FROM information_schema.columns
--                     WHERE table_name = split_part(name_table_1, '.', 2)
--                     AND table_schema = split_part(name_table_1, '.', 1)
--                     AND column_name NOT IN ('id', 'matching_key_input_1', 'matching_key_input_2')
--                 ) || ',
--                 decision = ''replaced''
--             FROM ' || name_table_2 || ' AS t2
--             WHERE c.matching_key_input_2 = t2.matching_key_input_2 
--             AND similarity >= ' || similarity_threshold || ';
--         ';
--     END IF;

--     IF decision_table_2 = 'add' THEN
--         EXECUTE '
--             INSERT INTO temporal.comparison_poi
--             SELECT *, matching_key_input_2, NULL as similarity ,''add''::varchar AS decision
--             FROM ' || name_table_2 || '
--             WHERE matching_key_input_2 NOT IN (SELECT DISTINCT matching_key_input_2 FROM temporal.comparison_poi WHERE matching_key_input_2 IS NOT NULL);
--         ';
--     ELSIF decision_table_2 = 'drop' THEN
--         -- Do nothing or add any necessary logic
--         NULL;
--     END IF;

-- END;
-- $function$ LANGUAGE plpgsql;


-- -- /*TODO: add example*/