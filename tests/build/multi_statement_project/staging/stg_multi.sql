-- Pre-statement: create a temp table for staging
CREATE TEMP TABLE tmp_raw_data AS SELECT 1 AS id, 'alice' AS name;

-- This is the model query
SELECT id, name FROM tmp_raw_data;

-- Post-statement: clean up
DROP TABLE IF EXISTS tmp_raw_data;
