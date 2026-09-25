-- Intentionally invalid: the `run --json` test selects this model to assert the
-- error payload and the non-zero exit code. Never select it in a passing run.
SELECT * FROM table_that_does_not_exist
