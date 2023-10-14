--
-- Const merging functionality
--
CREATE EXTENSION pg_stat_statements;

CREATE TABLE test_merge (id int, data int);

-- IN queries

-- No merging is performed, as a baseline result
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- Normal scenario, too many simple constants for an IN query
SET query_id_const_merge = on;

SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1);
SELECT * FROM test_merge WHERE id IN (1, 2, 3);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- More conditions in the query
SELECT pg_stat_statements_reset();

SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9) and data = 2;
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) and data = 2;
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) and data = 2;
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- No constants simplification
SELECT pg_stat_statements_reset();

SELECT * FROM test_merge WHERE id IN (1 + 1, 2 + 2, 3 + 3, 4 + 4, 5 + 5, 6 + 6, 7 + 7, 8 + 8, 9 + 9);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- Numeric type
CREATE TABLE test_merge_numeric (id int, data numeric(5, 2));
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge_numeric WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- Test constants evaluation, verifies a tricky part to make sure there are no
-- issues in the merging implementation
WITH cte AS (
    SELECT 'const' as const FROM test_merge
)
SELECT ARRAY['a', 'b', 'c', const::varchar] AS result
FROM cte;

RESET query_id_const_merge;
