CREATE EXTENSION test_undoread;

CREATE TABLE input (
	id	serial	PRIMARY KEY,
	value	text	NOT NULL,
	ptr	text
);

INSERT INTO input(value) VALUES ('one'), ('two'), ('three');

-- Write the data into the UNDO log and update the pointers in the table.
BEGIN;
SELECT test_undoread_create();
UPDATE	input
SET ptr = test_undoread_insert(value);
SELECT test_undoread_close();
COMMIT;

CREATE TABLE output (
	id	serial	PRIMARY KEY,
	value	text	NOT NULL
);

-- Read the data. Note that the last pointer should not be included in the
-- result.
INSERT INTO output(value)
SELECT v
FROM test_undoread_read(
	(SELECT ptr FROM input WHERE id = (SELECT min(id) FROM input)),
	(SELECT ptr FROM input WHERE id = (SELECT max(id) FROM input))) r(v);

-- Check that output data match the input.
SELECT i.id, i.value, o.id, o.value
FROM input i
FULL JOIN output o USING(value)
WHERE i.value ISNULL or o.value ISNULL;
