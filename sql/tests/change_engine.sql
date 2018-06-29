-- MySQL supports different storage engines, PostgreSQL doesn't.
-- The ALTER TABLE to change a table engine should be ignored.

CREATE TABLE test_engine (
	id INT PRIMARY KEY
) ENGINE MEMORY;
INSERT INTO test_engine (id) VALUES (100);
ALTER TABLE test_engine ENGINE InnoDB;
