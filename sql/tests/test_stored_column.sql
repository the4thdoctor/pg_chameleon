-- Test creation of a table with a VIRTUAL column and 1 row INSERTed.

CREATE TABLE test_virtual_col (
    id INT PRIMARY KEY,
    v INT GENERATED ALWAYS AS (id + 1) STORED NOT NULL
) ENGINE InnoDB;
INSERT INTO test_virtual_col (id) VALUES (100);
