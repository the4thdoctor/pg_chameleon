-- DROP TABLE IF EXISTS is supported by both MySQL and PostgreSQL
-- but we want to check if it works when a table only exists in MySQL,
-- or it only works in PostgreSQL.
-- Note that MySQL always replicates IF EXISTS statements.

-- Create table in MySQL, then drop it in PostgreSQL
-- and test DROP IF EXISTS
SET SESSION sql_log_bin = 0;
CREATE TABLE exists_mysql (
	id INT PRIMARY KEY
);
SET SESSION sql_log_bin = 1;
DROP TABLE IF EXISTS exists_mysql;

-- Create the table in both DBMSs, then drop it in MySQL
-- and test DROP IF EXISTS
CREATE TABLE exists_postgresql (
	id INT PRIMARY KEY
);
SET SESSION sql_log_bin = 0;
DROP TABLE exists_postgresql;
SET SESSION sql_log_bin = 1;
DROP TABLE IF EXISTS exists_postgresql;
