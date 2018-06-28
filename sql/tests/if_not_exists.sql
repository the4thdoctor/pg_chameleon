-- CREATE TABLE IF NOT EXISTS is supported by both MySQL and PostgreSQL
-- but we want to check if it works when a table only exists in MySQL,
-- or it only works in PostgreSQL.
-- Note that MySQL always replicates IF [NOT] EXISTS statements.

-- Create table in MySQL only,
-- then test CREATE TABLE IF NOT EXISTS.
SET SESSION sql_log_bin = 0;
CREATE TABLE exists_mysql (
    id INT PRIMARY KEY
);
SET SESSION sql_log_bin = 1;
CREATE TABLE IF NOT EXISTS exists_mysql (
    id INT PRIMARY KEY
);

-- Create table in MySQL and PostgreSQL, then drop it in MySQL
-- and test CREATE TABLE IF NOT EXISTS.
CREATE TABLE exists_postgresql (
    id INT PRIMARY KEY
);
SET SESSION sql_log_bin = 0;
DROP TABLE exists_postgresql;
SET SESSION sql_log_bin = 1;
CREATE TABLE IF NOT EXISTS exists_postgresql (
    id INT PRIMARY KEY
);
