
select * from information_schema.key_column_usage where table_schema='sakila' and referenced_table_name is not null
order by table_name,column_name,ordinal_position