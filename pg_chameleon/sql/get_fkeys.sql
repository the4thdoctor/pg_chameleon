SELECT 
    table_name,
    constraint_name,
    referenced_table_name,
    referenced_table_schema,
    GROUP_CONCAT(concat('"',column_name,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as fk_cols,
    GROUP_CONCAT(concat('"',REFERENCED_COLUMN_NAME,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as ref_columns
FROM 
    information_schema.key_column_usage 
WHERE 
        table_schema='obfuscated_dummy' 
    AND referenced_table_name IS NOT NULL
GROUP BY 
    table_name,
    constraint_name,
    referenced_table_name,
    referenced_table_schema
ORDER BY 
    table_name,
    constraint_name,
    ordinal_position
;