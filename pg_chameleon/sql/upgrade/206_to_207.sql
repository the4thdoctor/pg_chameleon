-- upgrade catalogue script 2.0.6 to 2.0.7
--TABLES
CREATE TABLE sch_chameleon.t_indexes 
    (
        i_id_index	bigserial,
        i_id_table	bigint NOT NULL,
        v_index_name varchar NOT NULL,
        t_index_drop text NULL,
        t_index_create text NULL,
        CONSTRAINT pk_t_indexes PRIMARY KEY (i_id_index),
        CONSTRAINT fk_t_indexes_t_table FOREIGN KEY (i_id_table) REFERENCES sch_chameleon.t_replica_tables (i_id_table)
            ON UPDATE RESTRICT ON DELETE CASCADE
    ); 
 CREATE UNIQUE INDEX idx_t_indexes_id_table_idx_name ON sch_chameleon.t_indexes USING btree(i_id_table,v_index_name);

--VIEWS
CREATE OR REPLACE VIEW sch_chameleon.v_idx_pkeys
AS
SELECT 
	i_id_table,
	oid_conid IS NOT NULL AS b_idx_pkey,
	CASE WHEN oid_conid IS NULL
	THEN 
		format('DROP INDEX %I.%I;',v_schema_name,v_index_name) 
	ELSE
 		format('ALTER TABLE %I.%I DROP CONSTRAINT %I;',v_schema_name,v_table_name,v_index_name)
	END AS t_sql_drop,
	CASE WHEN oid_conid IS NULL
	THEN 
		format('%s %s; SET default_tablespace=DEFAULT;',
				CASE WHEN v_index_tablespace IS NOT NULL 
				THEN 
					format('SET default_tablespace=%I;',v_index_tablespace) 
				END,
				v_index_def
			)
	ELSE
 		format('%s ALTER TABLE %I.%I ADD CONSTRAINT %I %s; SET default_tablespace=DEFAULT;',
			CASE WHEN v_index_tablespace IS NOT NULL 
			THEN 
				format('SET default_tablespace=%I;',v_index_tablespace) 
			END,
	 		v_schema_name,
	 		v_table_name,
	 		v_index_name,
	 		v_constraint_def
 		)
	END AS t_sql_create,
	v_index_name
	
FROM
(
	SELECT 
		tab.i_id_table,
		tab.v_table_name ,
		tab.v_schema_name,
		idx.indexname AS v_index_name,
		idx.indexdef AS v_index_def,
		pg_get_constraintdef(con.conid) AS v_constraint_def,
		idx.tablespace AS v_index_tablespace,
		con.conid AS oid_conid
		
	FROM 
		sch_chameleon.t_replica_tables tab
		INNER JOIN pg_indexes idx 
		ON
				tab.v_table_name =idx.tablename 
			AND tab.v_schema_name =idx.schemaname
		LEFT OUTER JOIN 
		(
			SELECT 
				con.oid AS conid,
				tab.relname,
				sch.nspname ,
				con.conname ,
				con.contype 
			FROM 
				pg_constraint con 
				INNER JOIN pg_class tab
				ON tab.oid= con.conrelid 
				INNER JOIN pg_namespace sch 
				ON sch."oid" = tab.relnamespace 
			WHERE con.contype='p'
		) con 
		ON 
				con.relname=tab.v_table_name
			AND con.nspname=tab.v_schema_name
			AND con.conname=idx.indexname 
) idx_con
;
