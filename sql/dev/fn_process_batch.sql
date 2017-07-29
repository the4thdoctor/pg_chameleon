--version 2 fn_process_batch
CREATE OR REPLACE FUNCTION sch_chameleon.fn_process_batch_v2(integer,integer)
RETURNS BOOLEAN AS
$BODY$
	DECLARE
		p_i_max_events	ALIAS FOR $1;
		p_i_source_id		ALIAS FOR $2;
		v_b_loop		boolean;
		v_r_rows		record;
		v_i_id_batch		bigint;
	BEGIN
		v_b_loop:=FALSE;
		RAISE DEBUG 'DROPPING REFERENCE TEMPORARY TABLE';
		DROP TABLE IF EXISTS t_table_fields;

		RAISE DEBUG 'CREATING REFERENCE TABLE';
		CREATE TEMPORARY TABLE t_table_fields
		AS
		SELECT 
			table_schema,
			table_name,
			columns ,
			replace(array_to_string(t_pkeys,','),'"','') as t_pkeys
			
			
		FROM
		(
			SELECT 
					table_schema,
					table_name,
					array_agg(column_name::text) as columns ,
					tab.v_table_pkey as t_pkeys
					
				FROM 
					information_schema.columns col
					INNER JOIN sch_chameleon.t_replica_tables tab
					ON
							tab.v_table_name=col.table_name
						AND	tab.v_schema_name=col.table_schema

				WHERE 
					table_schema = (

								SELECT 
									t_dest_schema 
								FROM 
									sch_chameleon.t_sources
								WHERE 
									i_id_source=p_i_source_id
							)

				GROUP BY
					col.table_name,
					col.table_schema,
					tab.v_table_pkey
		) t_get
		;

		v_i_id_batch:= (
			SELECT 
				i_id_batch 
			FROM ONLY
				sch_chameleon.t_replica_batch  
			WHERE 
					b_started 
				AND	b_processed 
				AND	NOT b_replayed
				AND	i_id_source=2
			ORDER BY 
				ts_created 
			LIMIT 1
			)
		;
		RAISE DEBUG 'Found id_batch %', v_i_id_batch;

		

		RETURN v_b_loop;

	
	END;
$BODY$
LANGUAGE plpgsql;