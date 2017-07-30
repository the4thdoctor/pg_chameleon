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
		v_t_ddl		text;
	BEGIN
		v_b_loop:=FALSE;
		RAISE DEBUG 'DROPPING REFERENCE TEMPORARY TABLE';
		DROP TABLE IF EXISTS t_table_fields;

		RAISE DEBUG 'CREATING REFERENCE TABLE';
		CREATE TEMPORARY TABLE t_table_fields
		AS
		SELECT 
			table_schema as v_schema_name,
			table_name as v_table_name,
			columns as t_columns, 
			string_to_array(replace(array_to_string(t_pkeys,','),'"',''),',') as v_pkey_where,
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
				AND	i_id_source=p_i_source_id
			ORDER BY 
				ts_created 
			LIMIT 1
			)
		;
		IF v_i_id_batch IS NULL 
		THEN
			RETURN v_b_loop;
		END IF;
		RAISE DEBUG 'Found id_batch %', v_i_id_batch;
		
		FOR v_r_rows IN 
			SELECT 
				log.i_id_event,
				log.i_id_batch,
				log.v_table_name,
				log.v_schema_name,
				log.enm_binlog_event,
				log.jsb_event_data,
				log.jsb_event_update,
				log.t_query,
				tab.v_pkey_where,
				tab.t_pkeys,
				t_columns
			FROM 
				sch_chameleon.t_log_replica  log
				INNER JOIN t_table_fields tab
					ON
							tab.v_table_name=log.v_table_name
						AND tab.v_schema_name=log.v_schema_name
						

			WHERE
					log.i_id_batch=v_i_id_batch
			ORDER BY ts_event_datetime
			LIMIT p_i_max_events
		LOOP 	
			IF v_r_rows.enm_binlog_event='ddl'
			THEN
				v_t_ddl=format('SET search_path=%I;%s',v_r_rows.v_schema_name,v_r_rows.t_query);
			    RAISE DEBUG 'DDL: %',v_t_ddl;
			    EXECUTE  v_t_ddl;
			    DELETE FROM sch_chameleon.t_log_replica
			    WHERE
				    i_id_event=v_r_rows.i_id_event
			    ;
				UPDATE ONLY sch_chameleon.t_replica_batch  
				SET 
					i_ddl=coalesce(i_ddl,0)+1
				WHERE
					i_id_batch=v_r_rows.i_id_batch
				;
			END IF;
		END LOOP;

		RETURN v_b_loop;

	
	END;
$BODY$
LANGUAGE plpgsql;