CREATE OR REPLACE FUNCTION sch_chameleon.fn_process_batch()
RETURNS VOID AS
$BODY$
	DECLARE
		v_r_rows	record;
		v_t_fields	text[];
		v_t_values	text[];
	BEGIN
		FOR v_r_rows IN WITH t_batch AS
					(
						SELECT 
							i_id_batch 
						FROM 
							sch_chameleon.t_replica_batch  
						WHERE 
								b_started 
							AND 	b_processed 
						ORDER BY 
							ts_created 
						LIMIT 1
					)

				SELECT 
					v_table_name,
					v_schema_name,
					v_binlog_event,
					jsb_event_data 
				FROM 
					sch_chameleon.t_log_replica  log
					INNER JOIN t_batch bat
						ON	bat.i_id_batch=log.i_id_batch
				ORDER BY ts_event_datetime
			LOOP

			SELECT 
				array_agg(key) evt_fields,
				array_agg(value) evt_values
				INTO
					v_t_fields,
					v_t_values
			FROM (
				SELECT 
					key ,
					value
				FROM 
					jsonb_each(v_r_rows.jsb_event_data) js_event
			     ) js_dat
			;
			RAISE NOTICE '% % % % %',v_r_rows.v_table_name,
					v_r_rows.v_schema_name,
					v_r_rows.v_binlog_event,v_t_fields,v_t_values;

		END LOOP;
	
	END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION sch_chameleon.fn_process_batch() OWNER TO usr_replication ;

SELECT sch_chameleon.fn_process_batch()