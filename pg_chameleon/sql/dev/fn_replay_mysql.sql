CREATE OR REPLACE FUNCTION sch_chameleon.fn_replay_mysql(integer,integer,boolean)
RETURNS sch_chameleon.ty_replay_status AS
$BODY$
	DECLARE
		p_i_max_events	ALIAS FOR $1;
		p_i_id_source		ALIAS FOR $2;
		p_b_exit_on_error	ALIAS FOR $3;
		v_ty_status		sch_chameleon.ty_replay_status;
		v_r_statements	record;
		v_i_id_batch	bigint;
		v_t_ddl		text;
		v_i_replayed	integer;
		v_i_skipped	integer;
		v_i_ddl		integer;
		v_i_evt_replay	bigint[];
		v_i_evt_queue	bigint[];
		v_ts_evt_source	timestamp without time zone;
		v_tab_enabled	boolean;
		
	BEGIN
		v_tab_enabled:=TRUE;
		v_ty_status.b_continue:=FALSE;
		v_ty_status.b_error:=FALSE;
		v_i_replayed:=0;
		v_i_ddl:=0;
		v_i_skipped:=0;
		
		
		v_i_id_batch:= (
			SELECT 
				bat.i_id_batch 
			FROM 
				sch_chameleon.t_replica_batch bat
				INNER JOIN  sch_chameleon.t_batch_events evt
				ON
					evt.i_id_batch=bat.i_id_batch
			WHERE 
					bat.b_started 
				AND	bat.b_processed 
				AND	NOT bat.b_replayed
				AND	bat.i_id_source=p_i_id_source
			ORDER BY 
				bat.ts_created 
			LIMIT 1
			)
		;

		v_i_evt_replay:=(
			SELECT 
				i_id_event[1:p_i_max_events] 
			FROM 
				sch_chameleon.t_batch_events 
			WHERE 
				i_id_batch=v_i_id_batch
		);
		
		
		v_i_evt_queue:=(
			SELECT 
				i_id_event[p_i_max_events+1:array_length(i_id_event,1)] 
			FROM 
				sch_chameleon.t_batch_events 
			WHERE 
				i_id_batch=v_i_id_batch
		);

		v_ts_evt_source:=(
			SELECT 
				to_timestamp(i_my_event_time)
			FROM	
				sch_chameleon.t_log_replica
			WHERE
					i_id_event=v_i_evt_replay[array_length(v_i_evt_replay,1)]
				AND	i_id_batch=v_i_id_batch
		);
		IF v_i_id_batch IS NULL 
		THEN
			RETURN v_ty_status;
		END IF;
		RAISE DEBUG 'Found id_batch %', v_i_id_batch;
		FOR v_r_statements IN 

				WITH 
					t_tables AS
					(
						SELECT i_id_source,
							v_table_name,
							v_schema_name,
							unnest(v_table_pkey) as v_table_pkey
						FROM
							sch_chameleon.t_replica_tables
						WHERE
								b_replica_enabled
							AND 	i_id_source=p_i_id_source
					),
					t_events AS 
					(
						SELECT 
							i_id_event
						FROM
							unnest(v_i_evt_replay) AS i_id_event
					)
				SELECT 
					CASE
						WHEN enm_binlog_event = 'ddl'
						THEN 
							t_query
						WHEN enm_binlog_event = 'insert'
						THEN
							format(
								'INSERT INTO %I.%I (%s) VALUES (%s);',
								v_schema_name,
								v_table_name,
								array_to_string(t_colunm,','),
								array_to_string(t_event_data,',')
								
							)
						WHEN enm_binlog_event = 'update'
						THEN
							format(
								'UPDATE %I.%I SET %s WHERE %s;',
								v_schema_name,
								v_table_name,
								t_update,
								t_pk_update
							)
						WHEN enm_binlog_event = 'delete'
						THEN
							format(
								'DELETE FROM %I.%I WHERE %s;',
								v_schema_name,
								v_table_name,
								t_pk_data
							)
						
					END AS t_sql,
					i_id_event,
					i_id_batch,
					enm_binlog_event,
					v_schema_name,
					v_table_name,
					t_pk_data
				FROM
				(
					SELECT
						i_id_event,
						i_id_batch,
						v_table_name,
						v_schema_name,
						enm_binlog_event,
						t_query,
						ts_event_datetime,
						t_pk_data,
						t_pk_update,
						array_agg(quote_ident(t_column)) AS t_colunm,
						string_agg(distinct format('%I=%L',t_column,jsb_event_after->>t_column),',') as  t_update,
						array_agg(quote_nullable(jsb_event_after->>t_column)) as t_event_data
					FROM
					(
						SELECT
							i_id_event,
							i_id_batch,
							v_table_name,
							v_schema_name,
							enm_binlog_event,
							jsb_event_after,
							jsb_event_before,
							t_query,
							ts_event_datetime,
							string_agg(distinct format('%I=%L',v_pkey,jsb_event_after->>v_pkey),' AND ') as  t_pk_data,
							string_agg(distinct format('%I=%L',v_pkey,jsb_event_before->>v_pkey),' AND ') as  t_pk_update,
							(jsonb_each_text(coalesce(jsb_event_after,'{"foo":"bar"}'::jsonb))).key AS t_column
						FROM
						(
							SELECT 
								log.i_id_event,
								log.i_id_batch,
								log.v_table_name,
								log.v_schema_name,
								log.enm_binlog_event,
								log.jsb_event_after,
								log.jsb_event_before,
								log.t_query,
								ts_event_datetime,
								v_table_pkey as v_pkey
								
								
								
							FROM 
								sch_chameleon.t_log_replica  log
								INNER JOIN t_tables tab
									ON
											tab.v_table_name=log.v_table_name
										AND	tab.v_schema_name=log.v_schema_name
								INNER JOIN t_events evt
									ON	log.i_id_event=evt.i_id_event
						) t_pkey
						GROUP BY
							i_id_event,
							i_id_batch,
							v_table_name,
							v_schema_name,
							enm_binlog_event,
							jsb_event_after,
							jsb_event_before,
							t_query,
							ts_event_datetime
					) t_columns
					GROUP BY
						i_id_event,
						i_id_batch,
						v_table_name,
						v_schema_name,
						enm_binlog_event,
						t_query,
						ts_event_datetime,
						t_pk_data,
						t_pk_update
				) t_sql
				ORDER BY i_id_event			
		LOOP
			BEGIN
				EXECUTE v_r_statements.t_sql;
				IF v_r_statements.enm_binlog_event='ddl'
				THEN
					v_i_ddl:=v_i_ddl+1;
				ELSE
					v_i_replayed:=v_i_replayed+1;
				END IF;
				
			EXCEPTION
				WHEN OTHERS THEN
					v_tab_enabled:=(
						SELECT 
							b_replica_enabled
						FROM 	
							sch_chameleon.t_replica_tables
						WHERE
								v_schema_name=v_r_statements.v_schema_name
								AND	v_table_name=v_r_statements.v_table_name
						)
						;
				
					IF v_tab_enabled
					THEN
						RAISE NOTICE 'An error occurred when replaying data for the table %.%',v_r_statements.v_schema_name,v_r_statements.v_table_name;
						RAISE NOTICE 'SQLSTATE: % - ERROR MESSAGE %',SQLSTATE, SQLERRM;
						RAISE NOTICE 'The table %.% has been removed from the replica',v_r_statements.v_schema_name,v_r_statements.v_table_name;
						v_ty_status.v_table_error:=array_append(v_ty_status.v_table_error, format('%I.%I SQLSTATE: %s - ERROR MESSAGE: %s',v_r_statements.v_schema_name,v_r_statements.v_table_name,SQLSTATE, SQLERRM)::character varying) ;
						RAISE NOTICE 'Adding error log entry for table %.% ',v_r_statements.v_schema_name,v_r_statements.v_table_name;
						INSERT INTO sch_chameleon.t_error_log
							(
								i_id_batch, 
								i_id_source,
								v_schema_name, 
								v_table_name, 
								t_table_pkey, 
								t_binlog_name, 
								i_binlog_position, 
								ts_error, 
								t_sql,
								t_error_message
							)
							SELECT 
								i_id_batch, 
								p_i_id_source,
								v_schema_name, 
								v_table_name, 
								v_r_statements.t_pk_data as t_table_pkey, 
								t_binlog_name, 
								i_binlog_position, 
								clock_timestamp(), 
								quote_literal(v_r_statements.t_sql) as t_sql,
								format('%s - %s',SQLSTATE, SQLERRM) as t_error_message
							FROM
								sch_chameleon.t_log_replica  log
							WHERE 
								log.i_id_event=v_r_statements.i_id_event
						;
						IF p_b_exit_on_error
						THEN
							v_ty_status.b_continue:=FALSE;
							v_ty_status.b_error:=TRUE;
							RETURN v_ty_status;
						ELSE
						
							RAISE NOTICE 'Statement %', v_r_statements.t_sql;
							UPDATE sch_chameleon.t_replica_tables 
								SET 
									b_replica_enabled=FALSE
							WHERE
									v_schema_name=v_r_statements.v_schema_name
								AND	v_table_name=v_r_statements.v_table_name
							;

							RAISE NOTICE 'Deleting the log entries for the table %.% ',v_r_statements.v_schema_name,v_r_statements.v_table_name;
							DELETE FROM sch_chameleon.t_log_replica  log
							WHERE
									v_table_name=v_r_statements.v_table_name
								AND	v_schema_name=v_r_statements.v_schema_name
								AND 	i_id_batch=v_i_id_batch
							;
						END IF;
					END IF;
					

			END;
		END LOOP;
		IF v_ts_evt_source IS NOT NULL
		THEN
			UPDATE sch_chameleon.t_last_replayed
				SET
					ts_last_replayed=v_ts_evt_source
			WHERE 	
				i_id_source=p_i_id_source
			;
		END IF;
		IF v_i_replayed=0 AND v_i_ddl=0
		THEN
			DELETE FROM sch_chameleon.t_log_replica
			WHERE
    			    i_id_batch=v_i_id_batch
			;
				
			GET DIAGNOSTICS v_i_skipped = ROW_COUNT;

			UPDATE ONLY sch_chameleon.t_replica_batch  
			SET 
				b_replayed=True,
				i_skipped=v_i_skipped,
				ts_replayed=clock_timestamp()
				
			WHERE
				i_id_batch=v_i_id_batch
			;

			DELETE FROM sch_chameleon.t_batch_events
			WHERE
				i_id_batch=v_i_id_batch
			;

			v_ty_status.b_continue:=FALSE;
		ELSE
			UPDATE ONLY sch_chameleon.t_replica_batch  
			SET 
				i_ddl=coalesce(i_ddl,0)+v_i_ddl,
				i_replayed=coalesce(i_replayed,0)+v_i_replayed,
				i_skipped=v_i_skipped,
				ts_replayed=clock_timestamp()
				
			WHERE
				i_id_batch=v_i_id_batch
			;

			UPDATE sch_chameleon.t_batch_events
				SET
					i_id_event = v_i_evt_queue
			WHERE
				i_id_batch=v_i_id_batch
			;

			DELETE FROM sch_chameleon.t_log_replica
			WHERE
					i_id_batch=v_i_id_batch
				AND 	i_id_event=ANY(v_i_evt_replay) 
			;
			v_ty_status.b_continue:=TRUE;
			RETURN v_ty_status;
		END IF;
		
		v_i_id_batch:= (
			SELECT 
				bat.i_id_batch 
			FROM 
				sch_chameleon.t_replica_batch bat
				INNER JOIN  sch_chameleon.t_batch_events evt
				ON
					evt.i_id_batch=bat.i_id_batch
			WHERE 
					bat.b_started 
				AND	bat.b_processed 
				AND	NOT bat.b_replayed
				AND	bat.i_id_source=p_i_id_source
			ORDER BY 
				bat.ts_created 
			LIMIT 1
			)
		;
		
		IF v_i_id_batch IS NOT NULL
		THEN
			v_ty_status.b_continue:=TRUE;
		END IF;
		
		
		RETURN v_ty_status;

	END;
	
$BODY$
LANGUAGE plpgsql;
