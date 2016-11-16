CREATE OR REPLACE VIEW sch_chameleon.v_version 
 AS
	SELECT '0.5'::TEXT t_version
;

ALTER TABLE sch_chameleon.t_log_replica ADD COLUMN jsb_event_update jsonb NULL;

CREATE OR REPLACE FUNCTION sch_chameleon.fn_process_batch(integer)
RETURNS BOOLEAN AS
$BODY$
	DECLARE
	    p_max_events   ALIAS FOR $1;
		v_r_rows	    record;
		v_t_fields	    text[];
		v_t_values	    text[];
		v_t_sql_rep	    text;
		v_t_pkey	    text;
		v_t_vals	    text;
		v_t_update	    text;
		v_t_ins_fld	    text;
		v_t_ins_val	    text;
		v_t_ddl		    text;
		v_b_loop	    boolean;
		v_i_id_batch	integer;
	BEGIN
	    v_b_loop:=True;
		FOR v_r_rows IN WITH t_batch AS
					(
						SELECT 
							i_id_batch 
						FROM 
							sch_chameleon.t_replica_batch  
						WHERE 
								    b_started 
							AND 	b_processed 
							AND     NOT b_replayed
						ORDER BY 
							ts_created 
						LIMIT 1
					),
				t_events AS
					(
						SELECT 
						    log.i_id_event,
							bat.i_id_batch,
							log.v_table_name,
							log.v_schema_name,
							log.enm_binlog_event,
							log.jsb_event_data,
							log.jsb_event_update,
							log.t_query,
							tab.v_table_pkey as v_pkey_where,
							replace(array_to_string(tab.v_table_pkey,','),'"','') as t_pkeys,
							array_length(tab.v_table_pkey,1) as i_pkeys
						FROM 
							sch_chameleon.t_log_replica  log
							INNER JOIN sch_chameleon.t_replica_tables tab
								ON
										tab.v_table_name=log.v_table_name
									AND 	tab.v_schema_name=log.v_schema_name
								INNER JOIN t_batch bat
								ON	bat.i_id_batch=log.i_id_batch
							
						ORDER BY ts_event_datetime
						LIMIT p_max_events
					)
				SELECT
				    i_id_event,
					i_id_batch,
					v_table_name,
					v_schema_name,
					enm_binlog_event,
					jsb_event_data,
					jsb_event_update,
					t_query,
					string_to_array(t_pkeys,',') as v_table_pkey,
					array_to_string(v_pkey_where,',') as v_pkey_where,
					t_pkeys,
					i_pkeys
				FROM
					t_events
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
            ELSE
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
    					jsonb_each_text(v_r_rows.jsb_event_data) js_event
    			     ) js_dat
    			;
    
    			
    			WITH 	t_jsb AS
    				(
    					SELECT 
							CASE
								WHEN v_r_rows.enm_binlog_event='update'
								THEN 
									v_r_rows.jsb_event_update
							ELSE
								v_r_rows.jsb_event_data 
							END jsb_event_data ,
    						v_r_rows.v_table_pkey v_table_pkey
    				),
    				t_subscripts AS
    				(
    					SELECT 
    						generate_subscripts(v_table_pkey,1) sub
    					FROM 
    						t_jsb
    				)
    			SELECT 
    				array_to_string(v_table_pkey,','),
    				''''||array_to_string(array_agg((jsb_event_data->>v_table_pkey[sub])::text),''',''')||'''' as pk_value
    				INTO 
    					v_t_pkey,
    					v_t_vals
    
    			FROM
    				t_subscripts,t_jsb
    			GROUP BY v_table_pkey
    			;
    			
    			RAISE DEBUG '% % % % % %',v_r_rows.v_table_name,
    					v_r_rows.v_schema_name,
    					v_r_rows.v_table_pkey,
    					v_r_rows.enm_binlog_event,v_t_fields,v_t_values;
    			IF v_r_rows.enm_binlog_event='delete'
    			THEN
    				v_t_sql_rep=format('DELETE FROM %I.%I WHERE (%s)=(%s) ;',
    							v_r_rows.v_schema_name,
    							v_r_rows.v_table_name,
    							v_r_rows.v_pkey_where,
    							v_t_vals
    						);
    				RAISE DEBUG '%',v_t_sql_rep;
    			ELSEIF v_r_rows.enm_binlog_event='update'
    			THEN 
    				SELECT 
    					array_to_string(array_agg(format('%I=%L',t_field,t_value)),',') 
    					INTO
    						v_t_update
    				FROM
    				(
    					SELECT 
    						unnest(v_t_fields) t_field, 
    						unnest(v_t_values) t_value
    				) t_val
    				;
    
    				v_t_sql_rep=format('UPDATE  %I.%I 
    								SET
    									%s
    							WHERE (%s)=(%s) ;',
    							v_r_rows.v_schema_name,
    							v_r_rows.v_table_name,
    							v_t_update,
    							v_r_rows.v_pkey_where,
    							v_t_vals
    						);
    				RAISE DEBUG '%',v_t_sql_rep;
    			ELSEIF v_r_rows.enm_binlog_event='insert'
    			THEN
    				SELECT 
    					array_to_string(array_agg(format('%I',t_field)),',') t_field,
    					array_to_string(array_agg(format('%L',t_value)),',') t_value
    					INTO
    						v_t_ins_fld,
    						v_t_ins_val
    				FROM
    				(
    					SELECT 
    						unnest(v_t_fields) t_field, 
    						unnest(v_t_values) t_value
    				) t_val
    				;
    				v_t_sql_rep=format('INSERT INTO  %I.%I 
    								(
    									%s
    								)
    							VALUES
    								(
    									%s
    								)
    							;',
    							v_r_rows.v_schema_name,
    							v_r_rows.v_table_name,
    							v_t_ins_fld,
    							v_t_ins_val
    							
    						);
    
    				RAISE DEBUG '%',v_t_sql_rep;
    			END IF;
    			EXECUTE v_t_sql_rep;
    			
    			DELETE FROM sch_chameleon.t_log_replica
    		    WHERE
    			    i_id_event=v_r_rows.i_id_event
    		    ;
            END IF;
		END LOOP;
		IF v_r_rows IS NULL
		THEN 
		    RAISE DEBUG 'v_r_rows: %',v_r_rows.i_id_event; 
		    v_b_loop=False;
		    
		
		UPDATE sch_chameleon.t_replica_batch  
			SET 
				b_replayed=True,
				ts_replayed=clock_timestamp()
				
		WHERE
			i_id_batch=(
    			            SELECT 
    							i_id_batch 
    						FROM 
    							sch_chameleon.t_replica_batch  
    						WHERE 
    								b_started 
    							AND 	b_processed 
    							AND     NOT b_replayed
    						ORDER BY 
    							ts_created 
    						LIMIT 1
						)
		RETURNING i_id_batch INTO v_i_id_batch
		;
		DELETE FROM sch_chameleon.t_log_replica
    		    WHERE
    			    i_id_batch=v_i_id_batch
    		    ;
		SELECT 
			count(*)>0 
			INTO
				v_b_loop
		FROM 
			sch_chameleon.t_replica_batch  
		WHERE 
				b_started 
			AND 	b_processed 
			AND     NOT b_replayed
		;

		END IF;
		
        RETURN v_b_loop	;
	END;
$BODY$
LANGUAGE plpgsql;
