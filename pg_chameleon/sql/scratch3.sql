--select * from sch_chameleon.t_sources;
set client_min_messages=debug;
--select * from sch_chameleon.fn_replay_mysql(1000,1,true)


SELECT 
	i_id_event AS i_id_event,
	enm_binlog_event,
	(enm_binlog_event='ddl')::integer as i_ddl,
	(enm_binlog_event<>'ddl')::integer as i_replay,
	t_binlog_name,
	i_binlog_position,
	v_table_name,
	v_schema_name,
	t_pk_data,
	CASE
		WHEN enm_binlog_event = 'ddl'
		THEN 
			t_query
		WHEN enm_binlog_event = 'insert'
		THEN
			format(
				'INSERT INTO %I.%I %s;',
				v_schema_name,
				v_table_name,
				t_dec_data
				
			)
		WHEN enm_binlog_event = 'update'
		THEN
			format(
				'UPDATE %I.%I SET %s WHERE %s;',
				v_schema_name,
				v_table_name,
				t_dec_data,
				t_pk_data
			)
		WHEN enm_binlog_event = 'delete'
		THEN
			format(
				'DELETE FROM %I.%I WHERE %s;',
				v_schema_name,
				v_table_name,
				t_pk_data
			)
		
	END AS t_sql
FROM 
(
	SELECT 
		dec.i_id_event,
		dec.v_table_name,
		dec.v_schema_name,
		dec.enm_binlog_event,
		dec.t_binlog_name,
		dec.i_binlog_position,
		dec.t_query as t_query,
		dec.ts_event_datetime,
		CASE
			WHEN dec.enm_binlog_event = 'insert'
			THEN
			format('(%s) VALUES (%s)',string_agg(format('%I',dec.t_column),','),string_agg(format('%L',jsb_event_after->>t_column),','))
			WHEN dec.enm_binlog_event = 'update'
			THEN
				string_agg(format('%I=%L',dec.t_column,jsb_event_after->>t_column),',')
			
		END AS t_dec_data,
		string_agg(DISTINCT 
				CASE
					WHEN dec.v_table_pkey IS NOT NULL
					THEN
						format(
							'%I=%L',
							dec.v_table_pkey,
							CASE 
								WHEN dec.enm_binlog_event = 'update'
								THEN
									jsb_event_before->>v_table_pkey
								ELSE
									jsb_event_after->>v_table_pkey
							END 	
				 	
						)
				END
				,' AND ') as  t_pk_data
	FROM 
	(
		SELECT 
			log.i_id_event,
			log.v_table_name,
			log.v_schema_name,
			log.enm_binlog_event,
			log.t_binlog_name,
			log.i_binlog_position,
			coalesce(log.jsb_event_after,'{"foo":"bar"}'::jsonb) as jsb_event_after,
			(jsonb_each_text(coalesce(log.jsb_event_after,'{"foo":"bar"}'::jsonb))).key AS t_column,
			log.jsb_event_before,
			log.t_query as t_query,
			log.ts_event_datetime,
			unnest(v_table_pkey) as v_table_pkey
		FROM 
			sch_chameleon.t_log_replica_mysql_2 log
			INNER JOIN sch_chameleon.t_replica_tables tab
				ON 
						tab.v_table_name=log.v_table_name
					AND	tab.v_schema_name=log.v_schema_name
		WHERE
				tab.b_replica_enabled
			AND	i_id_event = ANY('{10}')
	) dec
	GROUP BY 
		dec.i_id_event,
		dec.v_table_name,
		dec.v_schema_name,
		dec.enm_binlog_event,
		dec.t_query,
		dec.ts_event_datetime,
		dec.t_binlog_name,
		dec.i_binlog_position
) par
ORDER BY 
	i_id_event ASC
;