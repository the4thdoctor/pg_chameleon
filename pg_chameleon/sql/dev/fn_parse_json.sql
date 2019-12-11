-- parsing function prototype 
SELECT 
	agg.i_id_event,
	agg.v_table_name,
	agg.v_schema_name,
	agg.enm_binlog_event,
	agg.t_query,
	array_to_string(array_agg(quote_ident(t_column)),',') as t_event_columns,
	array_to_string(array_agg(quote_nullable(jsb_event_after->>t_column)),',') as t_event_data,
	array_agg(agg.t_pk_data) as t_pk_data,
	array_agg(format('%I=%L',t_column,agg.jsb_event_after->>t_column)) as t_update
FROM 
(
	SELECT 
		trn.i_id_event,
		trn.v_table_name,
		trn.v_schema_name,
		trn.enm_binlog_event,
		trn.jsb_event_after,
		trn.t_query,
		trn.t_column,
		trn.t_pk_data
		
	FROM
	(
		SELECT 
			evt.i_id_event,
			evt.v_table_name,
			evt.v_schema_name,
			evt.enm_binlog_event,
			evt.jsb_event_after,
			evt.t_query,
			v_table_pkey,
			format(
				'%I=%L',
				evt.v_table_pkey,
				CASE 
					WHEN evt.enm_binlog_event = 'update'
					THEN
						jsb_event_before->>v_table_pkey
					ELSE
						jsb_event_after->>v_table_pkey
				END 	
			) as  t_pk_data,
			

			FROM

			(
				SELECT 
					log.i_id_event,
					log.v_table_name,
					log.v_schema_name,
					log.enm_binlog_event,
					coalesce(log.jsb_event_after,'{"foo":"bar"}'::jsonb) as jsb_event_after,
					(jsonb_each_text(log.jsb_event_after)).key AS t_column,
					log.jsb_event_before,
					log.t_query as t_query,
					log.ts_event_datetime,
					unnest(v_table_pkey) as v_table_pkey
				FROM 
					--sch_chameleon.t_log_replica_mysql_1 log
					sch_chameleon.t_log_replica log
					INNER JOIN sch_chameleon.t_replica_tables tab
						ON 
								tab.v_table_name=log.v_table_name
							AND	tab.v_schema_name=log.v_schema_name
				WHERE
					True 
					--i_id_event = any('{{809600}}'::integer[])
				LIMIT 30
			) evt
	) trn
	GROUP BY
		trn.i_id_event,
		trn.v_table_name,
		trn.v_schema_name,
		trn.enm_binlog_event,
		trn.jsb_event_after,
		trn.t_query,
		trn.t_column,
		trn.t_pk_data
) agg
GROUP BY
	agg.i_id_event,
	agg.v_table_name,
	agg.v_schema_name,
	agg.enm_binlog_event,
	agg.t_query
ORDER BY i_id_event ASC