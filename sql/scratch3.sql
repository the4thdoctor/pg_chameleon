/*
ALTER TABLE sch_chameleon.t_replica_batch
	ADD COLUMN v_log_table character varying NOT NULL DEFAULT 't_log_replica';



-- get the id batch and the v_log_replica table
SELECT 
	bat.i_id_batch,v_log_table
FROM 
	sch_chameleon.t_replica_batch bat
	INNER JOIN  sch_chameleon.t_batch_events evt
	ON
		evt.i_id_batch=bat.i_id_batch
WHERE 
		bat.b_started 
	AND	bat.b_processed 
	AND	NOT bat.b_replayed
	AND	bat.i_id_source=1
ORDER BY 
	bat.ts_created 
LIMIT 1

;

SELECT 
	i_id_event[1:30] 
FROM 
	sch_chameleon.t_batch_events 
WHERE 
	i_id_batch=67
;

SELECT 
	i_id_event[30+1:array_length(i_id_event,1)] 
FROM 
	sch_chameleon.t_batch_events 
WHERE 
	i_id_batch=61;

SELECT 
	to_timestamp(i_my_event_time)
FROM	
	sch_chameleon.t_log_replica
WHERE
		i_id_event=('{40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69}'::integer[])[array_length('{40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69}'::integer[],1)]
	AND	i_id_batch=26;

*/

--string_agg(agg.t_pk_data,' AND ') as  t_pk_data

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
				array_to_string(t_column,','),
				array_to_string(t_event_data,',')
				
			)
		WHEN enm_binlog_event = 'update'
		THEN
			format(
				'UPDATE %I.%I SET %s WHERE %s;',
				v_schema_name,
				v_table_name,
				array_to_string(t_update,' , '),
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
		
	END AS t_sql,
	*
FROM
(
	SELECT
		agg.v_table_name,
		agg.v_schema_name,
		agg.enm_binlog_event,
		agg.jsb_event_after,
		array_agg(distinct agg.t_update) as t_update,
		agg.t_query,
		array_agg(distinct agg.t_column) t_column,
		array_agg(distinct agg.t_event_data) as t_event_data,
		string_agg(distinct agg.t_pk_data,' AND ') as  t_pk_data
	FROM
	(
		SELECT

				trn.v_table_name,
				trn.v_schema_name,
				trn.enm_binlog_event,
				trn.jsb_event_after,
				trn.t_query,
				trn.t_column,
				trn.t_pk_data,
				format('%I=%L',t_column,jsb_event_after->>t_column) as t_update,
				quote_nullable(jsb_event_after->>t_column) as t_event_data
				
		FROM

		(
			SELECT 
				dec.v_table_name,
				dec.v_schema_name,
				dec.enm_binlog_event,
				dec.jsb_event_after,
				dec.t_query,
				(jsonb_each_text(coalesce(dec.jsb_event_after,'{"foo":"bar"}'::jsonb))).key AS t_column,
				t_pk_data
				
			FROM

			(
				SELECT 
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
					) as  t_pk_data

					
				FROM

				(
					SELECT 
						log.v_table_name,
						log.v_schema_name,
						log.enm_binlog_event,
						log.jsb_event_after,--||format('{"i_id_event":%s}',log.i_id_event)::jsonb as jsb_event_after,
						log.jsb_event_before,
						log.t_query,
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
					
					ORDER BY i_id_event ASC
				) evt
			) dec
		) trn
	) agg
	GROUP BY
		agg.v_table_name,
		agg.v_schema_name,
	--	agg.t_update,
		agg.t_query,
	--	agg.t_event_data,
		agg.jsb_event_after,
		agg.enm_binlog_event
) sta
;
