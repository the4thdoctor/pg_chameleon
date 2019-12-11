--select * from sch_chameleon.t_replica_batch
--delete from sch_chameleon.t_replica_batch where i_id_batch=1
--REPLAY FUNCTION V2
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
	AND	bat.i_id_source=1
ORDER BY 
	bat.ts_created 
LIMIT 1
;

SELECT 
	i_id_event[1:5] 
FROM 
	sch_chameleon.t_batch_events 
WHERE 
	i_id_batch=3
;

SELECT 
	i_id_event[5+1:array_length(i_id_event,1)] 
FROM 
	sch_chameleon.t_batch_events 
WHERE 
	i_id_batch=3
;

SELECT 
	to_timestamp(i_my_event_time)
FROM	
	sch_chameleon.t_log_replica
WHERE
		i_id_event=7
	AND	i_id_batch=3

;

WITH 
	t_tables AS
	(
		SELECT 
			v_table_name,
			v_schema_name,
			unnest(v_table_pkey) as v_table_pkey
		FROM
			sch_chameleon.t_replica_tables
		WHERE
			b_replica_enabled
	),
	t_events AS 
	(
		SELECT 
			i_id_event
		FROM
			unnest('{3,4,5,6,7}'::bigint[]) AS i_id_event
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
	v_table_name
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