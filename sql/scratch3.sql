CREATE TEMPORARY TABLE t_table_fields
as
select 
	v_schema_name,
	v_table_name,
	array_agg(field) as field
from
(
	SELECT distinct
		v_schema_name,
		v_table_name,
		(jsonb_each(jsb_event_data)).key as field
	FROM
		sch_chameleon.t_log_replica
) tab
GROUP BY 
	v_schema_name,
	v_table_name
;

set client_min_messages='debug';



SELECT * FROM sch_chameleon.t_replica_batch where not b_replayed;
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
;

select * from sch_chameleon.t_log_replica limit 1;
SELECT sch_chameleon.fn_process_batch_v2(1000,2)
select * from t_table_fields
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
	t_columns,
	i_id_batch
FROM 
	sch_chameleon.t_log_replica  log
	INNER JOIN t_table_fields tab
		ON
				tab.v_table_name=log.v_table_name
			AND tab.v_schema_name=log.v_schema_name
WHERE
		log.i_id_batch=55
ORDER BY ts_event_datetime
LIMIT 100

select '{"\"film_id\",\"category_id\""}'::text[]
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
							i_id_source=2
					)

		GROUP BY
			col.table_name,
			col.table_schema,
			tab.v_table_pkey
) t_get
;



GroupAggregate  (cost=9930.27..10720.39 rows=2150 width=576) (actual time=1059.486..1358.073 rows=19999 loops=1)

WITH t_data AS
	(
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
			t_columns,
			i_id_batch
		FROM 
			sch_chameleon.t_log_replica  log
			INNER JOIN t_table_fields tab
				ON
						tab.v_table_name=log.v_table_name
					AND tab.v_schema_name=log.v_schema_name
		WHERE
				log.i_id_batch=55
		ORDER BY ts_event_datetime
		--LIMIT 10
),
	t_unnest AS
	(
		SELECT 
			unnest(t_columns) as t_col,
			i_id_event,
			v_table_name,
			v_schema_name,
			jsb_event_data,
			jsb_event_update,
			enm_binlog_event,
			t_pkeys
		FROM 
			t_data
	)
SELECT 
	i_id_event,
	v_table_name,
	v_schema_name,
	string_agg(quote_ident(t_col),','),
	string_agg(quote_literal(jsb_event_data->>t_col),',') as event_data,
	string_agg(quote_literal(jsb_event_update->>t_col),',') as  event_update,
	enm_binlog_event,
	t_pkeys
FROM t_unnest
GROUP BY
	i_id_event,
	v_table_name,
	v_schema_name,
	enm_binlog_event,
	t_pkeys


select * from sch_chameleon.t_log_replica where i_id_batch=61

--GroupAggregate  (cost=4200.62..4990.74 rows=2150 width=164) (actual time=954.133..1252.362 rows=19999 loops=1)
--SELECT sch_chameleon.fn_process_batch(100000,2)
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
				t_cols,
				t_events
			)
		WHEN enm_binlog_event = 'delete'
		THEN
			format(
				'DELETE FROM %I.%I WHERE (%s) =(%s);',
				v_schema_name,
				v_table_name,
				pkey_fields,
				pkey_data
			)
		WHEN enm_binlog_event = 'update'
		THEN
			format(
				'UPDATE %I.%I SET %s WHERE (%s) =(%s);',
				v_schema_name,
				v_table_name,
				t_update,
				pkey_fields,
				pkey_data
			)
	END AS t_sql
FROM
(
	SELECT 
		i_id_event,
		v_table_name,
		v_schema_name,
		string_agg(distinct quote_ident((r_event).key),',') as t_cols,
		string_agg(distinct quote_literal(jsb_event_data->>(r_event).key),',') as t_events,
		string_agg(distinct format('%I=%L',(r_event).key,jsb_event_update->>(r_event).key),',') as  t_update,
		string_agg(distinct quote_literal(jsb_event_data->>v_pkey_where),',') as pkey_data,
		string_agg(distinct quote_ident(v_pkey_where),',') as pkey_fields,
		enm_binlog_event,
		t_query,
		ts_event_datetime
	FROM
		(
			SELECT DISTINCT
				unnest(t_columns) as t_col,
				unnest(v_pkey_where) as v_pkey_where,
				i_id_event,
				v_table_name,
				v_schema_name,
				jsonb_each_text(jsb_event_data) as r_event,
				jsb_event_data,
				jsb_event_update,
				enm_binlog_event,
				t_pkeys,
				t_query,
				ts_event_datetime
			FROM 
				(
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
						t_columns,
						i_id_batch,
						ts_event_datetime
					FROM 
						sch_chameleon.t_log_replica  log
						INNER JOIN t_table_fields tab
							ON
									tab.v_table_name=log.v_table_name
								AND tab.v_schema_name=log.v_schema_name
					WHERE
							log.i_id_batch>=61
					ORDER BY ts_event_datetime
					--LIMIT 2

				) t_data

		) t_unnest
	GROUP BY
		i_id_event,
		v_table_name,
		v_schema_name,
		enm_binlog_event,
		t_pkeys,
		t_query,
		ts_event_datetime
) t_format
ORDER BY ts_event_datetime
;


UPDATE my_schema.film_actor SET actor_id='1',film_id='1',last_update='2006-02-15 05:05:03' WHERE actor_id='1' AND film_id='1';
 SET search_path=my_schema; DROP TABLE IF EXISTS "test";
 SET search_path=my_schema;CREATE TABLE "test" ("id" bigserial NOT NULL,"value1" character varying(45) NOT NULL,"value2" character varying(45) NOT NULL,"last_update" timestamp without time zone NOT NULL);ALTER TABLE "test" ADD CONSTRAINT "pk_test_0" PRIMAR (...)
INSERT INTO my_schema.test (value2,last_update,id,value1) VALUES ('dave','2017-07-30 21:31:18','1','hello');
INSERT INTO my_schema.test (id,value1,value2,last_update) VALUES ('2','knock knock','neo','2017-07-30 21:31:18');
INSERT INTO my_schema.test (value2,id,value1,last_update) VALUES ('phoenix','3','the','2017-07-30 21:31:18');
INSERT INTO my_schema.test (id,value1,value2,last_update) VALUES ('4','the answer','is 42','2017-07-30 21:31:18');
 SET search_path=my_schema;   ALTER TABLE test ADD "count" integer NULL , ADD "log" character varying(12) NULL DEFAULT 'blah', ADD "new_enum" enum_test_new_enum NULL , ADD "status" integer NULL  ; DROP TYPE "enum_test_log"   
 SET search_path=my_schema;ALTER TABLE test DROP count CASCADE, ADD "status_2" integer NULL  ;
DELETE FROM my_schema.test WHERE id='4';
UPDATE my_schema.test SET id='1',last_update='2017-07-30 21:31:18',log='blah',new_enum=NULL,status=NULL,status_2=NULL,value1='hello',value2='dave' WHERE id='1';
 SET search_path=my_schema; ALTER TABLE "test" ALTER COLUMN "log" SET DATA TYPE enum_test_log USING "log"::enum_test_log ; 
 SET search_path=my_schema; TRUNCATE TABLE "test" CASCADE;
