WITH t_batch AS
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
ORDER BY ts_event_datetime;

SELECT 
	array_agg(key) evt_fields,
	array_agg(value) evt_values
	
	
FROM (
	SELECT * from json_each('{"id": 1379483, "data": "Hello", "data2": "friend", "date_test": "2016-09-02 11:30:46"}'::json) js_event
     ) T
