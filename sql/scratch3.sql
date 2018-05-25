SELECT 
	* 
FROM 
	sch_chameleon.t_log_replica  
WHERE 
	format('%I.%I',v_schema_name,v_table_name) IN (format('%I.%I','sch_sakila','test_partition'))

update sch_chameleon.t_sources set b_maintenance='f'