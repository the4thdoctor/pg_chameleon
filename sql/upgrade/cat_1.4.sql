ALTER TABLE sch_chameleon.t_sources ADD  v_log_table character varying[];
UPDATE sch_chameleon.t_sources 
	SET v_log_table=ARRAY[
		format('t_log_replica_1_src_%s',i_id_source),
		format('t_log_replica_2_src_%s',i_id_source)
	]
;

