-- upgrade catalogue script 2.0.4 to 2.0.5

ALTER TABLE sch_chameleon.t_replica_batch
	ADD COLUMN v_log_table character varying NOT NULL DEFAULT 't_log_replica';

