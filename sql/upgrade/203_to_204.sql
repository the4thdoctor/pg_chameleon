-- upgrade catalogue script 2.0.2 to 2.0.3

ALTER TABLE sch_chameleon.t_replica_batch
	ADD COLUMN t_gtid_set text;

