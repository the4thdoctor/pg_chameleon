-- upgrade catalogue script 2.0.0 to 2.0.1

ALTER TABLE sch_chameleon.t_error_log
	ALTER COLUMN i_binlog_position SET DATA TYPE bigint;

ALTER TABLE sch_chameleon.t_sources
	ALTER COLUMN i_binlog_position SET DATA TYPE bigint;
	
ALTER TABLE sch_chameleon.t_replica_batch
	ALTER COLUMN i_binlog_position SET DATA TYPE bigint;
	
ALTER TABLE sch_chameleon.t_log_replica
	ALTER COLUMN i_binlog_position SET DATA TYPE bigint;

ALTER TABLE sch_chameleon.t_replica_tables
	ALTER COLUMN i_binlog_position SET DATA TYPE bigint;
