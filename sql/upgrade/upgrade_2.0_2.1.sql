--upgrade from catalogue 2.0.x
--RENAME BINLOG EVENT TYPE
ALTER TYPE sch_chameleon.en_binlog_event  
	RENAME TO en_event ;

--alter t_error_log
ALTER TABLE sch_chameleon.t_error_log
ADD COLUMN i_xid bigint;

ALTER TABLE sch_chameleon.t_error_log
	ALTER COLUMN t_binlog_name DROP NOT NULL, 
	ALTER COLUMN i_binlog_position DROP NOT NULL
;

--alter t_sources

ALTER TABLE sch_chameleon.t_sources
ADD COLUMN i_xid bigint;


--alter t_batch

ALTER TABLE sch_chameleon.t_replica_batch
ADD COLUMN i_lsn_position bigint;

--alter t_log_replica
ALTER TABLE sch_chameleon.t_log_replica
ADD COLUMN i_xid bigint;

ALTER TABLE sch_chameleon.t_log_replica
ADD COLUMN ts_pg_event_time timestamp with time zone;

CREATE OR REPLACE VIEW sch_chameleon.v_version 
 AS
	SELECT '2.1.0'::TEXT t_version
;
