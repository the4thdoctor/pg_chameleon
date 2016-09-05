--create schema
CREATE SCHEMA IF NOT EXISTS sch_chameleon;



CREATE TABLE sch_chameleon.t_replica_batch
(
  i_id_batch bigserial NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  b_started boolean NOT NULL DEFAULT False,
  b_processed boolean NOT NULL DEFAULT False,
  ts_created timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
  ts_started timestamp without time zone ,
  ts_completed timestamp without time zone ,
  CONSTRAINT pk_t_batch PRIMARY KEY (i_id_batch)
)
WITH (
  OIDS=FALSE
);

CREATE UNIQUE INDEX idx_t_replica_batch_binlog_name_position 
	ON sch_chameleon.t_replica_batch (t_binlog_name,i_binlog_position);

CREATE UNIQUE INDEX idx_t_replica_batch_ts_created
	ON sch_chameleon.t_replica_batch (ts_created);

CREATE TABLE IF NOT EXISTS sch_chameleon.t_log_replica
(
  i_id_event bigserial NOT NULL,
  i_id_batch bigserial NOT NULL,
  v_table_name character varying(100) NOT NULL,
  v_schema_name character varying(100) NOT NULL,
  v_binlog_event character varying(100) NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  ts_event_datetime timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
  jsb_event_data jsonb,
  CONSTRAINT pk_log_replica PRIMARY KEY (i_id_event),
  CONSTRAINT fk_replica_batch FOREIGN KEY (i_id_batch) 
	REFERENCES  sch_chameleon.t_replica_batch (i_id_batch)
	ON UPDATE RESTRICT ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);


CREATE TABLE sch_chameleon.t_replica_tables
(
  i_id_table bigserial NOT NULL,
  v_table_name character varying(100) NOT NULL,
  v_schema_name character varying(100) NOT NULL,
  CONSTRAINT pk_t_replica_tables PRIMARY KEY (i_id_table)
)
WITH (
  OIDS=FALSE
);