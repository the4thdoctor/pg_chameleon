--create schema
CREATE SCHEMA IF NOT EXISTS sch_chameleon;


CREATE TABLE IF NOT EXISTS sch_chameleon.t_log_replica
(
  i_id_event bigserial NOT NULL,
  v_table_name character varying(100) NOT NULL,
  v_schema_name character varying(100) NOT NULL,
  v_binlog_event character varying(100) NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  ts_event_datetime timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
  jsb_event_data jsonb,
  CONSTRAINT pk_log_replica PRIMARY KEY (i_id_event)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE sch_chameleon.t_replica_batch
(
  i_id_batch bigserial NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  b_processed boolean NOT NULL DEFAULT False,
  ts_created timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
  ts_started timestamp without time zone ,
  ts_completed timestamp without time zone ,
  CONSTRAINT pk_t_batch PRIMARY KEY (i_id_batch)
)
WITH (
  OIDS=FALSE
);