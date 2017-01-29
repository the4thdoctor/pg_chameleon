CREATE OR REPLACE VIEW sch_chameleon.v_version 
 AS
	SELECT '0.8'::TEXT t_version
;

CREATE TABLE sch_chameleon.t_index_def
(
  i_id_def bigserial NOT NULL,
  i_id_source bigint NOT NULL,
  v_schema character varying(100),
  v_table character varying(100),
  v_index character varying(100),
  t_create	text,
  t_drop	text,
  CONSTRAINT pk_t_index_def PRIMARY KEY (i_id_def)
)
WITH (
  OIDS=FALSE
);

CREATE UNIQUE INDEX idx_schema_table_source ON sch_chameleon.t_index_def(i_id_source,v_schema,v_table,v_index);
