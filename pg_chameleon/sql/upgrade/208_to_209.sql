-- upgrade catalogue script 2.0.8 to 2.0.9
--CHANGE VARCHARS TO VARCHAR 64 FOR THE IDENTIFIERS NAMES
ALTER TABLE sch_chameleon.t_replica_tables
ALTER COLUMN v_table_name TYPE character varying(64),
ALTER COLUMN v_schema_name TYPE character varying(64);

ALTER TABLE sch_chameleon.t_discarded_rows
ALTER COLUMN v_table_name TYPE character varying(64),
ALTER COLUMN v_schema_name TYPE character varying(64);


ALTER TABLE sch_chameleon.t_indexes
ALTER COLUMN v_table_name TYPE character varying(64),
ALTER COLUMN v_schema_name TYPE character varying(64), 
ALTER COLUMN v_index_name TYPE character varying(64);

ALTER TABLE sch_chameleon.t_pkeys
ALTER COLUMN v_table_name TYPE character varying(64),
ALTER COLUMN v_schema_name TYPE character varying(64), 
ALTER COLUMN v_index_name TYPE character varying(64);

ALTER TABLE sch_chameleon.t_ukeys
ALTER COLUMN v_table_name TYPE character varying(64),
ALTER COLUMN v_schema_name TYPE character varying(64), 
ALTER COLUMN v_index_name TYPE character varying(64);

ALTER TABLE sch_chameleon.t_fkeys
ALTER COLUMN v_table_name TYPE character varying(64),
ALTER COLUMN v_schema_name TYPE character varying(64), 
ALTER COLUMN v_constraint_name TYPE character varying(64);