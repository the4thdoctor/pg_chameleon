CREATE OR REPLACE VIEW sch_chameleon.v_version 
 AS
	SELECT '0.9'::TEXT t_version
;

ALTER TABLE sch_chameleon.t_sources ADD COLUMN ts_last_event timestamp without time zone;
