-- upgrade catalogue script 2.0.2 to 2.0.3

ALTER TABLE sch_chameleon.t_sources
	ADD COLUMN b_maintenance boolean NOT NULL DEFAULT False;

