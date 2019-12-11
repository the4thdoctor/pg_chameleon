-- upgrade catalogue script 2.0.1 to 2.0.2

ALTER TABLE sch_chameleon.t_sources
	ADD COLUMN b_paused boolean NOT NULL DEFAULT False,
	ADD COLUMN ts_last_maintenance timestamp without time zone NULL ;

ALTER TABLE sch_chameleon.t_last_received
	ADD COLUMN b_paused boolean NOT NULL DEFAULT False;

ALTER TABLE sch_chameleon.t_last_replayed
	ADD COLUMN b_paused boolean NOT NULL DEFAULT False;

