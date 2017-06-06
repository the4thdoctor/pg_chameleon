ALTER TABLE sch_chameleon.t_sources ADD  v_log_table character varying[];
UPDATE sch_chameleon.t_sources 
	SET v_log_table=ARRAY[
		format('t_log_replica_1_src_%s',i_id_source),
		format('t_log_replica_2_src_%s',i_id_source)
	]
;


CREATE OR REPLACE FUNCTION sch_chameleon.fn_refresh_parts() 
RETURNS VOID as 
$BODY$
DECLARE
    t_sql text;
    r_tables record;
BEGIN
    FOR r_tables IN SELECT unnest(v_log_table) as v_log_table FROM sch_chameleon.t_sources
    LOOP
        RAISE DEBUG 'CREATING TABLE %', r_tables.v_log_table;
        t_sql:=format('
                        CREATE TABLE IF NOT EXISTS sch_chameleon.%I
                        (
                        CONSTRAINT pk_%s PRIMARY KEY (i_id_event),
                          CONSTRAINT fk_%s FOREIGN KEY (i_id_batch) 
                        	REFERENCES  sch_chameleon.t_replica_batch (i_id_batch)
                    	ON UPDATE RESTRICT ON DELETE CASCADE
                        )
                        INHERITS (sch_chameleon.t_log_replica)
                        ;',
                        r_tables.v_log_table,
                        r_tables.v_log_table,
                        r_tables.v_log_table
                );
        EXECUTE t_sql;
    END LOOP;
END
$BODY$
LANGUAGE plpgsql 
;

DO LANGUAGE plpgsql 
$BODY$
DECLARE
    t_sql text;
    r_tables record;
BEGIN
    FOR r_tables IN SELECT unnest(v_log_table) as v_log_table FROM sch_chameleon.t_sources
    LOOP
        RAISE NOTICE 'CREATING TABLE %', r_tables.v_log_table;
        t_sql:=format('
                        CREATE TABLE IF NOT EXISTS sch_chameleon.%I
                        (
                        CONSTRAINT pk_%s PRIMARY KEY (i_id_event),
                          CONSTRAINT fk_%s FOREIGN KEY (i_id_batch) 
                        	REFERENCES  sch_chameleon.t_replica_batch (i_id_batch)
                    	ON UPDATE RESTRICT ON DELETE CASCADE
                        )
                        INHERITS (sch_chameleon.t_log_replica)
                        ;',
                        r_tables.v_log_table,
                        r_tables.v_log_table,
                        r_tables.v_log_table
                );
        EXECUTE t_sql;
    END LOOP;
    FOR r_tables IN SELECT i_id_source,v_log_table[1] AS v_log_table FROM sch_chameleon.t_sources
    LOOP
        t_sql:=format('
            INSERT INTO sch_chameleon.%I 
            SELECT 
                log.* 
            FROM
                sch_chameleon.t_log_replica_1 log
                INNER JOIN sch_chameleon.t_replica_batch bat
                ON log.i_id_batch=bat.i_id_batch
            WHERE
                bat.i_id_source=%L
            ON CONFLICT DO NOTHING   
            ;'
            ,
            r_tables.v_log_table,
            r_tables.i_id_source
            
        );
        EXECUTE t_sql;
        t_sql:=format('
            INSERT INTO sch_chameleon.%I 
            SELECT 
                log.* 
            FROM
                sch_chameleon.t_log_replica_2 log
                INNER JOIN sch_chameleon.t_replica_batch bat
                ON log.i_id_batch=bat.i_id_batch
            WHERE
                bat.i_id_source=%L
            ON CONFLICT DO NOTHING   
            ;'
            ,
            r_tables.v_log_table,
            r_tables.i_id_source
            
        );
        EXECUTE t_sql;
    END LOOP;
    DROP TABLE sch_chameleon.t_log_replica_1;
    DROP TABLE sch_chameleon.t_log_replica_2;
END
$BODY$
;
ALTER TABLE sch_chameleon.t_replica_batch DROP COLUMN v_log_table;
