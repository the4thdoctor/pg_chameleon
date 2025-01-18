--CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS sch_chameleon;

--TYPES
CREATE TYPE sch_chameleon.en_src_status
    AS ENUM ('ready', 'initialising','initialised','syncing','synced','stopped','running','error');

CREATE TYPE sch_chameleon.en_binlog_event
    AS ENUM ('delete', 'update', 'insert','ddl');

CREATE TYPE sch_chameleon.en_src_type
    AS ENUM ('mysql','pgsql');

CREATE TYPE sch_chameleon.ty_replay_status
    AS
    (
        b_continue boolean,
        b_error  boolean,
        v_table_error character varying[]
    );

--TABLES/INDICES

CREATE TABLE sch_chameleon.t_error_log
(
    i_id_log      bigserial,
    i_id_batch bigint NOT NULL,
    i_id_source bigint NOT NULL,
    v_table_name character varying(100) NOT NULL,
    v_schema_name character varying(100) NOT NULL,
    t_table_pkey text NOT NULL,
    t_binlog_name text NOT NULL,
    i_binlog_position bigint NOT NULL,
    ts_error  timestamp without time zone,
    t_sql text,
    t_error_message text,
    CONSTRAINT pk_t_error_log PRIMARY KEY (i_id_log)
)
;


CREATE TABLE sch_chameleon.t_sources
(
    i_id_source     bigserial,
    t_source        text NOT NULL,
    jsb_schema_mappings jsonb NOT NULL,
    enm_status sch_chameleon.en_src_status NOT NULL DEFAULT 'ready',
    t_binlog_name text,
    i_binlog_position bigint,
    b_consistent boolean NOT NULL DEFAULT TRUE,
    b_paused boolean NOT NULL DEFAULT FALSE,
    b_maintenance boolean NOT NULL DEFAULT FALSE,
    ts_last_maintenance timestamp without time zone NULL ,
    enm_source_type sch_chameleon.en_src_type NOT NULL,
    v_log_table character varying[] ,
    CONSTRAINT pk_t_sources PRIMARY KEY (i_id_source)
)
;

CREATE TABLE sch_chameleon.t_last_received
(
    i_id_source     bigserial,
    b_paused      boolean NOT NULL DEFAULT FALSE,
    ts_last_received    timestamp without time zone,
    CONSTRAINT pk_t_last_received PRIMARY KEY (i_id_source),
    CONSTRAINT fk_last_received_id_source FOREIGN KEY (i_id_source)
    REFERENCES  sch_chameleon.t_sources(i_id_source)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
;

CREATE TABLE sch_chameleon.t_last_replayed
(
    i_id_source     bigserial,
    b_paused      boolean NOT NULL DEFAULT FALSE,
    ts_last_replayed timestamp without time zone,
    CONSTRAINT pk_t_last_replayed PRIMARY KEY (i_id_source),
    CONSTRAINT fk_last_replayed_id_source FOREIGN KEY (i_id_source)
    REFERENCES  sch_chameleon.t_sources(i_id_source)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
;


CREATE UNIQUE INDEX idx_t_sources_t_source ON sch_chameleon.t_sources(t_source);

CREATE TABLE sch_chameleon.t_replica_batch
(
  i_id_batch bigserial NOT NULL,
  i_id_source bigint NOT NULL,
  t_binlog_name text,
  v_log_table character varying NOT NULL DEFAULT 't_log_replica',
  i_binlog_position bigint,
  t_gtid_set text,
  b_started boolean NOT NULL DEFAULT False,
  b_processed boolean NOT NULL DEFAULT False,
  b_replayed boolean NOT NULL DEFAULT False,
  ts_created timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
  ts_processed timestamp without time zone ,
  ts_replayed timestamp without time zone ,
  i_replayed bigint NULL,
  i_skipped bigint NULL,
  i_ddl bigint NULL,
  CONSTRAINT pk_t_batch PRIMARY KEY (i_id_batch)
)
WITH (
  OIDS=FALSE
);

CREATE UNIQUE INDEX idx_t_replica_batch_binlog_name_position
    ON sch_chameleon.t_replica_batch  (i_id_source,t_binlog_name,i_binlog_position);

CREATE UNIQUE INDEX idx_t_replica_batch_ts_created
    ON sch_chameleon.t_replica_batch (i_id_source,ts_created);

CREATE TABLE IF NOT EXISTS sch_chameleon.t_log_replica
(
  i_id_event bigserial NOT NULL,
  i_id_batch bigserial NOT NULL,
  v_table_name character varying(100) NOT NULL,
  v_schema_name character varying(100) NOT NULL,
  enm_binlog_event sch_chameleon.en_binlog_event NOT NULL,
  t_binlog_name text,
  i_binlog_position bigint,
  ts_event_datetime timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
  jsb_event_after jsonb,
  jsb_event_before jsonb,
  t_query text,
  i_my_event_time bigint,
  CONSTRAINT pk_log_replica PRIMARY KEY (i_id_event),
  CONSTRAINT fk_replica_batch FOREIGN KEY (i_id_batch)
    REFERENCES  sch_chameleon.t_replica_batch (i_id_batch)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
;


CREATE TABLE sch_chameleon.t_replica_tables
(
  i_id_table bigserial NOT NULL,
  i_id_source bigint NOT NULL,
  v_table_name character varying(64) NOT NULL,
  v_schema_name character varying(64) NOT NULL,
  v_table_pkey character varying(100)[] NOT NULL,
  t_binlog_name text,
  i_binlog_position bigint,
  b_replica_enabled boolean NOT NULL DEFAULT true,
  CONSTRAINT pk_t_replica_tables PRIMARY KEY (i_id_table)
)
WITH (
  OIDS=FALSE
);

CREATE UNIQUE INDEX idx_t_replica_tables_table_schema
    ON sch_chameleon.t_replica_tables (i_id_source,v_table_name,v_schema_name);


CREATE TABLE sch_chameleon.t_discarded_rows
(
    i_id_row    bigserial,
    i_id_batch  bigint NOT NULL,
    ts_discard  timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    v_table_name character varying(64) NOT NULL,
    v_schema_name character varying(64) NOT NULL,
    t_row_data  text,
    CONSTRAINT pk_t_discarded_rows PRIMARY KEY (i_id_row)
)
;


ALTER TABLE sch_chameleon.t_replica_batch
    ADD CONSTRAINT fk_t_replica_batch_i_id_source FOREIGN KEY (i_id_source)
    REFERENCES sch_chameleon.t_sources (i_id_source)
    ON UPDATE RESTRICT ON DELETE CASCADE
    ;

ALTER TABLE sch_chameleon.t_replica_tables
    ADD CONSTRAINT fk_t_replica_tables_i_id_source FOREIGN KEY (i_id_source)
    REFERENCES sch_chameleon.t_sources (i_id_source)
    ON UPDATE RESTRICT ON DELETE CASCADE
    ;



CREATE TABLE sch_chameleon.t_batch_events
(
    i_id_batch  bigint NOT NULL,
    I_id_event  bigint[] NOT NULL,
    CONSTRAINT pk_t_batch_id_events PRIMARY KEY (i_id_batch)
)
;

ALTER TABLE sch_chameleon.t_batch_events
    ADD CONSTRAINT fk_t_batch_id_events_i_id_batch FOREIGN KEY (i_id_batch)
    REFERENCES sch_chameleon.t_replica_batch(i_id_batch)
    ON UPDATE RESTRICT ON DELETE CASCADE
    ;

CREATE TABLE sch_chameleon.t_indexes
    (
        i_id_index  bigserial,
        v_schema_name character varying(64) NOT NULL,
        v_table_name character varying(64) NOT NULL,
        v_index_name character varying(64) NOT NULL,
        t_index_drop text NULL,
        t_index_create text NULL,
        CONSTRAINT pk_t_indexes PRIMARY KEY (i_id_index)
    );
 CREATE UNIQUE INDEX idx_t_indexes_idx_table_schema ON sch_chameleon.t_indexes USING btree(v_schema_name,v_table_name,v_index_name);

CREATE TABLE sch_chameleon.t_pkeys
    (
        i_id_pkey bigserial,
        v_schema_name character varying(64) NOT NULL,
        v_table_name character varying(64) NOT NULL,
        v_index_name character varying(64) NOT NULL,
        t_pkey_drop text NULL,
        t_pkey_create text NULL,
        CONSTRAINT pk_t_pkeys PRIMARY KEY (i_id_pkey)
    );
CREATE UNIQUE INDEX idx_t_pkeys_table_schema ON sch_chameleon.t_pkeys USING btree(v_schema_name,v_table_name);

CREATE TABLE sch_chameleon.t_ukeys
    (
        i_id_ukey bigserial,
        v_schema_name character varying(64) NOT NULL,
        v_table_name character varying(64) NOT NULL,
        v_index_name character varying(64) NOT NULL,
        t_ukey_drop text NULL,
        t_ukey_create text NULL,
        CONSTRAINT pk_t_ukeys PRIMARY KEY (i_id_ukey)
    );
CREATE UNIQUE INDEX idx_t_ukeys_table_schema ON sch_chameleon.t_ukeys USING btree(v_schema_name,v_table_name,v_index_name);

CREATE TABLE sch_chameleon.t_fkeys
    (
        i_id_fkey bigserial,
        v_schema_name character varying(64) NOT NULL,
        v_table_name character varying(64) NOT NULL,
        v_constraint_name character varying(64) NOT NULL,
        t_fkey_drop text NULL,
        t_fkey_create text NULL,
        t_fkey_validate text NULL,
        CONSTRAINT pk_t_fkeys PRIMARY KEY (i_id_fkey)
    );
 CREATE UNIQUE INDEX idx_t_fkeys_idx_table_schema ON sch_chameleon.t_fkeys USING btree(v_schema_name,v_table_name,v_constraint_name);


--FUNCTIONS
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
    t_sql:=format('
            CREATE INDEX IF NOT EXISTS idx_id_batch_%s
            ON sch_chameleon.%I (i_id_batch)
            ;',
            r_tables.v_log_table,
                        r_tables.v_log_table
        );
    EXECUTE t_sql;
    END LOOP;
END
$BODY$
LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION sch_chameleon.fn_replay_mysql(integer,integer,boolean)
RETURNS sch_chameleon.ty_replay_status AS
$BODY$
    DECLARE
        p_i_max_events    ALIAS FOR $1;
        p_i_id_source   ALIAS FOR $2;
        p_b_exit_on_error ALIAS FOR $3;
        v_ty_status   sch_chameleon.ty_replay_status;
        v_r_statements    record;
        v_i_id_batch    bigint;
        v_v_log_table   text;
        v_t_ddl     text;
        v_t_main_sql    text;
        v_t_delete_sql    text;
        v_i_replayed    integer;
        v_i_skipped   integer;
        v_i_ddl     integer;
        v_i_evt_replay    bigint[];
        v_i_evt_queue   bigint[];
        v_ts_evt_source   timestamp without time zone;
        v_tab_enabled   boolean;

    BEGIN
        v_i_replayed:=0;
        v_i_ddl:=0;
        v_i_skipped:=0;
        v_ty_status.b_continue:=FALSE;
        v_ty_status.b_error:=FALSE;
        RAISE DEBUG 'Searching batches to replay for source id: %', p_i_id_source;
        v_i_id_batch:= (
            SELECT
                bat.i_id_batch
            FROM
                sch_chameleon.t_replica_batch bat
                INNER JOIN  sch_chameleon.t_batch_events evt
                ON
                    evt.i_id_batch=bat.i_id_batch
            WHERE
                    bat.b_started
                AND bat.b_processed
                AND NOT bat.b_replayed
                AND bat.i_id_source=p_i_id_source
            ORDER BY
                bat.ts_created
            LIMIT 1
            )
        ;

        v_v_log_table:=(
            SELECT
                v_log_table
            FROM
                sch_chameleon.t_replica_batch
            WHERE
                i_id_batch=v_i_id_batch
            )
        ;
        IF v_i_id_batch IS NULL
        THEN
            RAISE DEBUG 'There are no batches available for replay';
            RETURN v_ty_status;
        END IF;

        RAISE DEBUG 'Found id_batch %, data in log table %', v_i_id_batch,v_v_log_table;
        RAISE DEBUG 'Building a list of event id with max length %...', p_i_max_events;
        v_i_evt_replay:=(
            SELECT
                i_id_event[1:p_i_max_events]
            FROM
                sch_chameleon.t_batch_events
            WHERE
                i_id_batch=v_i_id_batch
        );


        v_i_evt_queue:=(
            SELECT
                i_id_event[p_i_max_events+1:array_length(i_id_event,1)]
            FROM
                sch_chameleon.t_batch_events
            WHERE
                i_id_batch=v_i_id_batch
        );

        RAISE DEBUG 'Finding the last executed event''s timestamp...';
        v_ts_evt_source:=(
            SELECT
                to_timestamp(i_my_event_time)
            FROM
                sch_chameleon.t_log_replica
            WHERE
                    i_id_event=v_i_evt_replay[array_length(v_i_evt_replay,1)]
                AND i_id_batch=v_i_id_batch
        );

        RAISE DEBUG 'Generating the main loop sql';

        v_t_main_sql:=format('
            SELECT
                i_id_event AS i_id_event,
                enm_binlog_event,
                (enm_binlog_event=''ddl'')::integer as i_ddl,
                (enm_binlog_event<>''ddl'')::integer as i_replay,
                t_binlog_name,
                i_binlog_position,
                v_table_name,
                v_schema_name,
                t_pk_data,
                CASE
                    WHEN enm_binlog_event = ''ddl''
                    THEN
                        t_query
                    WHEN enm_binlog_event = ''insert''
                    THEN
                        format(
                            ''INSERT INTO %%I.%%I %%s;'',
                            v_schema_name,
                            v_table_name,
                            t_dec_data

                        )
                    WHEN enm_binlog_event = ''update''
                    THEN
                        format(
                            ''UPDATE %%I.%%I SET %%s WHERE %%s;'',
                            v_schema_name,
                            v_table_name,
                            t_dec_data,
                            t_pk_data
                        )
                    WHEN enm_binlog_event = ''delete''
                    THEN
                        format(
                            ''DELETE FROM %%I.%%I WHERE %%s;'',
                            v_schema_name,
                            v_table_name,
                            t_pk_data
                        )

                END AS t_sql
            FROM
            (
                SELECT
                    pk.i_id_event,
                    pk.v_table_name,
                    pk.v_schema_name,
                    pk.enm_binlog_event,
                    pk.t_binlog_name,
                    pk.i_binlog_position,
                    pk.t_query as t_query,
                    pk.ts_event_datetime,
                    pk.t_dec_data,
                    pk.t_pk_data
                FROM
                (
                    SELECT
                        dec.i_id_event,
                        dec.v_table_name,
                        dec.v_schema_name,
                        dec.enm_binlog_event,
                        dec.t_binlog_name,
                        dec.i_binlog_position,
                        dec.t_query as t_query,
                        dec.ts_event_datetime,
                        CASE WHEN dec.enm_binlog_event = ''insert''
                            THEN format(''(%%s) VALUES (%%s)'',string_agg(format(''%%I'',dec.t_column_key),'',''),string_agg(format(''%%L'',dec.t_column_value),'',''))
                            WHEN dec.enm_binlog_event = ''update''
                            THEN string_agg(format(''%%I=%%L'',dec.t_column_key,dec.t_column_value),'','')
                        END AS t_dec_data,
                   CASE
                        WHEN dec.enm_binlog_event = ''ddl''
                        THEN ''N/A''
                        ELSE

                               string_agg(
                                       DISTINCT
                                            CASE
                                                WHEN
                                                        dec.v_table_pkey IS NOT NULL
                                                AND     dec.t_column_key = ANY (dec.v_table_pkey)
                                            THEN
                                                format(''%%I=%%L'', dec.t_column_key,
                                                        CASE
                                                            WHEN dec.enm_binlog_event = ''update''
                                                            THEN dec.jsb_event_before ->> dec.t_column_key
                                                        ELSE dec.t_column_value
                                                        END
                                            )
                                   END
                           , '' AND '')
                        END AS t_pk_data,
                        dec.jsb_event_before
                    FROM
                    (
                        SELECT
                            log.i_id_event,
                            log.v_table_name,
                            log.v_schema_name,
                            log.enm_binlog_event,
                            log.t_binlog_name,
                            log.i_binlog_position,
                            (jsonb_each_text(coalesce(log.jsb_event_after,''{"foo":"bar"}''::jsonb))).key AS t_column_key,
                            (jsonb_each_text(coalesce(log.jsb_event_after,''{"foo":"bar"}''::jsonb))).value AS t_column_value,
                            log.jsb_event_before,
                            log.t_query as t_query,
                            log.ts_event_datetime,
                            v_table_pkey
                        FROM
                            sch_chameleon.%I log
                            INNER JOIN sch_chameleon.t_replica_tables tab
                                ON
                                        tab.v_table_name=log.v_table_name
                                    AND tab.v_schema_name=log.v_schema_name
                        WHERE
                                tab.b_replica_enabled
                            AND i_id_event = ANY(%L)

                    ) dec
                    GROUP BY
                        dec.i_id_event,
                        dec.v_table_name,
                        dec.v_schema_name,
                        dec.enm_binlog_event,
                        dec.t_query,
                        dec.ts_event_datetime,
                        dec.t_binlog_name,
                        dec.i_binlog_position,
                        dec.jsb_event_before
                    ) pk
                GROUP BY
                    pk.i_id_event,
                    pk.v_table_name,
                    pk.v_schema_name,
                    pk.enm_binlog_event,
                    pk.t_binlog_name,
                    pk.i_binlog_position,
                    pk.t_query,
                    pk.ts_event_datetime,
                    pk.t_dec_data,
                    pk.t_pk_data

            ) par
            ORDER BY
                i_id_event ASC
            ;
        ',v_v_log_table,v_i_evt_replay);
        RAISE DEBUG '%',v_t_main_sql;
        FOR v_r_statements IN EXECUTE v_t_main_sql
        LOOP

            BEGIN
                EXECUTE v_r_statements.t_sql;
                v_i_ddl:=v_i_ddl+v_r_statements.i_ddl;
                v_i_replayed:=v_i_replayed+v_r_statements.i_replay;


            EXCEPTION
                WHEN OTHERS
                THEN
                RAISE NOTICE 'An error occurred when replaying data for the table %.%',v_r_statements.v_schema_name,v_r_statements.v_table_name;
                RAISE NOTICE 'SQLSTATE: % - ERROR MESSAGE %',SQLSTATE, SQLERRM;
                RAISE DEBUG 'SQL EXECUTED: % ',v_r_statements.t_sql;
                RAISE NOTICE 'The table %.% has been removed from the replica',v_r_statements.v_schema_name,v_r_statements.v_table_name;
                v_ty_status.v_table_error:=array_append(v_ty_status.v_table_error, format('%I.%I SQLSTATE: %s - ERROR MESSAGE: %s',v_r_statements.v_schema_name,v_r_statements.v_table_name,SQLSTATE, SQLERRM)::character varying) ;
                RAISE NOTICE 'Adding error log entry for table %.% ',v_r_statements.v_schema_name,v_r_statements.v_table_name;
                INSERT INTO sch_chameleon.t_error_log
                            (
                                i_id_batch,
                                i_id_source,
                                v_schema_name,
                                v_table_name,
                                t_table_pkey,
                                t_binlog_name,
                                i_binlog_position,
                                ts_error,
                                t_sql,
                                t_error_message
                            )
                            SELECT
                                i_id_batch,
                                p_i_id_source,
                                v_schema_name,
                                v_table_name,
                                v_r_statements.t_pk_data as t_table_pkey,
                                t_binlog_name,
                                i_binlog_position,
                                clock_timestamp(),
                                quote_literal(v_r_statements.t_sql) as t_sql,
                                format('%s - %s',SQLSTATE, SQLERRM) as t_error_message
                            FROM
                                sch_chameleon.t_log_replica  log
                            WHERE
                                log.i_id_event=v_r_statements.i_id_event
                        ;
                IF p_b_exit_on_error
                THEN
                    v_ty_status.b_continue:=FALSE;
                    v_ty_status.b_error:=TRUE;
                    RETURN v_ty_status;
                ELSE

                    RAISE NOTICE 'Statement %', v_r_statements.t_sql;
                    UPDATE sch_chameleon.t_replica_tables
                        SET
                            b_replica_enabled=FALSE
                    WHERE
                            v_schema_name=v_r_statements.v_schema_name
                        AND v_table_name=v_r_statements.v_table_name
                    ;

                    RAISE NOTICE 'Deleting the log entries for the table %.% ',v_r_statements.v_schema_name,v_r_statements.v_table_name;
                    DELETE FROM sch_chameleon.t_log_replica  log
                    WHERE
                            v_table_name=v_r_statements.v_table_name
                        AND v_schema_name=v_r_statements.v_schema_name
                        AND   i_id_batch=v_i_id_batch
                    ;
                END IF;
            END;
        END LOOP;
        IF v_ts_evt_source IS NOT NULL
        THEN
            UPDATE sch_chameleon.t_last_replayed
                SET
                    ts_last_replayed=v_ts_evt_source
            WHERE
                i_id_source=p_i_id_source
            ;
        END IF;
        IF v_i_replayed=0 AND v_i_ddl=0
        THEN
            DELETE FROM sch_chameleon.t_log_replica
            WHERE
                    i_id_batch=v_i_id_batch
            ;

            GET DIAGNOSTICS v_i_skipped = ROW_COUNT;
            RAISE DEBUG 'SKIPPED ROWS: % ',v_i_skipped;

            UPDATE ONLY sch_chameleon.t_replica_batch
            SET
                b_replayed=True,
                i_skipped=v_i_skipped,
                ts_replayed=clock_timestamp()

            WHERE
                i_id_batch=v_i_id_batch
            ;

            DELETE FROM sch_chameleon.t_batch_events
            WHERE
                i_id_batch=v_i_id_batch
            ;

            v_ty_status.b_continue:=FALSE;
        ELSE
            UPDATE ONLY sch_chameleon.t_replica_batch
            SET
                i_ddl=coalesce(i_ddl,0)+v_i_ddl,
                i_replayed=coalesce(i_replayed,0)+v_i_replayed,
                i_skipped=v_i_skipped,
                ts_replayed=clock_timestamp()

            WHERE
                i_id_batch=v_i_id_batch
            ;

            UPDATE sch_chameleon.t_batch_events
                SET
                    i_id_event = v_i_evt_queue
            WHERE
                i_id_batch=v_i_id_batch
            ;

            DELETE FROM sch_chameleon.t_log_replica
            WHERE
                    i_id_batch=v_i_id_batch
                AND   i_id_event=ANY(v_i_evt_replay)
            ;
            v_ty_status.b_continue:=TRUE;
            RETURN v_ty_status;
        END IF;
        v_i_id_batch:= (
            SELECT
                bat.i_id_batch
            FROM
                sch_chameleon.t_replica_batch bat
                INNER JOIN  sch_chameleon.t_batch_events evt
                ON
                    evt.i_id_batch=bat.i_id_batch
            WHERE
                    bat.b_started
                AND bat.b_processed
                AND NOT bat.b_replayed
                AND bat.i_id_source=p_i_id_source
            ORDER BY
                bat.ts_created
            LIMIT 1
            )
        ;

        IF v_i_id_batch IS NOT NULL
        THEN
            v_ty_status.b_continue:=TRUE;
        END IF;


        RETURN v_ty_status;



    END;

$BODY$
LANGUAGE plpgsql;

--CUSTOM AGGREGATES
CREATE OR REPLACE FUNCTION  sch_chameleon.fn_binlog_min(text[],text[])
RETURNS text[] AS
$BODY$
    SELECT
        CASE
            WHEN $1=array[0,0]::TEXT[]
            THEN $2
            WHEN (string_to_array($1[1],'.'))[2]::integer>(string_to_array($2[1],'.'))[2]::integer --$1[1]>$2[1]
            THEN $2
            WHEN $1[1]=$2[1] and $1[2]::integer>=$2[2]::integer
            THEN $2
            ELSE $1

        END
    ;
$BODY$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION  sch_chameleon.fn_binlog_max(text[],text[])
RETURNS text[] AS
$BODY$
    SELECT
        CASE
            WHEN $1=array[0,0]::TEXT[]
            THEN $2
            WHEN (string_to_array($2[1],'.'))[2]::integer>(string_to_array($1[1],'.'))[2]::integer
            THEN $2
            WHEN (string_to_array($2[1],'.'))[2]::integer<(string_to_array($1[1] ,'.'))[2]::integer
            THEN $1
            WHEN (string_to_array($2[1],'.'))[2]::integer=(string_to_array($1[1],'.'))[2]::integer AND $2[2]::integer>=$1[2]::integer
            THEN $2
            ELSE $1
        END
    ;
$BODY$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sch_chameleon.fn_binlog_max_final(text[])
RETURNS text[] as
$BODY$
    SELECT
        CASE
            WHEN $1=array['','']
            THEN NULL
        ELSE
            $1
        END;
$BODY$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION sch_chameleon.fn_binlog_min_final(text[])
RETURNS text[] as
$BODY$
    SELECT $1;
$BODY$
LANGUAGE sql;




CREATE AGGREGATE sch_chameleon.binlog_max(text[])
(
    SFUNC = sch_chameleon.fn_binlog_max,
    STYPE = text[],
    FINALFUNC = sch_chameleon.fn_binlog_max_final,
    INITCOND = '{0,0}'
);


CREATE AGGREGATE sch_chameleon.binlog_min(text[])
(
    SFUNC = sch_chameleon.fn_binlog_min,
    STYPE = text[],
    FINALFUNC = sch_chameleon.fn_binlog_min_final,
    INITCOND = '{0,0}'
);


--VIEWS
CREATE OR REPLACE VIEW sch_chameleon.v_version
 AS
    SELECT '2.0.10'::TEXT t_version
;

CREATE OR REPLACE VIEW sch_chameleon.v_idx_cons
AS
SELECT
    COALESCE(v_constraint_type,'i') v_constraint_type,
    CASE WHEN v_constraint_type IS NULL
    THEN
        format('DROP INDEX %I.%I;',v_schema_name,v_index_name)
    ELSE
         format('ALTER TABLE %I.%I DROP CONSTRAINT %I;',v_schema_name,v_table_name,v_index_name)
    END AS t_sql_drop,
    CASE WHEN v_constraint_type IS NULL
    THEN
        format('%s %s; SET default_tablespace=DEFAULT;',
                CASE WHEN v_index_tablespace IS NOT NULL
                THEN
                    format('SET default_tablespace=%I;',v_index_tablespace)
                END,
                v_index_def
            )
    ELSE
         format('%s ALTER TABLE %I.%I ADD CONSTRAINT %I %s; SET default_tablespace=DEFAULT;',
            CASE WHEN v_index_tablespace IS NOT NULL
            THEN
                format('SET default_tablespace=%I;',v_index_tablespace)
            END,
             v_schema_name,
             v_table_name,
             v_index_name,
             v_constraint_def
         )
    END AS t_sql_create,
    v_index_name,
    v_table_name,
    v_schema_name
FROM
(
    SELECT distinct
        idx.tablename AS v_table_name ,
        idx.schemaname AS v_schema_name,
        idx.indexname AS v_index_name,
        idx.indexdef AS v_index_def,
        pg_get_constraintdef(con.oid_conid) AS v_constraint_def,
        idx.tablespace AS v_index_tablespace,
        con.oid_conid,
        v_constraint_type

    FROM
        pg_indexes idx
        LEFT OUTER JOIN
        (
            SELECT
                con.oid AS oid_conid,
                tab.relname AS v_table_name,
                sch.nspname AS v_schema_name,
                con.conname AS v_constraint_name,
                con.contype AS v_constraint_type
            FROM
                pg_constraint con
                INNER JOIN pg_class tab
                ON tab.oid= con.conrelid
                INNER JOIN pg_namespace sch
                ON sch."oid" = tab.relnamespace
            WHERE con.contype IN ('p','u')
        ) con
        ON
                con.v_table_name=idx.tablename
            AND con.v_schema_name=idx.schemaname
            AND con.v_constraint_name=idx.indexname
) idx_con
;


CREATE OR REPLACE VIEW sch_chameleon.v_fkeys AS
SELECT
    v_schema_referenced,
    v_table_referenced,
    v_schema_referencing,
    v_table_referencing,
    format('ALTER TABLE ONLY %I.%I DROP CONSTRAINT %I ;',v_schema_referencing,v_table_referencing ,v_fk_name) AS t_con_drop,
    format('ALTER TABLE ONLY %I.%I ADD CONSTRAINT %I %s  NOT VALID ;',v_schema_referencing,v_table_referencing,v_fk_name,v_fk_definition) AS t_con_create,
    format('ALTER TABLE ONLY %I.%I VALIDATE CONSTRAINT %I ;',v_schema_referencing,v_table_referencing ,v_fk_name) AS t_con_validate,
    v_fk_name

FROM
(
SELECT
    tab.relname AS v_table_referenced,
    sch.nspname AS v_schema_referenced,
    pg_get_constraintdef(con.oid) AS v_fk_definition,
    tabref.relname AS v_table_referencing,
    schref.nspname AS v_schema_referencing,
    con.conname AS v_fk_name

FROM
    pg_class tab
    INNER JOIN pg_namespace  sch
    ON sch.oid=tab.relnamespace
    INNER JOIN pg_constraint con
    ON con.confrelid=tab.oid
    INNER JOIN pg_class tabref
    ON tabref.oid=con.conrelid
    INNER JOIN pg_namespace  schref
    ON schref.oid=tabref.relnamespace
WHERE
        tab.relkind='r'
    AND con.contype='f'
) fk
;

