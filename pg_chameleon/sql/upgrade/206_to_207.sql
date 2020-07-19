-- upgrade catalogue script 2.0.6 to 2.0.7
--TABLES
CREATE TABLE sch_chameleon.t_indexes
    (
        i_id_index	bigserial,
        i_id_table	bigint NOT NULL,
        v_index_name varchar NOT NULL,
        t_index_drop text NULL,
        t_index_create text NULL,
        CONSTRAINT pk_t_indexes PRIMARY KEY (i_id_index),
        CONSTRAINT fk_t_indexes_t_table FOREIGN KEY (i_id_table) REFERENCES sch_chameleon.t_replica_tables (i_id_table)
            ON UPDATE RESTRICT ON DELETE CASCADE
    );
 CREATE UNIQUE INDEX idx_t_indexes_id_table_idx_name ON sch_chameleon.t_indexes USING btree(i_id_table,v_index_name);

CREATE TABLE sch_chameleon.t_pkeys
    (
        i_id_pkey	bigserial,
        i_id_table	bigint NOT NULL,
        v_index_name varchar NOT NULL,
        t_pkey_drop text NULL,
        t_pkey_create text NULL,
        CONSTRAINT pk_t_pkeys PRIMARY KEY (i_id_pkey),
        CONSTRAINT fk_t_pkeys_t_table FOREIGN KEY (i_id_table) REFERENCES sch_chameleon.t_replica_tables (i_id_table)
            ON UPDATE RESTRICT ON DELETE CASCADE
    );
 CREATE UNIQUE INDEX idx_t_pkeys_id_table ON sch_chameleon.t_pkeys USING btree(i_id_table);

CREATE TABLE sch_chameleon.t_fkeys
    (
        i_id_fkey	bigserial,
        i_id_table	bigint NOT NULL,
        v_constraint_name varchar NOT NULL,
        t_fkey_drop text NULL,
        t_fkey_create text NULL,
        CONSTRAINT pk_t_fkeys PRIMARY KEY (i_id_fkey),
        CONSTRAINT fk_i_id_fkey_t_table FOREIGN KEY (i_id_table) REFERENCES sch_chameleon.t_replica_tables (i_id_table)
            ON UPDATE RESTRICT ON DELETE CASCADE
    );
 CREATE UNIQUE INDEX idx_i_id_fkey_id_table_v_constraint_name ON sch_chameleon.t_fkeys USING btree(i_id_table,v_constraint_name);



--VIEWS
CREATE OR REPLACE VIEW sch_chameleon.v_idx_pkeys
AS
SELECT
    oid_conid IS NOT NULL AS b_idx_pkey,
    CASE WHEN oid_conid IS NULL
    THEN
        format('DROP INDEX %I.%I;',v_schema_name,v_index_name)
    ELSE
         format('ALTER TABLE %I.%I DROP CONSTRAINT %I;',v_schema_name,v_table_name,v_index_name)
    END AS t_sql_drop,
    CASE WHEN oid_conid IS NULL
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
    v_index_name

FROM
(
    SELECT distinct
        idx.tablename AS v_table_name ,
        idx.schemaname AS v_schema_name,
        idx.indexname AS v_index_name,
        idx.indexdef AS v_index_def,
        pg_get_constraintdef(con.oid_conid) AS v_constraint_def,
        idx.tablespace AS v_index_tablespace,
        con.oid_conid

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
            WHERE con.contype='p'
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
    format('ALTER TABLE ONLY %I.%I VALIDATE CONSTRAINT %I ;',v_schema_referencing,v_table_referencing ,v_fk_name) AS t_con_validate


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
