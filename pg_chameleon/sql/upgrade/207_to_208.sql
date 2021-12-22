-- upgrade catalogue script 2.0.7 to 2.0.8
--TABLE
CREATE TABLE sch_chameleon.t_ukeys
    (
        i_id_ukey bigserial,
        v_schema_name varchar NOT NULL,
        v_table_name varchar NOT NULL,
        v_index_name varchar NOT NULL,
        t_ukey_drop text NULL,
        t_ukey_create text NULL,
        CONSTRAINT pk_t_ukeys PRIMARY KEY (i_id_ukey)
    );
CREATE UNIQUE INDEX idx_t_ukeys_table_schema ON sch_chameleon.t_ukeys USING btree(v_schema_name,v_table_name,v_index_name);

--DROP VIEW
DROP VIEW IF EXISTS sch_chameleon.v_idx_pkeys ;

--VIEW
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