CREATE OR REPLACE FUNCTION  sch_chameleon.fn_binlog_pos(integer[],integer[])
RETURNS integer[] AS
$BODY$
	DECLARE
		binlog_max   ALIAS FOR $1;
		binlog_value ALIAS FOR $2;
		binlog_state integer[];
	BEGIN
		IF binlog_value[1]>binlog_max[1] 
		THEN
			binlog_state:=binlog_value;
		ELSEIF binlog_value[1]<binlog_max[1] 
		THEN
			binlog_state:=binlog_max;
		ELSEIF binlog_value[1]=binlog_max[1] AND binlog_value[2]>=binlog_max[2]
		THEN
			binlog_state:=binlog_value;
		ELSE
			binlog_state:=binlog_max;
		END IF;

		RETURN binlog_state;
		
	END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sch_chameleon.fn_binlog_final(integer[])
RETURNS integer[] as 
$BODY$
	SELECT 
		CASE 
			WHEN ( $1[1] = 0 AND $1[2] = 0 )
		THEN NULL
		ELSE $1
	END;
$BODY$
LANGUAGE sql;


create aggregate sch_chameleon.binlog_max(integer[]) 
(
    SFUNC = sch_chameleon.fn_binlog_pos,
    STYPE = integer[],
    FINALFUNC = sch_chameleon.fn_binlog_final,
    INITCOND = '{0,0}'
);


SELECT sch_chameleon.binlog_max(array[(string_to_array(t_binlog_name,'.'))[2]::integer,i_binlog_position]) FROM sch_chameleon.t_replica_tables;
--SELECT  sch_chameleon.fn_binlog_final(array[0,0])