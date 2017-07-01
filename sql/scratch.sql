select
logdat[1] as prefix,
logdat[2]::integer as sequence
from
(
    select 
        string_to_array(t_binlog_name,'.') logdat
    from 
        sch_chameleon.t_replica_batch
) log
