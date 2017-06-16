RELEASE NOTES
*************************

Version 1.3 
--------------------------

The initial implementation for the relay data was to have two log tables t_log_replica_1 and t_log_replica_2 with the
replica process accessing one table at a time. 

This approach allows autovacuum to take care of the unused partition meanwhile the other is written. 
The method worked fine with only one replica worker. However as the flip flop between the tables is calculated indepentently 
for each source this could cause unwanted bloat on the log tables if several sources are replicating all togheter.
In this scenario autovacuum will struggle to truncate the empty space in the table's end.

The pg_chameleon version 1.3 implements the log tables per source. Each source have a dedicated couple of tables still inherited from 
the root partition t_log_replica. 

Upgrade
.....................................

pg_chameleon comes with an integrated schema migrator. The upgrade from the version
