RELEASE NOTES
*************************

Version 1.3 
--------------------------

sync_replica disabled
.....................................

The sync_replica command have some unexpected behaviour when running in single table mode.
As the implications need correct addressing with this release the command is temporarly disabled. 

Change in replica storage
.....................................
The initial implementation for the relay data was to have two log tables t_log_replica_1 and t_log_replica_2 with the
replica process accessing one table at a time. 

This approach allows autovacuum to take care of the unused partition meanwhile the other is written. 
The method worked fine with only one replica worker. However as the flip flop between the tables is calculated indepentently 
for each source this could cause unwanted bloat on the log tables if several sources are replicating all togheter.
In this scenario autovacuum will struggle to truncate the empty space in the table's end.

The pg_chameleon version 1.3 implements the log tables per source. Each source have a dedicated couple of tables still inherited from 
the root partition t_log_replica. 

The schema is migrated at the first run after the upgrade by pg_chameleon's integrated schema migrator. 
The upgrade scripts are installed in the python specific site-packages directory. 

For example if have a python 3.6 virtualenv  in the directory **~/venv** you'll find the upgrade files in 
**~/venv/lib/python-3.6/site-packages/pg_chameleon/sql/upgrade/**

The migration performs the following operations.

* add a field v_log_table to t_sources
* add an helper plpgsql function fn_refresh_parts() which creates the source's log tables if not present
* with a DO block creates the new log tables for the existing sources and copies the data from the old t_log_replica_x to the new log tables
* **drops the old log tables**
* removes the field v_log_table from t_replica_batch

Upgrade
--------------------------

**please read carefully before attempting any upgrade**

The schema upgrade  happen automatically at the first run. 
Because this one involves a data copy could take more time than the usual. If the process seems frozen **do not stop it otherwise you may lose your replica setup** .

Upgrade steps

* Stop all the replica sources. The show_status command must show all the rows in stopped status
* Take a backup of the schema sch_chameleon with pg_dump
* Upgrade pg_chameleon with ``pip install pg_chameleon --upgrade``
* Run ``chameleon.py upgrade_schema --config <your_config> --debug``
* When the upgrade is finished start the replica process as usual

Rollback
.....................................

If something goes wrong in the upgrade process you shall restore the sch_chameleon's backup, 
Then you should downgrade the installation to pg_chameleon 1.2 and start the replica as usual.


