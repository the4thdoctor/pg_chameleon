RELEASE NOTES
*************************
Version 1.8.2
--------------------------
The version 1.8.2 is the bugfix for the final release 1.8 of the version 1. 
Not many bugfixes, mostly backports from the version 2.0, currently in alpha release.
This release upgrades the replica catalogue to the version 1.7 which adds a new field to the ``t_sources`` table.
This field ``t_source_schema`` is used only for the migration to the version 2.0.0 and is updated every time a sourceid is requested
from the classpg_engine.

Version 1.8.1
--------------------------
The version 1.8.1 is the bugfix for the final release 1.8 of the version 1. 
Several bugfixes come with this release involving wrong escape and not sanitised types in the MySQL connection string.

The version 1 is not actively developed anymore and  will reach the end of life when the version 2 will release a stable version. 



Version 1.8
--------------------------
The version 1.8 fixes a wrong check when evaluating if the threads are alive.
It also adds the support for the RENAME, in order to make simpler the usage of the percona online schema change tool.
This should be the final release of the version 1. For this version I will focus only on bug fixes from now.

The upcoming release 2's development is already started and a pre-alpha version should appear in the next month or so.



Version 1.7
--------------------------
The version 1.7 supports the optional threaded read and replay. To enable the threaded execution just add ``--thread`` when running start_replica. 

e.g. ``chameleon.py start_replica --config default --thread``

The date fields with the values ``0000-00-00 00:00:00`` are set to ``NULL`` when the replica is initialised, reflecting the same behaviour of the mysql python replica library.

This release adds a basic handling of data type override during the init replica and the ddl parsing as requested in `Issue #21 <https://github.com/the4thdoctor/pg_chameleon/issues/21>`_.


The variable type_override maps the mysql data types with the corresponding postgresql data type and the tables where the override is applied.
The following example overrides tinyint(1) to boolean for all tables and tinyint(3) to smallint only for the tables foo and bar.

.. code-block:: yaml

    type_override:
      "tinyint(1)":
        override_to: boolean
        override_tables:
            - "*"
      "tinyint(3)":
        override_to: smallint
        override_tables:
            - "foo"
	    - "bar"


As there is no validation for the data type when replied if **any incompatible value is sent trough this mechaninsm the replica will break**.

Several improvements have been made for ddl parsing. 


Version 1.6
--------------------------
The version 1.6 rewrites completely the replay function. The original implementation used massively the SELECT INTO 
to determine the jsonb structure. This approach generates a change of memory context  **for each logged row**.
On high write databases the load imposed on the memory and the CPU was just not acceptable.
The new implementation collects the receiving row structures from the information schema and using the format function, builds the DDL on the fly
meanwhile reading from the jsonb objects. The function is also more readable and shorter than the initial version. Tests proved that the speed replay 
improved sensibly generating at same time a lower load level the CPU.

The show_status output now shows the read and replay lags. This change gives a better understanding of the replica lag, in away more similar to the MySQL's show slave command.
The change is also a preparation for the threaded read and replay feature which will appear in the version 1.7.

The version also add several bug fixes thanks to the user's feedback. 
Check the changelog for the details.

Upgrade
........................................................................
The upgrade procedure happens automatically when the chameleon.py is executed after the package's upgrade.

The change adds a new field to the log table, creates a new table used for collecting the event ids and reload the replay function.

Like for the version 1.3 before the upgrade stop the all the replica processes and take a backup of the sch_chameleon schema.

This will allow you to rollback the version if something goes wrong.




Version 1.5
--------------------------
The version 1.5 adds the support for default value for the DDL ALTER TABLE...ADD COLUMN, CHANGE and MODIFY. 
The previous implementation removed any **default** keyworkd before parsing the sql statement.

The child tables of t_log _replica have now indices on the i_id_batch field. This will speed up the cleanup for replayed batches.

Several bug fixes, check the changelog for the details.



Version 1.4 
--------------------------
sync_replica replaced by sync_tables
........................................................................
The command sync_replica is now replaced by sync_tables as this new name better reflects the concept behind the process. 
The command requires the option --table followed by a comma separated list of table names.

If the specified table is not present in the origin's schema the table is silently skipped. 
When a table is synchronised the existing copy in the target database is dropped and recreated from scratch.
In order to get the table in consistent state the log coordinates are saved in the the t_replica_tables. 
The replica process will ignore the table until the log position reaches the table's snapsot position, 
ensuring a consistent state for the replica target.


Version 1.3 
--------------------------

sync_replica disabled
.....................................

The sync_replica command do not work as expected when running in single table mode.
As the issue requires time to be fixed this release temporarly  disables the sync_replica command. 

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
........................................................................

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
=================

If something goes wrong in the upgrade process you shall restore the sch_chameleon's backup, 
Then you should downgrade the installation to pg_chameleon 1.2 and start the replica as usual.


