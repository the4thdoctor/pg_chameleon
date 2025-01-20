RELEASE NOTES
*************************

2.0.21
--------------------------
* PR #163 provided by @bukem providing an optimization of the procedure for applying changes to Postgresql
* Fix for issue #170. Now if the replica schema is not present on the target database it will display an hint instead of an exception
* Fix incorrect placement of the new parameter **net_read_timeout**. Now it's set as an instance variable from global_lib.py. The previous configuration may cause a crash of the replica process.

This release requires a replica catalogue upgrade, therefore is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump as a good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.


2.0.20
--------------------------
This long past due maintenance release adds the following bugfix and improvements.

Merge the new SQL library built by @nikochiko  for the `Google Summer of Code 2023 <https://summerofcode.withgoogle.com/archive/2023/projects/VnzdAl4z>`_ .

The setup.py is now fixed for working with newer python versions thanks to @darix.

Merged PR:

* PR #169 provided by @Jamal-B Fix read and replay daemons death detection if the multiprocessing queue is empty
* PR #171 provided by @JasonLiu1567 fix issue #111 Data lost when exceed batch size
* PR #173 provided by @acarapetis Ignore MySQL indices with prefix key parts.
  Btree indices on prefix keys (e.g. col_name(5) meaning the first 5 characters of "col_name") were previously
  being replicated as indices on the full column causing errors in the case of columns with values were too wide to fit
  in indices. As PostgreSQL doesn't have such indices then they are now ignored.

**DEPRECATION NOTICE**

The support for rollbar is now DEPRECATED and will be dropped in the upcoming releases.
A new generic notification support may appear in the next development cycle.

2.0.19
--------------------------
This maintenance release adds the following bugfix and improvements.

Merge pull request #144 adding mysql-replication support for PyMySQL>0.10.0 was introduced in v0.22
Adds support for fillfactor when running init_replica, it's now possible to specify the fillfactor for the tables when running init_replica.
Useful to mitigate bloat in advance when replicating/migrating from MySQL.

Improve logging on discarded rows, now the discarded row image is displayed in the log.

Add distinct on group concat when collecting foreign keys metadata to avoid duplicate fields in the foreign key definition.

Use mysql-replication>=0.31, this fix the crash when replicating from MariaDB introduced in  mysql-replication 0.27


2.0.18
--------------------------
This maintenance release adds the following bugfix and improvements.

Adds a new method `copy_schema` to copy only the schema without the data (EXPERIMENTAL).

Adds the support for the **ON DELETE** and **ON UPDATE** clause when creating the foreign keys in PostgreSQL with `detach_replica`
and `copy_schema`.

When running `init_replica` or `copy_schema` the names for the indices and foreign keys are preserved.
Only if there is any duplicate name then pg_chameleon will ensure that the names on PostgreSQL are unique within the same schema.

Adds a workaround for a regression introduced in **mysql-replication** by forcing the version to be lesser than 0.27.


Change the data type for the identifiers stored into the replica schema to varchar(64)

This release requires a replica catalogue upgrade, therefore is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump as a good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.

2.0.17
--------------------------
This maintenance release adds the following bugfix.

Fix the wrong order in copy data/create indices when keep_existing_schema is **No**.

Previously the indices were created before the data was loaded into the target schema with great performance degradation.

This fix applies only if the parameter keep_existing_schema is set to **No**.

Add the collect for unique constraints when keep_existing_schema is **Yes**.

Previously the unique constraint were not collected or dropped if defined as constraints instead of indices.

This fix applies only if the parameter keep_existing_schema is set to **Yes**.

This release adds the following changes:

* Remove argparse from the requirements as now it's part of the python3 core dist
* Remove check for log_bin when we replicate from Aurora MySQL
* Manage different the different behaviour in pyyaml to allow pg_chameleon to be installed as rpm in centos 7 via pgdg repository

This release works with Aurora MySQL. However Aurora MySQL 5.6 segfaults when FLUSH TABLES WITH READ LOCK is issued.

The replica is tested on Aurora MySQL 5.7.

This release requires a replica catalogue upgrade, therefore is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump as a good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.

2.0.16
--------------------------
This maintenance release fix a crash in init_replica caused by an early disconnection during the fallback on insert.
This caused the end of transaction to crash aborting the init_replica entirely.


2.0.15
--------------------------
This maintenance release adds the support for reduced lock if MySQL engine is transactional, thanks to @rascalDan.

The init_replica process checks whether the engine for the table is transactional and runs the initial copy within a transaction.
The process still requires a FLUSH TABLES WITH READ LOCK but the lock is released as soon as the transaction snapshot is acquired.
This improvement allows pg_chameleon to run agains primary databases with minimal impact during the init_replica process.

The python-mysql-replication requirement is now changed to version >=0.22. This release adds support for PyMySQL >=0.10.0.
The requirement for PyMySQL to version <0.10.0 is therefore removed from setup.py.

From this version pg_chameleon refuse to run as root.

2.0.14
--------------------------
This maintenance release improves the support for spatial datatypes.
When postgis is installed on the target database then the spatial data types
``point``,``geometry``,``linestring``,``polygon``, ``multipoint``, ``multilinestring``, ``geometrycollection`` are converted to
geometry and the data is replicated using the Well-Known Binary (WKB) Format. As the MySQL implementation for WKB is not standard pg_chameleon
removes the first 4 bytes from the decoded binary data before sending it to PostgreSQL.

When ``keep_existing_schema`` is set to ``yes`` now drops and recreates indices, and primary keys during the ``init_replica`` process.
The foreign keys are dropped as well and recreated when the replica reaches the consistent status.
This way the ``init_replica`` may complete successfully even when there are foreign keys in place and with the same speed of the usual ``init_replica``.

The setup.py now forces PyMySQL to version <0.10.0 because it breaks the python-mysql-replication library (issue #117).

Thanks to @porshkevich which fixed issue #115 by trim the space from PK index name.

This release requires a replica catalogue upgrade, therefore is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump as a good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.

If the upgrade procedure can't upgrade the replica catalogue because of running or errored replicas is it possible to reset the statuses by
using the command ``chameleon enable_replica --source <source_name>``.

If the catalogue upgrade is still  not possible then you can downgrade pgchameleon to the previous version. Please note that you may need to
install manually PyMySQL to fix the issue with the version 0.10.0.

``pip install pg_chameleon==2.0.13``

``pip install "PyMySQL<0.10.0"``




2.0.13
--------------------------
This maintenance release adds the **EXPERIMENTAL** support for Point datatype thanks to the contribution by @jovankricka-everon.

The support is currently limited to only the POINT datatype with hardcoded stuff to keep the init_replica and the replica working.
However as this feature is related with PostGIS, the next point release will rewrite this part of code using a more general approach.

The release adds the ``keep_existing_schema`` parameter in the MySQL source type. When set to ``Yes`` init_replica,refresh_schema and
sync_tables do not recreate the affected tables using the data from the MySQL source.
Instead the existing tables are truncated and the data is reloaded.

A REINDEX TABLE is executed in order to have the indices in good shape after the reload.
The next point release will very likely improve the approach on the reload and reindexing.

When ``keep_existing_schema`` is set to Yes the parameter ``grant_select_to`` have no effect.

From this release the codebase switched from tabs to spaces, following the guidelines in PEP-8.

2.0.12
--------------------------
This maintenance release fixes the issue #96 where the replica initialisation failed on MySQL 8 because of the wrong field names pulled out from the information_schema.
Thanks to @daniel-qcode for contributing with his fix.

The configuration and SQL files are now moved inside into the directory pg_chameleon. This change simplifies the setup.py file and allow pg_chameleon to be
built as source and wheel package.

As python 3.4 has now reached its end-of-life and has been retired the minimum requirement for pg_chameleon has been updated to Python 3.5.

2.0.11
--------------------------
This maintenance release fixes few things.
As reported in #95 the yaml filles were not completely valid. @rebtoor fixed them.

@clifff made a pull request to have the start_replica running in foreground when log_file set to `stdout`.
Previously the process remained in background with the log set to `stdout`.

As Travis seems to break down constantly the CI configuration is disabled until a fix or a different CI is found .

Finally the method which loads the yaml file is now using an explicit loader as required by the new PyYAML version.

Previously with newer version of PyYAML there was a warning emitted by the library because the default loader is unsafe.
If you have

2.0.10
--------------------------
This maintenance release  fixes a  regression caused by the new replay function with PostgreSQL 10. The unnested primary key was put in cartesian product with the
json elements generating NULL identifiers which made the subsequent format function to fail.

This release fixes adds a workaround for decoding the keys in the mysql's json fields. This allows the sytem to replicate the json data type as well.

The command ``enable_replica`` fixes a race condition when the maintenance flag is not returned to false (e.g. an application crash during the maintenance run) allowing the replica to start again.


The tokeniser for the ``CHANGE`` statement now parses the tables in the form of ``schema.table``. However the tokenised schema is not used to determine the
query's schema because the ``__read_replica_stream`` method uses the schema name pulled out from the mysql's binlog.


As this change requires a replica catalogue upgrade is very important to follow the upgrade instructions provided below.


* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump for good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.


If the upgrade procedure refuses to upgrade the catalogue because of running or errored replicas is possible to reset the statuses using the command ``chameleon enable_replica --source <source_name>``.

If the catalogue upgrade is still  not possible downgrading pgchameleon to the previous version. E.g. ``pip install pg_chameleon==2.0.9`` will make the replica startable again.




2.0.9
--------------------------
This maintenance release  fixes a wrong check for the next auto maintenance run if the maintenance wasn't run before.
Previously when changing the value of ``auto_maintenance`` from disabled to an interval, the process didn't run the automatic maintenance unless a manual maintenance
was executed before.

This release adds improvements on the replay function's speed. The new version is now replaying the data without accessing the parent log partition and
the decoding logic has been simplified. Not autoritative tests has shown a cpu gain of at least 10% and a better memory allocation.
However your mileage may vary.

The GTID operational mode has been improved removing the blocking mode which caused increased lag in systems with larger binlog size.

As this change requires a replica catalogue upgrade is very important to follow the upgrade instructions provided below.


* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump for good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.


If the upgrade procedure refuses to upgrade the catalogue because of running or errored replicas is possible to reset the statuses using the command ``chameleon enable_replica --source <source_name>``.

If the catalogue upgrade is still  not possible downgrading pgchameleon to the previous version. E.g. ``pip install pg_chameleon==2.0.8`` will make the replica startable again.


2.0.8
--------------------------
This maintenance release adds the support for skip events. Is now is possible to skip events (insert,delete,update) for single tables or for entire schemas.

A new optional source parameter ``skip_events:`` is available for the sources with type mysql.
Under skip events there are three keys one per each DML operation. Is possible to list an entire schema or single tables in the form of ``schema.table``.
The example snippet disables the inserts on the table ``delphis_mediterranea.foo`` and the deletes on the entire schema ``delphis_mediterranea``.

.. code-block:: yaml

    skip_events:
      insert:
        - delphis_mediterranea.foo #skips inserts on the table delphis_mediterranea.foo
      delete:
        - delphis_mediterranea #skips deletes on schema delphis_mediterranea
      update:



The release 2.0.8 adds the  **EXPERIMENTAL** support for the GTID for MySQL or Percona server. The GTID in MariaDb is currently not supported.
A new optional parameter ``gtid_enable:`` which defaults to ``No`` is available for the source type mysql.

When `MySQL is configured with the GTID <https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html>`_ and the parameter ``gtid_enable:`` is set to Yes,  pg_chameleon will use the GTID to auto position the replica stream.
This allows pg_chameleon to reconfigure the source within the MySQL replicas without the need to run init_replica.

This feature has been extensively tested but as it's new has to be considered  **EXPERIMENTAL**.


ALTER TABLE RENAME is now correctly parsed and executed.
ALTER TABLE MODIFY is now parsed correctly when the field have a default value. Previously modify with default values would parse wrongly and fail when translating to PostgreSQL dialect

The source no longer gets an error state when  running with ``--debug``.

The logged events are now cleaned when refreshing schema and syncing tables. Previously spurious logged events could lead to primary key violations when syncing single tables or refreshing single schemas.

As this change requires a replica catalogue upgrade is very important to follow the upgrade instructions provided below.


* If working via ssh is suggested to use screen or tmux for the upgrade
* Stop all the replica processes with ``chameleon stop_all_replicas --config <your_config>``
* Take a backup of the schema ``sch_chameleon`` with pg_dump for good measure.
* Install the upgrade with ``pip install pg_chameleon --upgrade``
* Check if the version is upgraded with ``chameleon --version``
* Upgrade  the replica schema with the command ``chameleon upgrade_replica_schema --config <your_config>``
* Start all the replicas.


If the upgrade procedure refuses to upgrade the catalogue because of running or errored replicas is possible to reset the statuses using the command ``chameleon enable_replica --source <source_name>``.

If the catalogue upgrade is still  not possible downgrading pgchameleon to the previous version. E.g. ``pip install pg_chameleon==2.0.7`` will make the replica startable again.


2.0.7
--------------------------
This maintenance release makes the multiprocess logging safe. Now each replica process logs in a separate file.

The ``--full`` option now is working. Previously the option had no effect causing the maintenance to run always a conventional vacuum.

This release fixes the issues reported  in ticket #73 and #75 by pg_chameleon's users.

The bug reported in ticket #73 caused a wrong data type tokenisation when an alter table adds a column with options (e.g. ``ADD COLUMN foo DEFAULT NULL``)

The bug reported in ticket #75 , caused a wrong conversion to string for the row keys with None value  during the cleanup of malformed rows for the init replica and the replica process.

A fix for the TRUNCATE TABLE tokenisation is implemented as well. Now if the statement specifies the table with the schema the truncate works properly.

A new optional source's parameter is added. ``auto_maintenance``  trigger a vacuum on the log tables after a specific timeout.
The timeout shall be expressed like a PostgreSQL interval (e.g. "1 day"). The special value "disabled" disables the auto maintenance.
If the parameter is omitted the auto maintenance is disabled.



2.0.6
--------------------------
The maintenance release 2.0.6 fixes a crash occurring when a new column is added on the source database with the default value ``NOW()``.

The maintenance introduced in the version 2.0.5 is now less aggressive.
In particular the ``run_maintenance`` command now executes a conventional ``VACUUM`` on the source's log tables, unless the switch ``--full`` is specified. In that case a ``VACUUM FULL`` is executed.
The detach has been disabled and may be completely removed in the future releases because very fragile and prone to errors.

However running VACUUM FULL on the log tables can cause  the other sources to be blocked during the maintenance run.

This release adds an optional parameter ``on_error_read:``  on the mysql type's sources which allow the read process to stay up if the mysql database is refusing connections (e.g. MySQL down for maintenance).
Following the  principle of least astonishment the parameter if omitted doesn't cause any change of behaviour. If added with the value continue (e.g. ``on_error_read: continue``)
will prevent the replica process to stop in the case of connection issues from the MySQL database with a warning is emitted on the replica log .

This release adds the support for mysql 5.5 which doesn't have the parameter ``binlog_row_image``.

``enable_replica`` now can reset the replica status to ``stopped`` even if the catalogue version is mismatched.
This simplifies the upgrade procedure in case of errored or wrongly running replicas.

As this change requires a replica catalogue upgrade is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to open a screen session
* Before upgrading pg_chameleon **stop all the replica processes.**
* Upgrade the pg_chameleon package with `pip install pg_chameleon --upgrade`
* Upgrade  the replica schema with the command `chameleon upgrade_replica_schema --config <your_config>`
* Start the replica processes

If the upgrade procedure refuses to upgrade the catalogue because of running or errored replicas is possible to reset the statuses with the ``enable_replica`` command.

If the catalogue upgrade is still  not possible downgrading pgchameleon to the version 2.0.5 with ``pip install pg_chameleon==2.0.5`` should make the replicas startable again.

2.0.5
--------------------------
The maintenance release 2.0.5 fixes a regression which prevented some tables to be synced with `sync_tables` when the parameter `limit_tables` was set.
Previously having two or more schemas mapped with only one schema listed in `limit_tables` prevented the other schema's tables to be synchronised with `sync_tables`.

This release add two new commands to improve the general performance and the management.

The command `stop_all_replicas` stops all the running sources within the target postgresql database.

The command `run_maintenance` performs a VACUUM FULL on the specified source's log tables.
In order to limit the impact on other sources eventually configured the command performs the following steps.

* The read and replay processes for the given source are paused
* The log tables are detached from the parent table `sch_chameleon.t_log_replica` with the command `NO INHERIT`
* The log tables are vacuumed with `VACUUM FULL`
* The log tables are attached to the parent table `sch_chameleon.t_log_replica` with the command `INHERIT`
* The read and replay processes are resumed

Currently the process is manual but it will become eventually automated if it's proven to be sufficiently robust.

The pause for the replica processes creates the infrastructure necessary to have a self healing replica.
This functionality will appear in future releases of the branch 2.0.

As this change requires a replica catalogue upgrade is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to open a screen session
* Before the upgrade stop all the replica processes.
* Upgrade pg_chameleon with `pip install pg_chameleon --upgrade`
* Run the upgrade command `chameleon upgrade_replica_schema --config <your_config>`
* Start the replica processes


2.0.4
--------------------------
The maintenance release 2.0.4 fix the wrong handling of the ``ALTER TABLE`` when generating the ``MODIFY`` translation.
The regression was added in the version 2.0.3 and can result in a broken replica process.

This version improves the way to handle the replica from tables with dropped columns in the future.
The `python-mysql-replication library with this commit <https://github.com/noplay/python-mysql-replication/commit/4c48538168f4cd3239563393a29b542cc6ffcf4b>`_ adds a way to
manage the replica with the tables having columns dropped before the read replica is started.

Previously the auto generated column name caused the replica process to crash as the type map dictionary didn't had the corresponding key.

The version 2.0.4 handles the ``KeyError`` exception and allow the row to be stored on the PostgreSQL target database.
However this will very likely cause the table to be removed from the replica in the replay step. A debug log message is emitted when this happens in order to
when the issue occurs.

2.0.3
--------------------------
The bugfix release 2.0.3 fixes the issue #63 changeing all the fields  `i_binlog_position` to bigint. Previously binlog files larger than 2GB would cause an integer overflow during the phase of write rows in the PostgreSQL database.
The issue can affect also MySQL databases with smaller `max_binlog_size` as it seems that this value is a soft limit.

As this change requires a replica catalogue upgrade is very important to follow the upgrade instructions provided below.

* If working via ssh is suggested to open a screen session
* Before the upgrade stop all the replica processes.
* Upgrade pg_chameleon with `pip install pg_chameleon --upgrade`
* Run the upgrade command `chameleon upgrade_replica_schema --config <your_config>`
* Start the replica processes

Please note that because the upgrade command will alter the data types with subsequent table rewrite.
The process can take long time, in particular if the log tables are large.
If working over a remote machine the best way to proceed is to run the command in a screen session.


This release fixes a regression introduced with the release 2.0.1.
When an alter table comes in the form of `ALTER TABLE ADD COLUMN is in the form datatype DEFAULT (NOT) NULL` the parser captures two words instead of one,
causing the  replica process crash.

The speed of the initial cleanup, when the replica starts has been improved as now the delete runs only on the sources log tables instead of the parent table.
This improvement is more effective when many sources are configured all togheter.

From this version the setup.py switches the psycopg2 requirement to using the psycopg2-binary which ensures that psycopg2 will install using the wheel package when available.



2.0.2
--------------------------
This bugfix relase adds a missing functionality which wasn't added during the application development and fixes a bug in the ``sync_tables`` command.

Previously the  parameter ``batch_retention`` was ignored making the replayed batches to accumulate in the table ``sch_chameleon.t_replica_batch``
with the conseguent performance degradation over time.

This release solves the issue re enabling the batch_retention.
Please note that after upgrading there will be an initial replay lag building.
This is normal as the first cleanup will have to remove a lot of rows.
After the cleanup is complete the replay will resume as usual.

The new private method ``_swap_enums`` added to the class ``pg_engine`` moves the enumerated types from the loading schema to the destination schema
when the method ``swap_tables`` is executed by the command ``sync_tables``.

Previously when running ``sync_tables`` tables with enum fields were created on PostgreSQL without the corresponding enumerated types.
This happened because the custom enumerated type were not moved into the destination schema and therefore dropped along with the loading schema when the
procedure performed the final cleanup.


2.0.1
--------------------------
The first maintenance release of pg_chameleon v2 adds a performance improvement in the read replica process when
the variables limit_tables or skip_tables are set.

Previously all the rows were read from the replica stream as the ``BinLogStreamReader`` do not allow the usage of  the tables in the form of
``schema_name.table_name``. This caused a large amount of useless data hitting the replica log tables as reported in the issue #58.

The private method ``__store_binlog_event`` now evaluates the row schema and table and returns a boolean value on whether the row or query
should be stored or not into the log table.

The release fixes also a crash in read replica if an alter table added a column was of type ``character varying``.

2.0.0
--------------------------
This stable release consists of the same code of the RC1 with few usability improvements.

A new option is now available to set to set the maximum level for the messages to be sent to rollbar.
This is quite useful if we configure a periodical init_replica (e.g. pgsql source type refreshed every hour) and we don't want to fill rollbar with noise.
For example ``chameleon init_replica --source pgsql --rollbar-level critical``  will send to rollbar only messages marked as critical.

There is now a command line alias ``chameleon`` which is a wrapper for ``chameleon.py``.

A new command ``enable_replica`` is now available to enable the source's replica if the source is not stopped clean.



2.0.0.rc1
--------------------------
This release candidate comes with few bug fixes and few usability improvements.

Previously when adding a table with a replicated DDL having an unique key, the table's creation failed because of the fields were
set as NULLable . Now the command works properly.

The system now checks if the MySQL configuration allows the replica when initialising or refreshing replicated entities.

A new class ``rollbar_notifier`` was added in order to simplyfi the message management within the source and engine classes.

Now the commands ``init_replica,refresh_schema,sync_tables`` send an info notification to rollbar when they complete successfully or
an error if they don't.

The command ``sync_tables`` now allows the special name ``--tables disabled`` to have all the tables with replica disabled
re synchronised at once.


2.0.0.beta1
--------------------------
The first beta for the milestone 2.0 adds fixes a long standing bug to the replica process and adds more features to the postgresql support.

The race condition fixed was caused by a not tokenised DDL preceeded by row images, causing the collected binlog rows to be added several times to the log_table.
It was quite hard to debug as the only visible effect was a primary key violation on random tables.

The issue is caused if a set of rows lesser than the ``replica_batch_size`` are followed by a DDL that is not tokenised (e.g. ``CREATE TEMPORARY TABLE `foo`;`` )
which coincides with the end of read from the binary log.
In that case the batch is not closed and the next read replica attempt will restart from the previous position reading and storing again the same set of rows.
When the batch is closed the replay function will eventually fail because of a primary/unique key violation.

The tokeniser now works properly when an ``ALTER TABLE ADD COLUMN``'s definition is surrounded by parentheses e.g. ``ALTER TABLE foo ADD COLUMN(bar varchar(30));``
There are now error handlers when wrong table names, wrong schema names, wrong source name and wrong commands are specified to ``chameleon.py``
When running commands that require a source name tye system checks if the source is registered.

The ``init_replica`` for source pgsql now can read from an hot standby but the copy is not consistent as it's not possible to export a snapshot from the hot standbys.
Also the ``* init_replica`` for source pgsql adds the copied tables as fake "replicated tables" for better  show_status display.

For the source type ``pgsql`` the following restrictions apply.

* There is no support for real time replica
* The data copy happens always with file method
* The copy_max_memory doesn't apply
* The type override doesn't apply
* Only ``init_replica`` is currently supported
* The source connection string requires a database name


2.0.0.alpha3
--------------------------
**please note this is a not production release. do not use it in production**

The third and final alpha3 for the milestone 2.0 fixes some issues and add more features to the system.

As there are changes in the replica catalog if upgrading from the alpha1 there will be need to do a ``drop_replica_schema``
followed by a ``create_replica_schema``. This **will drop any existing replica** and will require re adding the sources and
re initialise them with ``init_replica``.

The system now supports a source type ``pgsql`` with the following limitations.

* There is no support for real time replica
* The data copy happens always with file method
* The copy_max_memory doesn't apply
* The type override doesn't apply
* Only ``init_replica`` is currently supported
* The source connection string requires a database name
* In the ``show_status`` detailed command the replicated tables counters are always zero

A stack trace capture is now added on the log and the rollbar message for better debugging.
A new parameter ``on_error_replay`` is available for the sources to set whether the replay process should skip the tables or exit on error.

This release adds the command ``upgrade_replica_schema`` for upgrading the replica schema from the version 1.8 to the 2.0.

The upgrade procedure is described in the documentation.

**Please read it carefully before any upgrade and backup the schema sch_chameleon before attempting any upgrade.**


2.0.0.alpha2
--------------------------
**please note this is a not production release. do not use it in production**

The second alpha of the milestone 2.0 comes after a week of full debugging. This release is more usable and stable than the
alpha1. As there are changes in the replica catalog if upgrading from the alpha1 there will be need to do a ``drop_replica_schema``
followed by a ``create_replica_schema``. This **will drop any existing replica** and will require re adding the sources and
re initialise them with ``init_replica``.

The full list of changes is in the CHANGELOG file. However there are few notable remarks.

There is a detailed display of the ``show_status`` command when a source is specified. In particular the number of replicated and
not replicated tables is displayed. Also if any table as been pulled out from the replica it appears on the bottom.

From this release there is an error log which saves the exception's data during the replay phase.
The error log can be queried with the new command ``show_errors``.

A new source parameter ``replay_max_rows`` has been added to set the amount of rows to replay.
Previously the value was set by the parameter ``replica_batch_size``. If upgrading from alpha1 you may need to add
this parameter to your existing configuration.

Finally there is a new class called ``pgsql_source``, not yet functional though.
This class will add a very basic support for the postgres source type.
More details will come in the alpha3.


2.0.0.alpha1
--------------------------
**please note this is a not production release. do not use it in production**

This is the first alpha of the milestone 2.0. The project has been restructured in many ways thanks to the user's feedback.
Hopefully this will make the system much simple to use.

The main changes in the version 2 are the following.

The system is Python 3 only compatible. Python 3 is the future and there is no reason why to keep developing thing in 2.7.

The system now can read from multiple MySQL schemas in the same database and replicate them it into a target PostgreSQL database.
The source and target schema names can be different.

The system now use a conservative approach to the replica. The tables which generate errors during the replay are automatically excluded from the replica.

The init_replica process runs in background unless the logging is on the standard output or the debug option is passed to the command line.

The replica process now runs in background with two separated subprocess, one for the read and one for the replay.
If the logging is on the standard output or the debug option is passed to the command line the main process stays in foreground though.

The system now use a soft approach when initialising the replica .
The tables are locked only when copied. Their log coordinates will be used by the replica damon to put the database in a consistent status gradually.

The system can now use the rollbark key and environment to setup the Rollbar integration, for a better error detection.
