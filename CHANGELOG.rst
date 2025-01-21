changelog
*************************

2.0.21 - 21 January 2025
..........................................................
* PR #163 provided by @bukem providing an optimization of the procedure for applying changes to Postgresql
* Issue #170 add check for existing replica schema and display an hint instead of an exception
* Fix incorrect placement of the new parameter net_read_timeout. Now it's set as an instance variable from global_lib.py

2.0.20 - 01 January 2025
..........................................................
* Merge the SQL library improvements built by @nikochiko  for the `Google Summer of Code 2023 <https://summerofcode.withgoogle.com/archive/2023/projects/VnzdAl4z>`_
* Fix setup.py for newer python versions as per patch provided by @darix in Issue #172
* Merge PR #169 provided by @Jamal-B Fix read and replay daemons death detection
* Merge PR #171 provided by @JasonLiu1567 fix issue #111
* Merge PR #173 provided by @acarapetis Ignore MySQL indices with prefix key parts
* DEPRECATION of rollbar support

2.0.19 - 25 March 2023
..........................................................
* Merge pull request #144, mysql-replication support for PyMySQL>0.10.0 was introduced in v0.22
* add support for fillfactor when running init_replica
* improve logging on discarded rows
* add distinct on group concat when collecting foreign keys
* use mysql-replication>=0.31, fix for crash when replicating from MariaDB

2.0.18 - 31 March 2022
..........................................................
* Support the ON DELETE and ON UPDATE clause when creating the foreign keys in PostgreSQL
* change logic for index and foreign key names by managing only duplicates within same schema
* use mysql-replication<0.27 as new versions crash when receiving queries
* add copy_schema method for copying only the schema without data (EXPERIMENTAL)
* change type for identifiers in replica schema to varchar(64)

2.0.17 - 30 January 2022
..........................................................
* Remove argparse from the requirements
* Add the collect for unique constraints when keep_existing_schema is **Yes**
* Fix wrong order in copy data/create indices when keep_existing_schema is **No**
* Remove check for log_bin we are replicating from Aurora MySQL
* Manage different the different behaviour in pyyaml to allow pg_chameleon to be installed as rpm in centos 7

2.0.16 - 23 September 2020
..........................................................
* Fix for issue #126 init_replica failure with tables on transactional engine and invalid data

2.0.15 - 20 September 2020
..........................................................
* Support for reduced lock if MySQL engine is transactional, thanks to @rascalDan
* setup.py now requires python-mysql-replication to version 0.22 which adds support for PyMySQL >=0.10.0
* removed PyMySQL requirement <0.10.0 from setup.py
* prevent pg_chameleon to run as root

2.0.14 - 26 July 2020
..........................................................
* Add support for spatial data types (requires postgis installed on the target database)
* When ``keep_existing_schema`` is set to ``yes`` now drops and recreates indices, and constraints during the ``init_replica`` process
* Fix for issue #115 thanks to @porshkevich
* setup.py now forces PyMySQL to version <0.10.0 because it breaks the python-mysql-replication library (issue #117)

2.0.13 - 05 July 2020
..........................................................
* **EXPERIMENTAL** support for Point datatype - @jovankricka-everon
* Add ``keep_existing_schema`` in MySQL source type to keep the existing scema in place instead of rebuilding it from the mysql source
* Change tabs to spaces in code

2.0.12 - 11 Dec 2019
..........................................................
* Fixes for issue #96 thanks to @daniel-qcode
* Change for configuration and SQL files location
* Package can build now as source and wheel
* The minimum python requirements now is 3.5

2.0.11 - 25 Oct 2019
..........................................................
* Fix wrong formatting for yaml example files. @rebtoor
* Make start_replica run in foreground when log_file == stdout . @clifff
* Travis seems to break down constantly, Disable the CI until a fix is found. Evaluate to use a different CI.
* Add the add loader to yaml.load as required by the new PyYAML version.

2.0.10 - 01 Sep 2018
..........................................................
* Fix regression in new replay function with PostgreSQL 10
* Convert to string the dictionary entries pulled from a json field
* Let ``enable_replica`` to disable any leftover maintenance flag
* Add capture in CHANGE for tables in the form schema.table

2.0.9 - 19 Aug 2018
..........................................................
* Fix wrong check for the next auto maintenance run if the maintenance wasn't run before
* Improve the replay function's speed
* Remove blocking from the GTID operational mode


2.0.8 - 14 Jul 2018
..........................................................
* Add support for skip events as requested in issue #76. Is now possible to skip events (insert,delete,update) for single tables or for entire schemas.
* **EXPERIMENTAL** support for the GTID. When configured on MySQL or Percona server pg_chameleon will use the GTID to auto position the replica stream. Mariadb is not supported by this change.
* ALTER TABLE RENAME is now correctly parsed and executed
* Add horrible hack to ALTER TABLE MODIFY.  Previously modify with default values would parse wrongly and fail when translating to PostgreSQL dialect
* Disable erroring the source when running with ``--debug`` switch enabled
* Add cleanup for logged events when refreshing schema and syncing tables. previously spurious logged events could lead to primary key violations when syncing single tables or refreshing single schemas.


2.0.7 - 19 May 2018
..........................................................
* Fix for issue #71, make the multiprocess logging safe. Now each replica process logs in a separate file
* Fix the ``--full`` option to store true instead of false. Previously the option had no effect.
* Add `auto_maintenance` optional parameter to trigger a vacuum over the log tables after a specific timeout
* Fix for issue #75, avoid the wrong conversion to string for None keys when cleaning up malformed rows during the init replica and replica process
* Fix for issue #73, fix for wrong data type tokenisation when an alter table adds a column with options (e.g. ``ADD COLUMN foo DEFAULT NULL``)
* Fix wrong TRUNCATE TABLE tokenisation if the statement specifies the table with the schema.

2.0.6 - 29 April 2018
..........................................................
* fix for issue #69 add source's optional parameter ``on_error_read:`` to allow the read process to continue in case of connection issues with the source database (e.g. MySQL in maintenance)
* remove the detach partition during the maintenance process as this proved to be a very fragile approach
* add switch ``--full`` to run a ``VACUUM FULL`` during the maintenance
* when running the maintentenance execute a ``VACUUM`` instead of a ``VACUUM FULL``
* fix for issue #68. fallback to ``binlog_row_image=FULL`` if the parameter is missing in mysql 5.5.
* add cleanup for default value ``NOW()`` when adding a new column with ``ALTER TABLE``
* allow ``enable_replica`` to reset the source status in the case of a catalogue version mismatch

2.0.5 - 25 March 2018
..........................................................
* fix wrong exclusion when running sync_tables with limit_tables set
* add `run_maintenance` command to perform a VACUUM FULL on the source's log tables
* add `stop_all_replicas` command to stop all the running sources within the target postgresql database

2.0.4 - 04 March 2018
..........................................................
* Fix regression added in 2.0.3 when handling MODIFY DDL
* Improved handling of dropped columns during the replica


2.0.3 - 11 February 2018
..........................................................

*  fix regression added by commit 8c09ccb. when ALTER TABLE ADD COLUMN is in the form datatype DEFAULT (NOT) NULL the parser captures two words instead of one
*  Improve the speed of the cleanup on startup deleting only for the source's log tables  instead of the parent table
*  fix for issue #63. change the field i_binlog_position to bigint in order to avoid an integer overflow error when the binlog is largher than 2 GB.
*  change to psycopg2-binary in install_requires. This change will ensure the psycopg2 will install using the wheel package when available.
*  add upgrade_catalogue_v20 for minor schema upgrades

2.0.2 - 21 January 2018
..........................................................
* Fix for issue #61, missing post replay cleanup for processed batches.
* add private method ``_swap_enums`` to the class ``pg_engine`` which moves the enumerated types from the loading to the destination schema.

2.0.1 - 14 January 2018
..........................................................
* Fix for issue #58. Improve the read replica performance by filtering the row images when ``limit_tables/skip_tables`` are set.
* Make the ``read_replica_stream`` method private.
* Fix read replica crash if in alter table a column was defined as ``character varying``

2.0.0 - 01 January 2018
..........................................................
* Add option ``--rollbar-level`` to set the maximum level for the messages to be sent to rollbar. Accepted values: "critical", "error", "warning", "info". The Default is "info".
* Add command ``enable_replica`` used to reset the replica status in case of error or  unespected crash
* Add script alias ``chameleon`` along with ``chameleon.py``

2.0.0.rc1 - 24 December 2017
..........................................................
* Fix  for issue #52, When adding a unique key the table's creation fails because of the NULLable field
* Add check for the MySQL configuration when initialising or refreshing replicated entities
* Add class rollbar_notifier for simpler message management
* Add end of init_replica,refresh_schema,sync_tables notification to rollbar
* Allow ``--tables disabled`` when syncing the tables to re synchronise all the tables excluded from the replica

2.0.0.beta1 - 10 December 2017
..........................................................
* fix a race condition where an unrelated DDL can cause the collected binlog rows to be added several times to the log_table
* fix regression in write ddl caused by the change of private method
* fix wrong ddl parsing when a column definition is surrounded by parentheses e.g. ``ALTER TABLE foo ADD COLUMN(bar varchar(30));``
* error handling for wrong table names, wrong schema names, wrong source name and wrong commands
* init_replica for source pgsql now can read from an hot standby but the copy is not consistent
* init_replica for source pgsql adds "replicated tables" for better  show_status display
* check if the source is registered when running commands that require a source name

2.0.0.alpha3 - 03 December 2017
..........................................................
* Remove limit_tables from binlogreader initialisation, as we can read from multiple schemas we should only exclude the tables not limit
* Fix wrong formatting for default value when altering a field
* Add upgrade procedure from version 1.8.2 to 2.0
* Improve error logging and table exclusion in replay function
* Add stack trace capture to the rollbar and log message when one of the replica daemon crash
* Add ``on_error_replay`` to set whether the replay process should skip the tables or exit on error
* Add init_replica support for source type pgsql (EXPERIMENTAL)


2.0.0.alpha2 - 18 November 2017
..........................................................
* Fix wrong position when determining the destination schema in read_replica_stream
* Fix wrong log position stored in the source's high watermark
* Fix wrong table inclusion/exclusion in read_replica_steam
* Add source parameter ``replay_max_rows`` to set the amount of rows to replay. Previously the value was set by ``replica_batch_size``
* Fix crash when an alter table affected a table not replicated
* Fixed issue with alter table during the drop/set default for the column (thanks to psycopg2's sql.Identifier)
* add type display to source status
* Add fix for issue #33 cleanup NUL markers from the rows before trying to insert them in PostgreSQL
* Fix broken save_discarded_row
* Add more detail to show_status when specifying the source with --source
* Changed some methods to private
* ensure the match for the alter table's commands are enclosed by  word boundaries
* add if exists when trying to drop the table in  swap tables. previously adding a new table failed because the table wasn't there
* fix wrong drop enum type when adding a new field
* add log error for storing the errors generated during the replay
* add not functional class pgsql_source for source type pgsql
* allow ``type_override`` to be empty
* add show_status command for displaying the log error entries
* add separate logs for per source
* change log line formatting inspired by the super clean look in pgbackrest (thanks you guys)

2.0.0.alpha1 - 11 November 2017
..........................................................

* Python 3 only development
* Add support for reading from multiple MySQL schemas and restore them it into a target PostgreSQL database. The source and target schema names can be different.
* Conservative approach to the replica. Tables which generate errors are automatically excluded from the replica.
* Daemonised init_replica process.
* Daemonised replica process with two separated subprocess, one for the read and one for the replay.
* Soft replica initialisation. The tables are locked when needed and stored with their log coordinates. The replica damon will put the database in a consistent status gradually.
* Rollbar integration for a simpler error detection.
