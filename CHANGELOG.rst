changelog 
*************************

2.0.3 - XX February 2018
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
