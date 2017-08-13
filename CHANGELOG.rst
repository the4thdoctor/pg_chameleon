changelog 
*************************

1.6 13 Aug 2017
.................................
* fix wrong table name when parsing **CREATE TABLE schema_name.table_name**
* fix missing parse for numeric_scale in sql_utils
* lock only the affected tables when running sync_tables
* improve performance for the replay plpgsql function
* rename *change lag* to *read lag* and add *replay  lag*  in the the *show_status* output
* add `TravisCI configuration <https://travis-ci.org/the4thdoctor/pg_chameleon>`_ to the source tree
* add set_config for initial config dir creation (needed by the CI tests)
* add a regexp match to exclude the keywords in the parse alter  table 
* add **FOREIGN KEY** to the excluded keyworkds when parsing alter table
* fix `Issue #22 <https://github.com/the4thdoctor/pg_chameleon/issues/22>`_ add **KEY** to the excluded keyworkds when parsing alter table


1.5 23 Jul 2017
.................................
* fix wrong evaluation in table's consistent state (row and DDL)
* fix wrong dimensions when building floating point data types 
* add support for DEFAULT value on ADD COLUMN,CHANGE,MODIFY
* add indices to the t_log _replica tables in order to speedup the batch cleanup 
* Fix for `Issue #5 <https://github.com/the4thdoctor/pg_chameleon/issues/5>`_   add cast to char with character set in column generation in order to override collation mix. the charset is the mysql general character set
* Improve logging messages to be more descriptive
* Remove total_events and evaluate when writing the batch using the real number of events stored from the mysql replica
* fix ALTER TABLE...CHANGE parsing to lower the data type
* add warning when a table is without primary key
* Fix  `Issue #9 <https://github.com/the4thdoctor/pg_chameleon/issues/9>`_   add configuration check for MySQL before starting the replica
* Fix  `Issue #10 <https://github.com/the4thdoctor/pg_chameleon/issues/10>`_   clarify running replica log messages to describe better what's happening
* Add --nolock option as per request on `Issue #13 <https://github.com/the4thdoctor/pg_chameleon/issues/13>`_ When started with --nolock the replica doesn't create the lock file in case of error.

1.4 - 08 Jul 2017
...........................................
* add varbinary to the type conversion dictionary
* fix wrong quoting when key field is surrounded by backtick `
* add geometry to the supported types
* add varbinary and geometry to hexify in config-example.yaml
* add INDEX and UNIQUE to excluded words when parsing alter table. this prevents the ddl replica to crash when the alter table adds an index
* Fix for `Issue #4 <https://github.com/the4thdoctor/pg_chameleon/issues/4>`_  add generic exception when fallback on inserts to trap unexpected data not handled by psycopg2  
* Replace sync_replica with sync_tables. Check the release notes for implementation.
* Add --version to display the program version.
* Move documentation on `pgchameleon.org <http://www.pgchameleon.org/documents/index.html>`_ 

1.3.1 - 19 Jun 2017
...........................................
* fix regression in save_master_status when the time is missing in the master's coordinates
* fix missing timestamp for event when capturing a DDL

1.3 - 17 Jun 2017
...........................................
* each source now uses two dedicated log tables for better performance
* set descriptive application_name in postgres process to track the replica activity
* fix race condition when two sources have a batch with the same value in t_replica_batch.ts_created
* add switch --debug for having the log on console with DEBUG verbosity without need to change configuration file
* fix regexp for foreign keys when omitting CONSTRAINT keyword
* change lag display in show_status from seconds to interval for better display
* add quote_col method in sql_token class to ensure all index columns are correctly quoted
* add a release_notes file for the details on the releases 1.3+
* fix wrong timestamp save when updating the last received event in sources
* temporarly disable sync_replica because is not working as expected

1.2 - 25 May 2017
...........................................
* fix deadlock when replicating from multiple sources:
* add source id when cleaning batches
* add missing marker when outputting failed mysql query in copy_tabl_data
* fix wrong decimal handling in build_tab_ddl
* add bool to the data dictionary
* exclude ddl when coming from schemas different from the one replicated
* fix wrong quoting when capturing primary key inline
* add error handling in read_replica
* move the version view management in the pg_engine code

1.1 - 13 May 2017
...........................................
* fix race condition when capturing  queries not tokenised that leave the binglog position unchanged
* completed docstrings in sql_util.py

1.0 - 07 May 2017
............................................
* Completed docstrings in pg_lib.py 

1.0 RC2  -  26 Apr 2017
............................................
* Completed docstrings in global_lib.py and mysql_lib.py
* Partial writing for docstrings in pg_lib.py
* Restore fallback on inserts when the copy batch data fails
* Reorganise documentation in docs

1.0 RC1  -  15 Apr 2017
............................................
* add support for primay key defined as column constraint
* fix regression if null constraint is omitted in a primary key column
* add foreign key generation to detach replica. keys are added invalid and a validation attempt is performed.
* add support for truncate table 
* add parameter out_dir to set the output destination for the csv files during init_replica
* add set tables_only  to table_limit when streaming the mysql replica
* force a close batch on rotate event if binlog changes without any row events collected
* fixed replica regression with python 3.x and empty binary data
* added event_update in hexlify strategy
* add tabulate for nice display for sources/status
* logs are rotated on a daily basis
* removed parameter log_append 
* add parameter log_days_keep to specify how many days keep the logs
* feature freeze


1.0 Beta 2  -  02 Apr 2017
............................................
* add detach replica with automatic sequence reset (no FK generation yet)
* replica speed improvement with the exclusion  of BEGIN,COMMIT when capturing the queries from MySQL
* fix the capturing regexp  for primary keys and foreign keys
* fix version in documentation 


1.0 Beta 1  -  18 Mar 2017
............................................
* changed not python files in package  to work properly with system wide installations
* fixed issue with ALTER TABLE ADD CONSTRAINT
* add datetime.timedelta to json encoding exceptions
* added support for enum in ALTER TABLE MODIFY
* requires psycopg2 2.7 which installs without postgresql headers



1.0 Alpha 4  -  28 Feb 2017
............................................

* Add batch retention to avoid bloating of t_replica_batch
* Packaged for pip, now you can install the replica tool in a virtual env just typing pip install pg_chameleon


1.0 Alpha 3  -  7 Feb 2017
............................................


* Basic DDL Support (CREATE/DROP/ALTER TABLE, DROP PRIMARY KEY)
* Replica from multiple MySQL schema or servers
* Python 3 support


1.0 Alpha 2  -  31 Dec 2016 
............................................

Changelog from alpha 1

* Several fixes in the DDL replica and add support for CHANGE statement.
* Add support for check if process is running already, in order to avoid two replica processes run at the same time.
* Port to python 3.6. This is still experimental. Any feedback is more than welcome.




1.0 Alpha 1  -  27 Nov 2016
............................................

Installation in virtualenv

For working properly you should use virtualenv for installing the requirements via pip
No daemon yet

The script should be executed in a screen session to keep it running. Currently there's no respawning of the process on failure nor failure detector.
psycopg2 requires python and postgresql dev files

The psycopg2's pip installation requires the python development files and postgresql source code.
Please refer to your distribution for fulfilling those requirements.
DDL replica limitations

DDL and DML mixed in the same transaction are not decoded in the right order. This can result in a replica breakage caused by a wrong jsonb descriptor if the DML change the data on the same table modified by the DDL. I know the issue and I'm working on a solution.
Test please!

Please submit the issues you find.
Bear in mind this is an alpha release. if you use the software in production keep an eye on the process to ensure the data is correctly replicated.
