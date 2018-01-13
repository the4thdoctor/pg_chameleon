RELEASE NOTES
*************************
2.0.1
--------------------------
The first maintenance release of pg_chameleon v2 adds a senssible performance improvement in the read replica process when 
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

