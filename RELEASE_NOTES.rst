RELEASE NOTES
*************************
2.0-alpha2 
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


2.0-alpha1 
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

