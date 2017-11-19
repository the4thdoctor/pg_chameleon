changelog 
*************************

2.0.0.alpha3 XX December 2017
.............................
* Remove limit_tables from binlogreader initialisation, as we can read from multiple schemas we should only exclude the tables not limit
* Fix wrong formatting for default value when altering a field

2.0.0.alpha2 18 November 2017
.............................
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

2.0.0.alpha1 11 November 2017
.............................

* Python 3 only development
* Add support for reading from multiple MySQL schemas and restore them it into a target PostgreSQL database. The source and target schema names can be different.
* Conservative approach to the replica. Tables which generate errors are automatically excluded from the replica.
* Daemonised init_replica process.
* Daemonised replica process with two separated subprocess, one for the read and one for the replay.
* Soft replica initialisation. The tables are locked when needed and stored with their log coordinates. The replica damon will put the database in a consistent status gradually.
* Rollbar integration for a simpler error detection.
