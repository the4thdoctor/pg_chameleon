### pg_chameleon MySQL to PostgreSQL replica system

![Alt text](images/pgchameleon.png "Igor the chameleon")

The chameleon logo was made by [Elena Toma](http://tonkipappero.deviantart.com/ "Tonkipappero's Art")


pg_chameleon is a replication system for replicating MySQL schemas into a target PostgreSQL database.

The latest release is the 2.0.0

pg_chameleon is Python 3.3+ compatible. 

The system  can read from multiple MySQL schemas  and replicate them it into a target PostgreSQL database.

A conservative approach to the replica is implemented.  The tables which generate errors during the replay are automatically excluded from the replica.

The init_replica, sync_tables and refresh_schema processes are started in background.

The replica process is fully daemonised with two child subprocesses, one for the read and one for the replay. 

pg_chameleon uses a soft approach when initialising the replica .
The tables are locked only during the copy. Their log coordinates are used  by the replica daemon to put the database in a consistent status gradually.

The system supports the rollbar integration for a better error detection and event monitoring (e.g. when the init_replica ends a message is sent to rollbar).

Package page [https://pypi.python.org/pypi/pg_chameleon](https://pypi.python.org/pypi/pg_chameleon)

The documentation [http://www.pgchameleon.org/documents/](http://www.pgchameleon.org/documents/)

Changelog available [http://www.pgchameleon.org/documents/changelog.html](http://www.pgchameleon.org/documents/changelog.html)