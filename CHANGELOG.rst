changelog 
*************************
2.0-alpha1 11 November 2017
.............................

* Python 3 only development
* Add support for reading from multiple MySQL schemas and restore them it into a target PostgreSQL database. The source and target schema names can be different.
* Conservative approach to the replica. Tables which generate errors are automatically excluded from the replica.
* Daemonised init_replica process.
* Daemonised replica process with two separated subprocess, one for the read and one for the replay.
* Soft replica initialisation. The tables are locked when needed and stored with their log coordinates. The replica damon will put the database in a consistent status gradually.
* Rollbar integration for a simpler error detection.
