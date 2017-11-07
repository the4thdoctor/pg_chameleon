The configuration file
********************************

The file config-example.yaml is stored in **~/.pg_chameleon/configuration** and should be used as template for the other configuration files. 
The configuration consists of three configuration groups.

Global settings
..............................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 1-10
   :linenos:

* pid_dir directory where the process pids are saved.
* log_dir directory where the logs are stored.
* log_dest log destination. stdout for debugging purposes, file for the normal activity.
* log_level logging verbosity. allowed values are debug, info, warning, error.
* log_days_keep configure the retention in days for the daily rotate replica logs.
* rollbar_key: the optional rollbar key
* rollbar_env: the optional rollbar environment

If both rollbar_key and rollbar_env are configured some messages are sent to the rollbar conf

PostgreSQL target connection
...............................................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 11-18
   :linenos:

The pg_conn key maps the target database connection string.  
   
   
sources configuration
...............................................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-49
   :linenos:

The key sources allow to setup multiple replica sources writing on the same postgresql database. The key name myst be unique wihin the replica configuration.

Database connection
=============================


.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-48
   :emphasize-lines: 3-9
   :linenos:

The db_conn key maps the target database connection string.  Within the connection is possible to configure the connect_timeout which is 10 seconds by default.
Larger values could help the tool working better on slow networks. Low values can cause the connection to fail before any action is performed.

Schema mappings
=============================

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-48
   :emphasize-lines: 10-11
   :linenos:

The key schema mappings is a dictionary. Each key is a MySQL database that needs to be replicated in PostgreSQL. Each value is the destination schema in the PostgreSQL database.
In the example provided the MySQL database my_database is replicated into the schema pg_database stored in the database specified in the pg_conn key (db_replica). 

Limit and skip tables
=============================

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-48
   :emphasize-lines: 12-15
   :linenos:

* limit_tables list with the tables to replicate. If the list is empty then the entire mysql database is replicated.
* skip_tables list with the tables to exclude from the replica. 

The table's names should be in the form SCHEMA_NAME.TABLE_NAME. 

Grant select to and create views in schema
=============================================================

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-46
   :emphasize-lines: 16-17
   :linenos:

**Not implemented yet.**

Source configuration parameters
====================================

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-46
   :emphasize-lines: 18-26
   :linenos:
   
* lock_timeout the max time in seconds that the target postgresql connections should wait for acquiring a lock. This parameter applies  to init_replica,refresh_schema and sync_tables.
* my_server_id the server id for the mysql replica. must be unique within the replica cluster
* replica_batch_size the max number of rows that are pulled from the mysql replica before a write on the postgresql database is performed. See caveats in README for a complete explanation.
* batch_retention the max retention for the replayed batches rows in t_replica_batch. The field accepts any valid interval accepted by PostgreSQL
* copy_max_memory the max amount of memory to use when copying the table in PostgreSQL. Is possible to specify the value in (k)ilobytes, (M)egabytes, (G)igabytes adding the suffix (e.g. 300M).
* copy_mode the allowed values are ‘file’ and ‘direct’. With direct the copy happens on the fly. With file the table is first dumped in a csv file then reloaded in PostgreSQL.
* out_dir the directory where the csv files are dumped during the init_replica process if the copy mode is file.
* sleep_loop seconds between a two replica batches.
* type specifies the source database type. Currently only mysql is accepted. 
   
type override
...............................................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 50-56
   :linenos:

The type_override allows the user to override the default type conversion into a different one. 
Each type key should be named exactly  like the mysql type to override including the dimensions.
Each type key needs two subkeys.

* override_to specifies the destination type which must be a postgresql type and the type cast should be possible
* override_tables is a yaml list which specifies to which tables the override applies. If the first list item is set to "*" then the override is applied to all tables in the replicated schemas.

The override is applied when running the init_replica,refresh_schema andsync_tables process.
The override is also applied for each matching DDL (create table/alter table) if the table name matches the override_tables values.
