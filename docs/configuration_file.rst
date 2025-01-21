The configuration file
********************************

The file config-example.yaml is stored in **~/.pg_chameleon/configuration** and should be used as template for the other configuration files.
The configuration consists of three configuration groups.

Global settings
..............................

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 2-9
   :linenos:

* pid_dir directory where the process pids are saved.
* log_dir directory where the logs are stored.
* log_dest log destination. stdout for debugging purposes, file for the normal activity.
* log_level logging verbosity. allowed values are debug, info, warning, error.
* log_days_keep configure the retention in days for the daily rotate replica logs.
* rollbar_key: the optional rollbar key
* rollbar_env: the optional rollbar environment

If both rollbar_key and rollbar_env are configured some messages are sent to the rollbar conf

type override
...............................................

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 11-16
   :linenos:

The type_override allows the user to override the default type conversion into a different one.
Each type key should be named exactly  like the mysql type to override including the dimensions.
Each type key needs two subkeys.

* override_to specifies the destination type which must be a postgresql type and the type cast should be possible
* override_tables is a yaml list which specifies to which tables the override applies. If the first list item is set to "*" then the override is applied to all tables in the replicated schemas.

The override is applied when running the init_replica,refresh_schema andsync_tables process.
The override is also applied for each matching DDL (create table/alter table) if the table name matches the override_tables values.


PostgreSQL target connection
...............................................

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 21-28
   :linenos:

The pg_conn key maps the target database connection string.


sources configuration
...............................................

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-95
   :linenos:

The key sources allow to setup multiple replica sources writing on the same postgresql database.
The key name myst be unique within the replica configuration.


The following remarks apply only to the mysql source type.

For the postgresql source type. See the last section for the description and the limitations.

Database connection
=============================


.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 3-9
   :linenos:

The db_conn key maps the target database connection string.  Within the connection is possible to configure the connect_timeout which is 10 seconds by default.
Larger values could help the tool working better on slow networks. Low values can cause the connection to fail before any action is performed.

Schema mappings
=============================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 10-11
   :linenos:

The key schema mappings is a dictionary. Each key is a MySQL database that needs to be replicated in PostgreSQL. Each value is the destination schema in the PostgreSQL database.
In the example provided the MySQL database ``delphis_mediterranea`` is replicated into the schema ``loxodonta_africana`` stored in the database specified in the pg_conn key (db_replica).

Limit and skip tables
=============================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 12-15
   :linenos:

* limit_tables list with the tables to replicate. If the list is empty then the entire mysql database is replicated.
* skip_tables list with the tables to exclude from the replica.

The table's names should be in the form SCHEMA_NAME.TABLE_NAME.

Grant select to option
=============================================================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 16-17
   :linenos:

This key allows to specify a list of database roles which will get select access on the replicate tables.






Source configuration parameters
====================================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 18-31
   :linenos:

* lock_timeout the max time in seconds that the target postgresql connections should wait for acquiring a lock. This parameter applies  to init_replica,refresh_schema and sync_tables when performing the relation's swap.
* my_server_id the server id for the mysql replica. must be unique within the replica cluster
* replica_batch_size the max number of rows that are pulled from the mysql replica before a write on the postgresql database is performed. See caveats in README for a complete explanation.
* batch_retention the max retention for the replayed batches rows in t_replica_batch. The field accepts any valid interval accepted by PostgreSQL
* copy_max_memory the max amount of memory to use when copying the table in PostgreSQL. Is possible to specify the value in (k)ilobytes, (M)egabytes, (G)igabytes adding the suffix (e.g. 300M).
* copy_mode the allowed values are ‘file’ and ‘direct’. With direct the copy happens on the fly. With file the table is first dumped in a csv file then reloaded in PostgreSQL.
* out_dir the directory where the csv files are dumped during the init_replica process if the copy mode is file.
* sleep_loop seconds between a two replica batches.
* on_error_replay specifies whether the replay process should ``exit`` or ``continue``  if any error during the replay happens. If ``continue`` is specified the offending tables are removed from the replica.
* on_error_read specifies whether the read process should ``exit`` or ``continue``  if a connection error during the read process happens. If ``continue`` is specified the process emits a warning and waits for the connection to come back. If the parameter is omitted the default is ``exit`` which cause the replica process to stop with error.
* auto_maintenance specifies the timeout after an automatic maintenance is triggered. The parameter accepts values valid for the `PostgreSQL interval data type <https://www.postgresql.org/docs/current/static/datatype-datetime.html#DATATYPE-INTERVAL-INPUT>`_ (e.g. ``1 day``). If the value is set to ``disabled`` the automatic maintenance doesn't run. If the parameter is omitted the default is ``disabled``.
* gtid_enable **(EXPERIMENTAL)** Specifies whether to use the gtid to auto position the replica stream. This parameter have effect only on MySQL and only if the server is configured with the GTID.
* type specifies the source database type. The system supports ``mysql`` or  ``pgsql``. See below for the pgsql limitations.


Skip events configuration
====================================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 32-37
   :linenos:


The ``skip_events`` variable allows to tell pg_chameleon to skip events for tables or entire schemas.
The example provided with configuration-example.ym disables the inserts on the table ``delphis_mediterranea.foo`` and disables the deletes on the entire schema ``delphis_mediterranea``.

Keep existing schema
====================================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 38-38
   :linenos:


When set to ``Yes`` init_replica,refresh_schema and
sync_tables do not recreate the affected tables using the data from the MySQL source.

Instead the existing tables are truncated and the data is reloaded.
A REINDEX TABLE is executed in order to have the indices in good shape after the reload.

When ``keep_existing_schema`` is set to Yes the parameter ``grant_select_to`` have no effect.

net_read_timeout
====================================

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 30-68
   :emphasize-lines: 39-39
   :linenos:

Configures for the session the net_read_timeout.
Useful if the table copy during init replica fails on slow networks.

It defaults to 600 seconds.

PostgreSQL source type (EXPERIMENTAL)
================================================================

pg_chameleon 2.0 has an experimental support for the postgresql source type.
When set to ``pgsql`` the system expects a postgresql source database rather a mysql.
The following limitations apply.

* There is no support for real time replica
* The data copy happens always with file method
* The copy_max_memory doesn't apply
* The type override doesn't apply
* Only ``init_replica`` is currently supported
* The source connection string requires a database name
* In the ``show_status`` detailed command the replicated tables counters are always zero


.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 69-96
   :emphasize-lines: 7,16,25,27
   :linenos:

Fillfactor
================================================================
The dictionary fillfactor is used to set the fillfactor for tables that are expected to work with large updates.
The key name defines the fillfactor level (The allowed values range is 10 to 100).
If key name is set to "*" then the fillfactor applies to all tables in the replicated schema.
If the table appears multiple times, then only the last matched value will be applied

.. literalinclude:: ../pg_chameleon/configuration/config-example.yml
   :language: yaml
   :lines: 101-108
   :linenos: