Usage
**************************************************

Command line reference
............................................

.. code-block:: bash

    chameleon command [ [ --config ] [ --source ] [ --schema ]  [ --tables ] [--logid] [ --debug ] [ --rollbar-level ] ] [ --version ] [ --full ]

.. csv-table:: Options
   :header: "Option", "Description", "Default","Example"

   ``--config``, Specifies the configuration to use in ``~.pg_chameleon/configuration/``. The configuration name should be the file without the extension ``.yml`` , ``default``,``--config foo`` will use the file ``~.pg_chameleon/configuration/foo.yml``
   ``--source``, Specifies the source within a configuration file., N/A, ``--source bar``
   ``--schema``, Specifies a schema configured within a source., N/A, ``--schema schema_foo``
   ``--tables``, Specifies one or more tables configured in a schema. Multiple tables can be specified separated by comma. The table must have the schema., N/A, ``--tables schema_foo.table_bar``
   ``--logid``, Specifies the log id entry for displaying the error details, N/A, ``--logid 30``
   ``--debug``,When added to the command line the debug option disables any daemonisation and outputs all the logging to the console. The keybord interrupt signal is trapped correctly., N/A, ``--debug``
   ``--version``,Displays the package version., N/A, ``--version``
   ``--rollbar-level``, Sets the maximum level for the messages to be sent  to rolllbar. Accepted values: "critical" "error" "warning" "info", ``info`` ,``--rollbar-level error``
   ``--full``,Runs a VACUUM FULL  on the log tables when the run_maintenance is executed, N/A,``--full``


.. csv-table:: Command list reference
   :header: "Command", "Description", "Options"

    ``set_configuration_files``, Setup the example configuration files and directories in ``~/.pg_chameleon``
    ``show_config``, Displays the configuration  for the configuration, ``--config``
    ``show_sources``, Displays the sourcches configured for the configuration, ``--config``
    ``show_status``,Displays an overview of the status of the sources configured within the configuration. Specifying the source gives more details about that source , ``--config`` ``--source``
    ``show_errors``,Displays  the errors logged by the replay  function. If a log id is specified then the log entry is displayed entirely, ``--config`` ``--logid``
    ``create_replica_schema``, Creates a new replication schema into the config's destination database, ``--config``
    ``drop_replica_schema``, Drops an existing replication schema from the config's destination database, ``--config``
    ``upgrade_replica_schema``,Upgrades the replica schema from a an older version,``--config``
    ``add_source``, Adds a new source to the replica catalogue, ``--config`` ``--source``
    ``drop_source``, Remove an existing source from the replica catalogue, ``--config`` ``--source``
    ``init_replica``, Initialise the replica for an existing source , ``--config`` ``--source``
    ``copy_schema``, Copy only the schema from mysql to PostgreSQL., ``--config`` ``--source``
    ``update_schema_mappings``,Update the schema mappings stored in the replica catalogue using the data from the configuration file. , ``--config`` ``--source``
    ``refresh_schema``, Synchronise all the tables for a given schema within an already initialised source. , ``--config`` ``--source`` ``--schema``
    ``sync_tables``, Synchronise one or more tables within an already initialised source.  The switch ``--tables`` accepts the special name ``disabled`` to resync all the tables with replica disabled., ``--config`` ``--source`` ``--tables``
    ``start_replica``, Starts the replica process daemon, ``--config`` ``--source``
    ``stop_replica``, Stops the replica process daemon, ``--config`` ``--source``
    ``detach_replica``, Detaches a replica from the mysql master configuring the postgres schemas to work as a standalone system. Useful for migrations., ``--config`` ``--source``
    ``enable_replica``, Enables the replica for the given source changing the source status to stopped. It's useful if the replica crashes., ``--config`` ``--source``
    ``run_maintenance``, Runs a VACUUM on the log tables for the given source. If  is specified then the maintenance runs a VACUUM FULL, ``--config`` ``--source`` ``--full``
    ``stop_all_replicas``, Stops all the running sources within the target postgresql database., ``--config``


Example
............................................

Create a virtualenv and activate it

.. code-block:: none

    python3 -m venv venv
    source venv/bin/activate


Install pg_chameleon

.. code-block:: none

    pip install pip --upgrade
    pip install pg_chameleon

Run the ``set_configuration_files`` command in order to create the configuration directory.

.. code-block:: none

    chameleon set_configuration_files


cd in ``~/.pg_chameleon/configuration`` and copy the file ``config-example.yml` to ``default.yml``.



In MySQL create a user for the replica.

.. code-block:: sql

    CREATE USER usr_replica ;
    SET PASSWORD FOR usr_replica=PASSWORD('replica');
    GRANT ALL ON sakila.* TO 'usr_replica';
    GRANT RELOAD ON *.* to 'usr_replica';
    GRANT REPLICATION CLIENT ON *.* to 'usr_replica';
    GRANT REPLICATION SLAVE ON *.* to 'usr_replica';
    FLUSH PRIVILEGES;

Add the configuration for the replica to my.cnf. It requires a MySQL restart.



.. code-block:: ini

    binlog_format= ROW
    binlog_row_image=FULL
    log-bin = mysql-bin
    server-id = 1
    expire_logs_days = 10
    # MARIADB 10.5.0+ OR MYSQL 8.0.14+ versions
    binlog_row_metadata = FULL




In PostgreSQL create a user for the replica and a database owned by the user

.. code-block:: sql

    CREATE USER usr_replica WITH PASSWORD 'replica';
    CREATE DATABASE db_replica WITH OWNER usr_replica;

Check you can connect to both databases from the machine where pg_chameleon is installed.

For MySQL

.. code-block:: none

    mysql -p -h derpy -u usr_replica sakila
    Enter password:
    Reading table information for completion of table and column names
    You can turn off this feature to get a quicker startup with -A

    Welcome to the MySQL monitor.  Commands end with ; or \g.
    Your MySQL connection id is 116
    Server version: 5.6.30-log Source distribution

    Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

    Oracle is a registered trademark of Oracle Corporation and/or its
    affiliates. Other names may be trademarks of their respective
    owners.

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

    mysql>

For PostgreSQL

.. code-block:: none

    psql  -h derpy -U usr_replica db_replica
    Password for user usr_replica:
    psql (9.5.5)
    Type "help" for help.
    db_replica=>

Check the docs for the configuration file reference. It will help  you to configure correctly the connections.

Initialise the replica


.. code-block:: none

    chameleon create_replica_schema --debug
    chameleon add_source --config default  --debug
    chameleon init_replica --config default --debug


Start the replica with


.. code-block:: none

  chameleon start_replica --config default --source example

Check the source status

.. code-block:: none

  chameleon show_status --source example

Check the error log

.. code-block:: none

  chameleon show_errors

.. code-block:: none

  chameleon start_replica --config default --source example


To stop the replica

.. code-block:: none

  chameleon stop_replica --config default --source example


To detach the replica

.. code-block:: none

  chameleon detach_replica --config default --source example




