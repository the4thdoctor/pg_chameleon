
.. image:: images/pgchameleon.png

pg_chameleon is a replication tool from MySQL to PostgreSQL developed in Python 2.7 and Python 3.3+
The system use the library mysql-replication to pull the row images from MySQL which are transformed into a jsonb object. 
A pl/pgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an  initial replica setup which pulls the data from MySQL in read only mode. 
This is done by the tool running FLUSH TABLE WITH READ LOCK;  .

pg_chameleon can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.


Current version: **v1.0-beta.1**



.. image:: https://readthedocs.org/projects/pg-chameleon/badge/?version=latest
    :target: http://pg-chameleon.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

`Documentation available at readthedocs <http://pg-chameleon.readthedocs.io/>`_


Platform and versions
****************************

The tool is developed using Linux Slackware 14.2 with python 2.7 and python 3.6.

The databases source and target are tested on FreeBSD 11.0

* MySQL: 5.6
* PostgreSQL: 9.5
  
What does it work
..............................
* Read the schema and data from MySQL and restore it into a PostgreSQL schema
* Saves in PostgreSQL the MySQL's master coordinates
* Create primary keys and indices on PostgreSQL
* Replica from MySQL to postgreSQL

 
What does seems to work
..............................
* Basic DDL Support (CREATE/DROP/ALTER TABLE, DROP PRIMARY KEY)
* Discards of rubbish data which is saved in the table sch_chameleon.t_discarded_rows
* Replica from multiple MySQL schema or servers
* Basic replica monitoring 

What doesn't work
..............................
* Full DDL replica 


Caveats
..............................
The copy_max_memory is just an estimate. The average rows size is extracted from mysql's informations schema and can be outdated.
If the copy process fails for memory problems check the failing table's row length. This could be a probable cause of memory overload.

The batch is processed every time the replica stream is empty, when a DDL is captured or when the MySQL switches to another log segment (ROTATE EVENT). 
Therefore the replica_batch_size  is just indicative.

Currently the process is sequential. 

Read the replica -> Store the rows -> Replay the stored rows. 

The version 2.0 will improve this aspect.

Python 3 is supported but only from version 3.3 as required by mysql-replication .

The lag is determined using the last received event timestamp and the postgresql timestamp. If the mysql is read only the lag will increase because
no replica event is coming in. I'll try to improve this aspect in the future.


Test please!
..............................

This software needs user's feeback. 
The system has proven to be quite efficient and stable. 
However the tool is not fully tested yet. I just ask you **to be very carefull if you want to use it in production**.

The best way to report a bug is to submit the issue GitHub.
However if you like to get in touch you can ping me on **twitter @4thdoctor_scarf** or if you prefer to chat, on irc there is a dedicated channel **irc.freenode.net #pgchameleon**.



Requirements
******************

MySQL: 5.5+

PostgreSQL: 9.5+

* `PyMySQL <https://github.com/PyMySQL/PyMySQL>`_ 
* `argparse <https://github.com/bewest/argparse>`_
* `mysql-replication <https://github.com/noplay/python-mysql-replication>`_
* `psycopg2 <https://github.com/psycopg/psycopg2>`_
* `PyYAML <https://github.com/yaml/pyyaml>`_
* `sphinx <http://www.sphinx-doc.org/en/stable/>`_
* `sphinx-autobuild <https://github.com/GaretJax/sphinx-autobuild>`_


Quick Setup 
*****************

* Create a virtual environment (e.g. python3 -m venv venv)
* Activate the virtual environment (e.g. source venv/bin/activate)
* Install pgchameleon with **pip install pg_chameleon**
* Create a user on mysql for the replica (e.g. usr_replica)
* Grant access to usr on the replicated database (e.g. GRANT ALL ON sakila.* TO 'usr_replica';)
* Grant RELOAD privilege to the user (e.g. GRANT RELOAD ON \*.\* to 'usr_replica';)
* Grant REPLICATION CLIENT privilege to the user (e.g. GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)
* Grant REPLICATION SLAVE privilege to the user (e.g. GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)



Configuration parameters
********************************
The system wide install is now supported correctly. 

The first time chameleon.py is executed it creates a configuration directory in $HOME/.pg_chameleon.
Inside the directory there are two subdirectories. 


* config is where the configuration files live. Use config-example.yaml as template for the other configuration files. Please note the logs and pid directories with relative path will no longer work. The you should either use an absolute path or provide the home alias. Again, check the config-example.yaml for an example.

* pid is where the replica pid file is created. it can be changed in the configuration file

* logs is where the replica logs are saved if log_dest is file. It can be changed in the configuration file

The file config-example.yaml is stored in ~/.pg_chameleon/config and should be used as template for the other configuration files. 


**do not use config-example.yaml** directly. The tool skips this filename as the file gets overwritten when pg_chameleon is upgraded.

Is it possible to have multiple configuration files for configuring the replica from multiple source databases. It's compulsory to chose different destination schemas on postgresql.

Each source requires to be started in a separate process (e.g. a cron entry).


The configuration file is a yaml file. Each parameter controls the
way the program acts.

* my_server_id the server id for the mysql replica. must be unique among the replica cluster.
* copy_max_memory the max amount of memory to use when copying the table in PostgreSQL. Is possible to specify the value in (k)ilobytes, (M)egabytes, (G)igabytes adding the suffix (e.g. 300M).
* my_database mysql database to replicate. a schema with the same name will be initialised in the postgres database.
* pg_database destination database in PostgreSQL. 
* copy_mode the allowed values are 'file'  and 'direct'. With direct the copy happens on the fly. With file the table is first dumped in a csv file then reloaded in PostgreSQL.
* hexify is a yaml list with the data types that require coversion in hex (e.g. blob, binary). The conversion happens on the copy and on the replica.
* log_dir directory where the logs are stored.
* log_level logging verbosity. allowed values are debug, info, warning, error.
* log_dest log destination. stdout for debugging purposes, file for the normal activity.
* my_charset mysql charset for the copy. Please note the replica library read is always in utf8.
* pg_charset PostgreSQL connection's charset. 
* tables_limit yaml list with the tables to replicate. If  the list is empty then the entire mysql database is replicated.
* sleep_loop seconds between a two replica  batches.
* pause_on_reindex determines whether to pause the replica if a reindex process is found in pg_stat_activity
* sleep_on_reindex seconds to sleep when a reindex process is found
* reindex_app_names  lists the application names to check for reindex (e.g. reindexdb). This is a workaround which required for keeping the replication user unprivileged. 
* source_name  this must be unique along the list of sources. The tool detects if there's a duplicate when registering a new source
* dest_schema this is also a unique value. once the source is registered the dest_schema can't be changed anymore
* log_append append to log file or truncate it at each restart
* batch_retention the max retention for the replayed batches rows in t_replica_batch. The field accepts any valid interval accepted by PostgreSQL

Reindex detection example setup

.. code-block:: yaml

    #Pause the replica for the given amount of seconds if a reindex process is found
    pause_on_reindex: Yes
    sleep_on_reindex: 30

    #list the application names which are supposed to reindex the database
    reindex_app_names:
    - 'reindexdb'
    - 'my_custom_reindex'



MySQL connection parameters
    
.. code-block:: yaml

    mysql_conn:
        host: localhost
        port: 3306
        user: replication_username
        passwd: never_commit_passwords


PostgreSQL connection parameters

.. code-block:: yaml

    pg_conn:
        host: localhost
        port: 5432
        user: replication_username
        password: never_commit_passwords


Usage
**********************
The script chameleon.py requires one of the following commands.

* drop_schema Drops the service schema sch_chameleon with cascade option. 
* create_schema Create the service schema sch_chameleon.
* upgrade_schema Upgrade an existing schema sch_chameleon to an newer version. 
* init_replica Create the table structure from the mysql into a PostgreSQL schema with the same mysql's database name. The mysql tables are locked in read only mode and  the data is  copied into the PostgreSQL database. The master's coordinates are stored in the PostgreSQL service schema. The command drops and recreate the service schema.
* start_replica Starts the replication from mysql to PostgreSQL using the master data stored in sch_chameleon.t_replica_batch. The master's position is updated time a new batch is processed. The command upgrade the service schema if required.
* list_config List the available configurations and their status ('ready', 'initialising','initialised','stopped','running')
* add_source register a new configuration file as source
* drop_source remove the configuration from the registered sources
* stop_replica ends the replica process gracefully
* disable_replica ends the replica process and disable the restart
* enable_replica enable the replica process
* sync_replica sync the data between mysql and postgresql without dropping the tables
* show_status displays the replication status for each source, with the lag in seconds and the last received event

the optional command **--config** followed by the configuration file name, without the yaml suffix, allow to specify different configurations.
If omitted the configuration defaults to **default**.

Example
**********************

Create a virtualenv and activate it
.. code-block:: none
    
    python3 -m venv venv
    source venv/bin/activate
    
    
Install pg_chameleon

.. code-block:: none
    
    pip install pg_chameleon


Run the script in order to create the configuration directory.

.. code-block:: none
    
    chameleon.py
    
    
cd in ~/.pg_chameleon/config and copy the configuration-example.yaml to default.yaml. Please note this is the default configuration and can be omitted when executing the chameleon.py script.

    
    
In MySQL create a user for the replica.

.. code-block:: sql

    CREATE USER usr_replica ;
    SET PASSWORD FOR usr_replica=PASSWORD('replica');
    GRANT ALL ON sakila.* TO 'usr_replica';
    GRANT RELOAD ON *.* to 'usr_replica';
    GRANT REPLICATION CLIENT ON *.* to 'usr_replica';
    GRANT REPLICATION SLAVE ON *.* to 'usr_replica';
    FLUSH PRIVILEGES;
    
Add the configuration for the replica to my.cnf (requires mysql restart)

.. code-block:: none
    
    binlog_format= ROW
    binlog_row_image=FULL
    log-bin = mysql-bin
    server-id = 1

If you are using a cascading replica configuration ensure the parameter 	log_slave_updates is set to ON.

.. code-block:: none
    
    log_slave_updates= ON

	
In PostgreSQL create a user for the replica and a database owned by the user

.. code-block:: sql

    CREATE USER usr_replica WITH PASSWORD 'replica';
    CREATE DATABASE db_replica WITH OWNER usr_replica;

Check you can connect to both databases from the replication system.

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

Setup the connection parameters in default.yaml

.. code-block:: yaml

    ---
    #global settings
    my_server_id: 100
    replica_batch_size: 1000
    my_database:  sakila
    pg_database: db_replica

    #mysql connection's charset. 
    my_charset: 'utf8'
    pg_charset: 'utf8'

    #include tables only
    tables_limit:

    #mysql slave setup
    mysql_conn:
        host: derpy
        port: 3306
        user: usr_replica
        passwd: replica

    #postgres connection
    pg_conn:
        host: derpy
        port: 5432
        user: usr_replica
        password: replica
    


Initialise the schema and the replica with


.. code-block:: none
    
    chameleon.py create_schema 
    chameleon.py add_source --config default
    chameleon.py init_replica --config default


Start the replica with


.. code-block:: none
    
	chameleon.py start_replica --config default
	

	


