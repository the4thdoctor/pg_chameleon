pg_chameleon
##############


Migration and replica from MySQL to PostgreSQL

Current version: 0.1 DEVEL

.. image:: https://readthedocs.org/projects/pg-chameleon/badge/?version=latest
    :target: http://pg-chameleon.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

`Documentation available at readthedocs <http://pg-chameleon.readthedocs.io/en/latest/>`_

Setup 
**********

* Download the package or git clone
* Create a virtual environment in the main app
* Install the required packages listed in requirements.txt 
* Create a user on mysql for the replica (e.g. usr_replica)
* Grant access to usr on the replicated database (e.g. GRANT ALL ON sakila.* TO 'usr_replica';)
* Grant RELOAD privilege to the user (e.g. GRANT RELOAD ON \*.\* to 'usr_replica';)
* Grant REPLICATION CLIENT privilege to the user (e.g. GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)
* Grant REPLICATION SLAVE privilege to the user (e.g. GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)


Requirements
******************
* `PyMySQL==0.7.6 <https://github.com/PyMySQL/PyMySQL>`_ 
* `argparse==1.2.1 <https://github.com/bewest/argparse>`_
* `mysql-replication==0.9 <https://github.com/noplay/python-mysql-replication>`_
* `psycopg2==2.6.2 <https://github.com/psycopg/psycopg2>`_
* `PyYAML==3.11 <https://github.com/yaml/pyyaml>`_
* `daemonize==2.4.7 <https://pypi.python.org/pypi/daemonize/>`_
* `sphinx==1.4.6 <http://www.sphinx-doc.org/en/stable/>`_
* `sphinx-autobuild==0.6.0 <https://github.com/GaretJax/sphinx-autobuild>`_

Configuration parameters
********************************
The configuration file is a yaml file. Each parameter controls the
way the program acts.

* my_server_id the server id for the mysql replica. must be unique among the replica cluster
* copy_max_memory the max amount of memory to use when copying the table in PostgreSQL. Is possible to specify the value in (k)ilobytes, (M)egabytes, (G)igabytes adding the suffix (e.g. 300M)
* my_database mysql database to replicate. a schema with the same name will be initialised in the postgres database
* pg_database destination database in PostgreSQL. 
* copy_mode the allowed values are 'file'  and 'direct'. With direct the copy happens on the fly. With file the table is first dumped in a csv file then reloaded in PostgreSQL.
* hexify is a yaml list with the data types that require coversion in hex (e.g. blob, binary). The conversion happens on the copy and on the replica.
* log_dir directory where the logs are stored
* log_level logging verbosity. allowed values are debug, info, warning, error
* log_dest log destination. stdout for debugging purposes, file for the normal activity.
* my_charset mysql charset for the copy (please note the replica is always in utf8)
* pg_charset PostgreSQL connection's charset. 
* tables_limit yaml list with the tables to replicate. if empty the entire mysql database is replicated.

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
The script pg_chameleon.py have a very basic command line interface. Accepts three commands

* drop_schema Drops the schema sch_chameleon with cascade option
* create_schema Create the schema sch_chameleon
* upgrade_schema Upgrade an existing schema sch_chameleon
* init_replica Creates the table structure and copy the data from mysql locking the tables in read only mode. It saves the master status in sch_chameleon.t_replica_batch.
* start_replica Starts the replication from mysql to PostgreSQL using the master data stored in sch_chameleon.t_replica_batch and update the master position every time an new batch is processed.

After running init_schema and init_replica start replica will initiate the mysql to PostgreSQL replication.

Example
**********************

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
    log-bin = mysql-bin
    server-id = 1

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
    psql (9.5.4)
    Type "help" for help.
    db_replica=> 

Setup the connection parameters in config.yaml

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
    
    ./pg_chameleon.py create_schema
    ./pg_chameleon.py init_replica


Start the replica with


.. code-block:: none
    
    ./pg_chameleon.py start_replica
	

Platform and versions
****************************

The library is being developed on Ubuntu 14.04 with python 2.7.6.

The databases source and target are:

* MySQL: 5.6.32 on FreeBSD 10.3
* PostgreSQL: 9.5.4 on FreeBSD 10.3
  
What does it work
..............................
* Read the schema specifications from MySQL and replicate the same structure it into PostgreSQL
* Locks the tables in mysql and gets the master coordinates
* Create primary keys and indices on PostgreSQL
* Write in PostgreSQL frontier table

 
What does seems to work
..............................
* Enum support
* Blob import into bytea (needs testing)
* Read replica from MySQL
* Copy the data from MySQL to PostgreSQL on the fly
* Replay of the replicated data in PostgreSQL
 
What does'n work
..............................
* DDL replica 
* Materialisation of the MySQL views
* Foreign keys build on PostgreSQL

Test please!
..............................

This software is in a very early stage of development. 
Please submit the issues you find and please **do not use it in production** unless you know what you're doing.



