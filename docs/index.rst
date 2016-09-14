.. pg_chameleon documentation master file, created by
   sphinx-quickstart on Wed Sep 14 22:19:28 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pg_chameleon's documentation!
========================================

Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

pg_chameleon
##############


Migration and replica from MySQL to PostgreSQL

Current version: 0.1 DEVEL

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
* PyMySQL==0.7.6 
* argparse==1.2.1
* `mysql-replication==0.9 <https://github.com/noplay/python-mysql-replication>`_
* psycopg2==2.6.2
* wsgiref==0.1.2
* PyYAML==3.11
* `daemonize==2.4.7 <https://pypi.python.org/pypi/daemonize/>`_


Configuration parameters
********************************
The configuration file is a yaml file. Each parameter controls the
way the program acts.

* my_server_id the server id for the mysql replica. must be unique among the replica cluster
* replica_batch_size the number of row replicated in each replica run
* copy_max_size the max rows pulled out in each slice when copying the table in postgresql
* my_database mysql database to replicate. a schema with the same name will be initialised in the postgres database
* pg_database destination database in postgresql. 
* copy_mode the allowed values are 'file'  and 'direct'. With direct the copy happens on the fly. With file the table is first dumped in a csv file then reloaded in postgresql.
* hexify is a yaml list with the data types that require coversion in hex (e.g. blob, binary). The conversion happens on the copy and on the replica.
* log_dir directory where the logs are stored
* log_level logging verbosity. allowed values are debug, info, warning, error
* log_dest log destination. stdout for debugging purposes, file for the normal activity.
* my_charset mysql charset for the copy (please note the replica is always in utf8)
* pg_charset postgresql connection's charset. 
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

Replica setup
**********************
The script pg_chameleon.py have a very basic command line interface. Accepts three commands

* init_schema Initialises the PostgreSQL service schema sch_chameleon.  **Warning!! It drops the existing schema**
* init_replica Creates the table structure and copy the data from mysql locking the tables in read only mode. It saves the master status in sch_chameleon.t_replica_batch.
* start_replica Starts the replication from mysql to PostgreSQL using the master data stored in sch_chameleon.t_replica_batch and update the master position every time an new batch is processed.

