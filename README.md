pg_chameleon
============

Migration and replica from MySQL to PostgreSQL

Current version: 0.1 DEVEL

# Setup instruction: 

* Download the package or git clone
* Create a virtual environment in the main app
* Install the required packages listed in requirements.txt 
* Create a user on mysql for the replica (e.g. usr_replica)
* Grant access to usr on the replicated database (e.g. GRANT ALL ON sakila.* TO 'usr_replica';)
* Grant RELOAD privilege to the user (e.g. GRANT RELOAD ON \*.\* to 'usr_replica';)
* Grant REPLICATION CLIENT privilege to the user (e.g. GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)
* Grant REPLICATION SLAVE privilege to the user (e.g. GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)


## Requirements
* PyMySQL==0.7.6
* argparse==1.2.1
* [mysql-replication==0.9](https://github.com/noplay/python-mysql-replication)
* psycopg2==2.6.2
* wsgiref==0.1.2
* PyYAML==3.11
* [daemonize==2.4.7](https://pypi.python.org/pypi/daemonize/)


Copy config-example.yaml in config.yaml and edit the connection settings.

# Replica setup
The script pg_chameleon.py have a very basic command line interface. Accepts three commands

* init_schema Initialises the PostgreSQL service schema sch_chameleon.  **Warning!! It drops the existing schema**
* init_replica Creates the table structure and copy the data from mysql locking the tables in read only mode. It saves the master status in sch_chameleon.t_replica_batch.
* start_replica Starts the replication from mysql to PostgreSQL using the master data stored in sch_chameleon.t_replica_batch and update the master position every time an new batch is processed.

The demonisation of the replica process is currently WIP.

# Example
We'll use the sakila schema available [for download here](https://dev.mysql.com/doc/index-other.html)
## MySQL
Create a user for the replica.

    CREATE USER usr_replica ;
    SET PASSWORD FOR usr_replica=PASSWORD('replica');
    GRANT ALL ON sakila.* TO 'usr_replica';
    GRANT RELOAD ON \*.\* to 'usr_replica';
    GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';
    GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';
    FLUSH PRIVILEGES;
    
Configure mysql for the replica (requires restart)
    
    binlog_format= ROW
    log-bin = mysql-bin
    server-id = 1


## PostgreSQL
Create a user for the replica and a database owned by the user

    CREATE USER usr_replica WITH PASSWORD 'replica';
    CREATE DATABASE db_replica WITH OWNER usr_replica;

## Setup the replica
Check you can connect to both databases from the replication system.

For MySQL

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

    psql  -h derpy -U usr_replica db_replica
    Password for user usr_replica: 
    psql (9.5.4)
    Type "help" for help.

    db_replica=> 

Setup the connection parameters in config.yaml

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
    

Initialise the schema on PostgreSQL with


    ./pg_chameleon.py init_schema
    getting table metadata
    Creating service schema

Initialise the replica with 

    (env)thedoctor@tardis:~/Documents/git/github/pg_chameleon$ ./pg_chameleon.py init_replica
    getting table metadata
    locking the tables
    copying table category
    Processed 1 slices out of 1 
    copying table city
    Processed 1 slices out of 1 
    copying table store
    Processed 1 slices out of 1 
    copying table film_text
    Processed 1 slices out of 1 
    copying table language
    Processed 1 slices out of 1 
    copying table country
    Processed 1 slices out of 1 
    copying table actor
    Processed 1 slices out of 1 
    copying table film_category
    Processed 1 slices out of 1 
    copying table customer
    Processed 1 slices out of 1 
    copying table film_actor
    Processed 1 slices out of 1 
    copying table inventory
    Processed 1 slices out of 1 
    copying table address
    Processed 1 slices out of 1 
    copying table rental
    Processed 1 slices out of 1 
    copying table payment
    Processed 1 slices out of 1 
    copying table film
    Processed 1 slices out of 1 
    copying table staff
    Processed 1 slices out of 1 
    releasing the lock
    saving master data
    done
    creating indices


Start the replica with



    ./pg_chameleon.py start_replica
    getting table metadata
    run a full resync before starting the replica
    start replica stream[(1L, 'mysql-bin.000008', 8624, 't_log_replica_1')]
    working out master datamysql-bin.000008 8624
    saving master data
    stream empty processing batch
    sleeping 10 seconds



# Platform and versions

The library is being developed on Ubuntu 14.04 with python 2.7.6.

The databases source and target are:

* MySQL: 5.6.30 on FreeBSD 10.3
* PostgreSQL: 9.5.4 on FreeBSD 10.3
  
# What does it work
* Read the schema specifications from MySQL and replicate the same structure it into PostgreSQL
* Locks the tables in mysql and gets the master coordinates
* Create primary keys and indices on PostgreSQL
* Write in PostgreSQL frontier table

 
# What seems to work
* Enum support
* Blob import into bytea (needs testing)
* Read replica from MySQL
* Copy the data from MySQL to PostgreSQL on the fly
* Replay of the replicated data in PostgreSQL
 
# What does'n work
* DDL replica 
* Materialisation of the MySQL views
* Foreign keys build on PostgreSQL

# Test please!

This software is in a very early stage of development. 
Please submit the issues you find and please *do not use it in production* unless you know what you're doing.


