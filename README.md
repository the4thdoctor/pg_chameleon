pg_chameleon
============

Migration and replica from MySQL to PostgreSQL

Current version: 0.1 DEVEL

# Setup instruction: 

* Download the package or git clone
* Create a virtual environment in the main app
* Install the required packages listed in requirements.txt 

## Requirements
* PyMySQL==0.7.6
* argparse==1.2.1
* mysql-replication==0.9
* psycopg2==2.6.2
* wsgiref==0.1.2
* PyYAML==3.11

Copy config-example.yaml in config.yaml and edit the connection settings.

Run the example script init_replica.py. This will copy the mysql schema/data in PostgreSQL. 

**The script init_replica.py drops any schema and object present in PostgreSQL** use it at your own risk.

# Platform and versions

The library is being developed on Ubuntu 14.04 with python 2.7.6.

The databases source and target are:

* MySQL: 5.6.30 on FreeBSD 10.3
* PostgreSQL: 9.5.3 on FreeBSD 10.3
  
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
 
 
# What does'n work
  * DDL replica 
  * Materialisation of the MySQL views
  * Foreign keys build on PostgreSQL

# Test please!

This software is in a very early stage of development. 
Please submit the issues you find and please *do not use it in production* unless you know what you're doing.
