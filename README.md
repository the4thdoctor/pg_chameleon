pg_chameleon
============

Migration and replica from MySQL to PostgreSQL

Current version: 0.1 ALPHA

Setup instruction: 

  * Download the package or git clone
  * Create a virtual environment in the main app
  * Install the required packages listed in requirements.txt
  
  * PyMySQL==0.7.6
  * argparse==1.2.1
  * mysql-replication==0.9
  * psycopg2==2.6.2
  * wsgiref==0.1.2
  * PyYAML==3.11

  
# What does works
Read the schema specifications from MySQL and replicate the same structure it into PostgreSQL
Copy the data from MySQL to PostgreSQL with the copy option 
Locks the tables in mysql and gets the master coordinates
Create primary keys and indices on PostgreSQL

# What does'n works
Replica from MySQL
Materialisation of the MySQL views
Foreign keys build on PostgreSQL

# Test!

This software is in a very early stage of development. 
Please submit the issues you find and please *do not use it in production* unless you know what you're doing.