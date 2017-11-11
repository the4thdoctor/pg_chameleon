.. image:: https://img.shields.io/github/issues/the4thdoctor/pg_chameleon.svg   
        :target: https://github.com/the4thdoctor/pg_chameleon/issues

.. image:: https://img.shields.io/github/forks/the4thdoctor/pg_chameleon.svg   
        :target: https://github.com/the4thdoctor/pg_chameleon/network

.. image:: https://img.shields.io/github/stars/the4thdoctor/pg_chameleon.svg   
        :target: https://github.com/the4thdoctor/pg_chameleon/stargazers
	
.. image:: https://img.shields.io/badge/license-BSD-blue.svg   
        :target: https://raw.githubusercontent.com/the4thdoctor/pg_chameleon/master/LICENSE
	
.. image:: https://travis-ci.org/the4thdoctor/pg_chameleon.svg
    :target: https://travis-ci.org/the4thdoctor/pg_chameleon
    
pg_chameleon  v2.0alpha1 `is available on pypi test for testing  <https://pypi.python.org/pypi/pg_chameleon>`_  

**This is a pre-release version and shouldn't be used in production.**

Please report any issue at `https://github.com/the4thdoctor/pg_chameleon/issues <https://github.com/the4thdoctor/pg_chameleon/issues>`_  

The documentation `is available on pgchameleon.org <http://www.pgchameleon.org/documents_v2/index.html>`_ 

Live community `available on gitter <https://gitter.im/pg_chameleon/Lobby>`_

Please submit your `bug reports on GitHub <https://github.com/the4thdoctor/pg_chameleon>`_.


Platform and versions
****************************

The tool is developed using Linux Slackware 14.2. 


The database server is a FreeBSD  11.0 with MySQL: 5.6 and PostgreSQL: 9.5 installed.

Example scenarios 
..............................

* Analytics 
* Migrations
* Data aggregation from multiple MySQL databases
  
Features
..............................

* Read from multiple MySQL schemas and  restore them it into a target PostgreSQL  database. The source and target schema names can be different.
* Setup PostgreSQL to act as a MySQL slave.
* Support for enumerated and binary data types.
* Basic DDL Support (CREATE/DROP/ALTER TABLE, DROP PRIMARY KEY/TRUNCATE, RENAME).
* Discard of rubbish data coming from the replica. 
* Conservative approach to the replica. Tables which generate errors are automatically excluded from the replica.
* Possibilty to refresh single tables or single schemas.
* Basic replica monitoring.
* Detach replica from MySQL for migration support.
* Data type override (e.g. tinyint(1) to boolean)
* Daemonised init_replica process.
* Daemonised replica process with two separated subprocess, one for the read and one for the replay.
* Rollbar integration



Requirements
******************

Python: CPython 3.3+ on Linux

MySQL: 5.5+

PostgreSQL: 9.5+

* `PyMySQL <https://pypi.python.org/pypi/PyMySQL>`_ 
* `argparse <https://pypi.python.org/pypi/argparse>`_
* `mysql-replication <https://pypi.python.org/pypi/mysql-replication>`_
* `psycopg2 <https://pypi.python.org/pypi/psycopg2>`_
* `PyYAML <https://pypi.python.org/pypi/PyYAML>`_
* `tabulate <https://pypi.python.org/pypi/tabulate>`_

Optionals for building documentation

* `sphinx <http://www.sphinx-doc.org/en/stable/>`_
* `sphinx-autobuild <https://github.com/GaretJax/sphinx-autobuild>`_



Caveats
..............................
The replica requires the tables to have a primary key. Tables without primary key are initialised during the init_replica process but the replica
doesn't update them.

The copy_max_memory is just an estimate. The average rows size is extracted from mysql's informations schema and can be outdated.
If the copy process fails for memory error check the failing table's row length and the number of rows for each slice. 

Python 3 is supported only from version 3.3 as required by mysql-replication .

The lag is determined using the last received event timestamp and the postgresql timestamp. If the mysql is read only the lag will increase because
no replica event is coming in. 

The detach replica process resets the sequences in postgres to let the database work standalone. The foreign keys from the source MySQL schema are extracted and created initially as NOT VALID.  The foreign keys are created without the ON DELETE or ON UPDATE clauses.
A second run tries to validate the foreign keys. If an error occurs it gets logged out according to the source configuration. 



Setup 
*****************

* Create a virtual environment (e.g. python3 -m venv venv)
* Activate the virtual environment (e.g. source venv/bin/activate)
* Upgrade pip with **pip install pip --upgrade**
* Install pg_chameleon with **pip install pg_chameleon==2.0a1**. 
* Create a user on mysql for the replica (e.g. usr_replica)
* Grant access to usr on the replicated database (e.g. GRANT ALL ON sakila.* TO 'usr_replica';)
* Grant RELOAD privilege to the user (e.g. GRANT RELOAD ON \*.\* to 'usr_replica';)
* Grant REPLICATION CLIENT privilege to the user (e.g. GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)
* Grant REPLICATION SLAVE privilege to the user (e.g. GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)



Configuration directory
********************************
The system wide install is now supported correctly. 

The first time chameleon.py is executed it creates a configuration directory in $HOME/.pg_chameleon.
Inside the directory there are two subdirectories. 


* configuration is where the configuration files are stored. 
* pid is where the replica pid file is created. it can be changed in the configuration file
* logs is where the replica logs are saved if log_dest is file. It can be changed in the configuration file

You should  use config-example.yaml as template for the other configuration files. 
Check the `configuration file reference <http://www.pgchameleon.org/documents_v2/configuration_file.html>`_   for an overview.

