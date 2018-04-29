.. image:: https://img.shields.io/github/issues/the4thdoctor/pg_chameleon.svg   
        :target: https://github.com/the4thdoctor/pg_chameleon/issues

.. image:: https://img.shields.io/github/forks/the4thdoctor/pg_chameleon.svg   
        :target: https://github.com/the4thdoctor/pg_chameleon/network

.. image:: https://img.shields.io/github/stars/the4thdoctor/pg_chameleon.svg   
        :target: https://github.com/the4thdoctor/pg_chameleon/stargazers
	
.. image:: https://img.shields.io/badge/license-BSD-blue.svg   
        :target: https://raw.githubusercontent.com/the4thdoctor/pg_chameleon/master/LICENSE
	
.. image:: https://api.travis-ci.org/the4thdoctor/pg_chameleon.svg?branch=ver2.0
    :target: https://travis-ci.org/the4thdoctor/pg_chameleon
    
pg_chameleon is a MySQL to PostgreSQL replica system written in Python 3. 
The system use the library mysql-replication to pull the row images from MySQL which are stored into PostgreSQL as JSONB. 
A pl/pgsql function decodes the jsonb values and replays the changes against the PostgreSQL database.
    
pg_chameleon  2.0 `is available on pypi <https://pypi.org/project/pg_chameleon/>`_  

The documentation `is available on pgchameleon.org <http://www.pgchameleon.org/documents_v2/index.html>`_ 

Live chat `available on gitter <https://gitter.im/pg_chameleon/Lobby>`_

Please submit your `bug reports on GitHub <https://github.com/the4thdoctor/pg_chameleon>`_.


Requirements
******************

Replica host
..............................

Operating system: Linux, FreeBSD, OpenBSD
Python: CPython 3.3+ 

* `PyMySQL <https://pypi.python.org/pypi/PyMySQL>`_ 
* `argparse <https://pypi.python.org/pypi/argparse>`_
* `mysql-replication <https://pypi.python.org/pypi/mysql-replication>`_
* `psycopg2 <https://pypi.python.org/pypi/psycopg2>`_
* `PyYAML <https://pypi.python.org/pypi/PyYAML>`_
* `tabulate <https://pypi.python.org/pypi/tabulate>`_
* `rollbar <https://pypi.python.org/pypi/rollbar>`_
* `daemonize <https://pypi.python.org/pypi/daemonize>`_

Optionals for building documentation

* `sphinx <http://www.sphinx-doc.org/en/stable/>`_
* `sphinx-autobuild <https://github.com/GaretJax/sphinx-autobuild>`_


Origin database
.................................
MySQL: 5.5+

Destination database
..............................
PostgreSQL: 9.5+


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





Caveats
..............................
The replica requires the tables to have a primary or unique key. Tables without primary/unique key are initialised during the init_replica process but not replicated.

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
* Install pg_chameleon with **pip install pg_chameleon**. 
* Create a user on mysql for the replica (e.g. usr_replica)
* Grant access to usr on the replicated database (e.g. GRANT ALL ON sakila.* TO 'usr_replica';)
* Grant RELOAD privilege to the user (e.g. GRANT RELOAD ON \*.\* to 'usr_replica';)
* Grant REPLICATION CLIENT privilege to the user (e.g. GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)
* Grant REPLICATION SLAVE privilege to the user (e.g. GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)



Configuration directory
********************************
The system wide install is now supported correctly. 

The configuration is set with the command ``chameleon set_configuration_files`` in $HOME/.pg_chameleon .
Inside the directory there are three subdirectories. 


* configuration is where the configuration files are stored. 
* pid is where the replica pid file is created. it can be changed in the configuration file
* logs is where the replica logs are saved if log_dest is file. It can be changed in the configuration file

You should  use config-example.yaml as template for the other configuration files. 
Check the `configuration file reference <http://www.pgchameleon.org/documents_v2/configuration_file.html>`_   for an overview.

