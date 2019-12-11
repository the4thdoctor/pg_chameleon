.. pg_chameleon documentation master file, created by
   sphinx-quickstart on Wed Sep 14 22:19:28 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pg_chameleon MySQL to PostgreSQL replica
=========================================================

.. image:: ../images/pgchameleon.png
        :align: right
	




pg_chameleon is a replication tool from MySQL to PostgreSQL developed in  Python 3.5+
The system use the library mysql-replication to pull the row images from MySQL which are transformed into a jsonb object. 
A pl/pgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an  initial replica setup which pulls the data from MySQL in read only mode. 

pg_chameleon can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.



`Documentation available at pgchameleon.org <http://www.pgchameleon.org/documents/index.html>`_

`Release available via pypi <https://pypi.python.org/pypi/pg_chameleon/>`_


FEATURES
*************************
* Replicates multiple MySQL schemas within the same MySQL cluster into a target PostgreSQL database. The source and target schema names can be different.
* Conservative approach to the replica. Tables which generate errors are automatically excluded from the replica.
* Daemonised init_replica,refresh_schema,sync_tables processes.
* Daemonised replica process with two separated subprocess, one for the read and one for the replay.
* Soft replica initialisation. The tables are locked when needed and stored with their log coordinates. The replica damon will put the database in a consistent status gradually.
* Rollbar integration for a simpler error detection and alerting.


CHANGELOG
********************

.. toctree::
   :maxdepth: 2
    
    <./changelog.rst>
    
   



RELEASE NOTES
********************

.. toctree::
   :maxdepth: 2

    <./release_notes.rst>


Upgrade procedure
********************

.. toctree::
   :maxdepth: 2

    <./upgrade_procedure.rst>

    
README
********************

.. toctree::
   :maxdepth: 2

    <./readme.rst>

The configuration file
***********************************

.. toctree::
   :maxdepth: 2
    
    <./configuration_file.rst>
	

    
Usage instructions
***********************************

.. toctree::
   :maxdepth: 2
    
    <./usage.rst>

Module reference
***********************************

.. toctree::
   :maxdepth: 2
   
    Module global_lib <./global_lib.rst>
    Module mysql_lib <./mysql_lib.rst>
    Module pg_lib <./pg_lib.rst>
    Module sql_util <./sql_util.rst>



