.. pg_chameleon documentation master file, created by
   sphinx-quickstart on Wed Sep 14 22:19:28 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pg_chameleon MySQL to PostgreSQL replica
=========================================================

.. image:: ../images/pgchameleon.png
        :align: right
	:scale: 70%




pg_chameleon is a replication tool from MySQL to PostgreSQL developed in Python 2.7 and Python 3.3+
The system use the library mysql-replication to pull the row images from MySQL which are transformed into a jsonb object. 
A pl/pgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an  initial replica setup which pulls the data from MySQL in read only mode. 
This is done by the tool running FLUSH TABLE WITH READ LOCK;  .

pg_chameleon can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.



`Documentation available at pgchameleon.org <http://www.pgchameleon.org/documents/index.html>`_

`Release available via pypi <https://pypi.python.org/pypi/pg_chameleon/>`_

RELEASE NOTES
********************

.. toctree::
   :maxdepth: 2

    <./release_notes.rst>


README
********************

.. toctree::
   :maxdepth: 2

    <./readme.rst>


MODULE REFERENCE
***********************************

.. toctree::
   :maxdepth: 2
   
    Module global_lib <./global_lib.rst>
    Module mysql_lib <./mysql_lib.rst>
    Module pg_lib <./pg_lib.rst>
    Module sql_util <./sql_util.rst>



CHANGELOG
********************

.. toctree::
   :maxdepth: 2
    
    <./changelog.rst>
    
   

	

