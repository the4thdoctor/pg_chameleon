Upgrade procedure 1.8 to 2.0
********************************
pg_chameleon 2.0 can upgrade an existing 1.8 replica catalogue using the command ``upgrade_replica_schema``.
As the new version supports different schema mappings  within the same source the parameter ``schema_mappings`` must match all the pairs
``my_database destination_schema`` for the source database that we are configuring. 
Any discrepancy will abort the upgrade procedure.

Preparation
..............................
* Check the pg_chameleon version you are upgrading is 1.8.2. If not upgrade it and **start the replica  for each source present in the old catalogue**.
   This step is required in order to have the destination and target schema's updated from the configuration files.
* Check the replica catalogue version is 1.7 with ``SELECT * FROM sch_chameleon.v_version;``.
* Check the  field t_source_schema have a schema name set ``SELECT t_source_schema  FROM sch_chameleon.t_sources;``
* Take a backup of the existing schema sch_chameleon with ``pg_dump``
* Install pg_chameleon version 2 and create the configuration files executing ``chameleon set_configuration_files``. 
* cd in ``~/.pg_chameleon/configuration/`` and copy the file ``config-example.yml`` in a different file ``e.g. cp config-example.yml upgraded.yml``
* Edit  the file and set the target and source's database connections. You may want to change the source name as well 


For each configuration file in the old setup ``~/.pg_chameleon/config/`` using the MySQL database configured in the source you should get the values stored in 
``my_database`` and ``destination_schema`` and add it to the new source's schema_mappings.

For example, if there are two sources ``source_01.yaml`` and ``source_02.yaml`` with the following configuration:

Both sources are pointing the same MySQL database

.. code-block:: yaml

    mysql_conn:
        host: my_host.foo.bar
        port: 3306
        user: my_replica
    passwd: foo_bar

source_01.yaml have the following schema setup

.. code-block:: yaml

    my_database: my_schema_01
    destination_schema: db_schema_01

	
source_02.yaml have the following schema setup

.. code-block:: yaml

    my_database: my_schema_02
    destination_schema: db_schema_02
    
The new source's database configuration  should be

.. code-block:: yaml

    mysql:
        db_conn:
            host: "my_host.foo.bar"
            port: "3306"
            user: "my_replica"
            password: "foo_bar"
            charset: 'utf8'
            connect_timeout: 10
        schema_mappings:
                my_schema_01: db_schema_01
                my_schema_02: db_schema_02

		
Upgrade
..............................

Execute the following command 
``chameleon upgrade_replica_schema --config upgraded``

The procedure checks if the start catalogue version is 1.7 and fails if the value is different.
After answering YES the procedure executes the following steps.

* Replays any exising batches present in the catalogue 1.7
* Checks if the schema_mappings are compatible with the values stored in the schema ``sch_chameleon``
* Renames the schema ``sch_chameleon`` to ``_sch_chameleon_version1``
* Installs a new 2.0 schema in ``sch_chameleon``
* Stores a new source using the schema mappings 
* Migrates the existing tables into the new catalogue using the replica batch data to store the tables start of consistent point.
* Determines maximum and minimum point for the binlog coordinates and use them for writing the new batch start point and the source's consistent point

If the migration is successful, before starting the replica process is better to check that all tables are correctly mapped with 

``chameleon show_status --source upgraded``


Rollback
..............................

If something goes wrong during the  upgrade procedure, then the changes are rolled back. 
The schema ``sch_chameleon`` is renamed to  ``_sch_chameleon_version2`` and the previous version's schema ``_sch_chameleon_version1`` is put batck to ``sch_chameleon``.
If this happens  the procedure 1.8.2 will continue to work as usual. The schema ``_sch_chameleon_version2`` can be used to check what went wrong.

Before attempting a new upgrade schema  ``_sch_chameleon_version2`` should be dropped or renamed in order to avoid a schema conflict in the case of another failure.
