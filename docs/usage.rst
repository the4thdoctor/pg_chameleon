Usage instructions
**************************************************

Command line reference
............................................

.. code-block:: bash
    
    chameleon.py command [ [ --config ] [ --source ] [ --schema ]  [ --tables ] [ --debug ] ] [ --version ] 

.. csv-table:: Options 
   :header: "Option", "Description", "Default","Example"
   
   ``--config``, Specifies the configuration to use in ``~.pg_chameleon/configuration/``. The configuration name should be the file without the extension ``.yml`` , ``default``,``--config foo`` will use the file ``~.pg_chameleon/configuration/foo.yml``
   ``--source``, Specifies the source within a configuration file., N/A, ``--source bar``
   ``--schema``, Specifies a schema configured within a source., N/A, ``--schema schema_foo``
   ``--tables``, Specifies one or more tables configured in a schema. Multiple tables can be specified separated by comma. The table must have the schema., N/A, ``--tables schema_foo.table_bar``
   ``--debug``,When added to the command line the debug option disables any daemonisation and outputs all the logging to the console. The keybord interrupt signal is trapped correctly., N/A, ``--debug``
   ``--version``,Displays the package version., N/A, ``--version``

   
   
.. csv-table:: Command list reference
   :header: "Command", "Description", "Options"
      
    ``set_configuration_files``, Setup the example configuration files and directories in ``~.pg_chameleon``
    ``show_config``, Displays the configuration  for the configuration, ``--config``
    ``show_sources``, Displays the sourcches configured for the configuration, ``--config``
   ``show_status``,Displays an overview of the status of the sources configured within the configuration , ``--config``
   ``create_replica_schema``, Creates a new replication schema into the config's destination database, ``--config``
    ``drop_replica_schema``, Drops an existing replication schema from the config's destination database, ``--config``
    ``upgrade_replica_schema``,**not implemented yet**
    add_source
    drop_source
    init_replica
    update_schema_mappings
    refresh_schema
    sync_tables
    start_replica, 
    stop_replica
    detach_replica
