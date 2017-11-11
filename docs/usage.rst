Usage instructions
**************************************************

Command line reference
............................................

.. code-block:: bash
    
    chameleon.py command [ [ --config ] [ --source ] [ --schema ]  [ --tables ] [ --debug ] ] [ --version ] 

.. csv-table:: Options 
   :header: "Option", "Description", "Default","Example"
   
   ``--config``, Specifies the configuration to use in ``~.pg_chameleon/configuration/``. The configuration name should be the file without the extension ``.yml`` , ``default``,``--config foo`` will search for ``~.pg_chameleon/configuration/foo.yml``


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
