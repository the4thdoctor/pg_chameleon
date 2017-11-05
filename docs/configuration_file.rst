The configuration file
********************************

The file config-example.yaml is stored in **~/.pg_chameleon/configuration** and should be used as template for the other configuration files. 

Global settings
..............................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 1-10
   :linenos:

* pid_dir directory where the process pids are saved.
* log_dir directory where the logs are stored.
* log_level logging verbosity. allowed values are debug, info, warning, error.
* log_dest log destination. stdout for debugging purposes, file for the normal activity.

PostgreSQL target connection
...............................................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 11-18
   :linenos:

sources configuration
...............................................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 20-49
   :linenos:

type override
...............................................

.. literalinclude:: ../configuration/config-example.yml
   :language: yaml
   :lines: 52-60
   :linenos:
