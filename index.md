### pg_chameleon a lightweigth MySQL to PostgreSQL replica

![Alt text](images/pgchameleon.png "Igor the chameleon")

The chameleon logo was made by [Elena Toma](http://tonkipappero.deviantart.com/ "Tonkipappero's Art")

Pg_chameleon is a replication tool from MySQL to PostgreSQL developed in Python 2.7 and 3.6. The system uses on
the [mysql-replication](https://github.com/noplay/python-mysql-replication "mysql-replication") library to pull the changes from MySQL and covert them into a jsonb object.
A plpgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool can initialise the replica pulling out the data from MySQL but this requires the FLUSH TABLE WITH READ LOCK; to work properly.

The tool can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.


The latest release is the v1.1

This release is [available on pypi](https://pypi.python.org/pypi/pg_chameleon)

The documentation is [available on here](http://pythonhosted.org/pg_chameleon/)

Please ensure you are running the latest pip version before installing pg_chameleon. 

The full changelog is [available here](https://github.com/the4thdoctor/pg_chameleon/blob/master/CHANGELOG.rst) 


#### Branches and testing

The branch currently developed is the pgchameleon_v1. 

The branch pgchameleon_v2 is currently under development and it works python3 only.

#### Caveats 
##### Installation in virtualenv

For working properly you should use virtualenv and install it using *pip install pg_chameleon*.


##### No daemon
The replica process should be executed in a cron job in order to keep it running. As the replica detects if there's already another
running process, the cron job can be executed frequently (e.g. every 30 minutes) without issues.

There's no respawning of the process or failure detection.

##### DDL replica limitations

DDL and DML mixed in the same transaction are not decoded in the right order. 
This can affect the replica because of a wrong jsonb descriptor. 

### pg_chameleon a lightweight replication system!
A recording of a presentation  bout pg_chameleon is available on the Brighton PostgreSQL meetup page.

Unfortunately the audio is suboptimal.

[![IMAGE ALT TEXT](http://img.youtube.com/vi/ZZeBGDpUhec/0.jpg)](http://www.youtube.com/watch?v=ZZeBGDpUhec "pg_chameleon a lightweight replication system")
