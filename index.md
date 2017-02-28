### pg_chameleon a lightweigth MySQL to PostgreSQL replica

![Alt text](images/pgchameleon.png "Igor the chameleon")

The chameleon logo was made by [Elena Toma](http://tonkipappero.deviantart.com/ "Tonkipappero's Art")

Pg_chameleon is a replication tool from MySQL to PostgreSQL developed in Python 2.7 and 3.6. The system relies on
the [mysql-replication](https://github.com/noplay/python-mysql-replication "mysql-replication") library to pull the changes from MySQL and covert them into a jsonb object.
A plpgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool can initialise the replica pulling out the data from MySQL but this requires the FLUSH TABLE WITH READ LOCK; to work properly.

The tool can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.


The latest release is the [v1.0-alpha.4](https://github.com/the4thdoctor/pg_chameleon/releases/tag/v1.0-alpha.4)

This release is available via pypi. Please ensure you are running the latest pip version before installing pg_chameleon. 


#### Branches and testing

The branch currently developed is the pgchameleon_v1. 

The branch pgchameleon_v2 is the pure python 3 project's revamp.
I will start active development when psycopg2 2.7 will become available.


#### Installation in virtualenv

For working properly you should use virtualenv and install it using *pip install pg_chameleon*.

The script should be executed in a screen session to keep it running or using cron. 
Currently there's no respawning of the process or failure detection.

#### psycopg2 requires python and postgresql dev files

The psycopg2's pip installation requires the python development files and postgresql source code.
Please refer to your distribution for fulfilling those requirements.

#### DDL replica limitations

DDL and DML mixed in the same transaction are not decoded in the right order. 
This can affect the replica because of a wrong jsonb descriptor. 
I know the issue and I'm trying to address the problem and therefore build on a solution.

### pg_chameleon a lightweight replication system!
A recording of a presentation  bout pg_chameleon is available on the Brighton PostgreSQL meetup page.

Unfortunately the audio is suboptimal.

[![IMAGE ALT TEXT](http://img.youtube.com/vi/ZZeBGDpUhec/0.jpg)](http://www.youtube.com/watch?v=ZZeBGDpUhec "pg_chameleon a lightweight replication system")
