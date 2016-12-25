## pg_chameleon a lightweigth MySQL to PostgreSQL replica

![Alt text](images/pgchameleon.png "Igor the chameleon")

The chameleon logo was made by [Elena Toma](http://tonkipappero.deviantart.com/ "Tonkipappero's Art")

Pg_chameleon is a replication tool from MySQL to PostgreSQL developed in Python 2.7. The system relies on
the [mysql-replication](https://github.com/noplay/python-mysql-replication "mysql-replication") library to pull the changes from MySQL and covert them into a jsonb object.
A plpgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool can initialise the replica pulling out the data from MySQL but this requires the FLUSH TABLE WITH READ LOCK; to work properly.

The tool can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.

#### Branches and testing

The revamp branch is the development branch and should't be used for testis.

The master branch receives the updates from revamp when the branch is working properly. The master branch is suggested for
testing the tool.


There is an initial  Alpha 1  release which works but is not completely tested. Several bugs have been fixed since the release.

An Alpha 2 release should appear very soon.

The tool comes with the following limitations.

#### Installation in virtualenv

For working properly you should use virtualenv for installing the requirements via pip
No daemon yet

The script should be executed in a screen session to keep it running. Currently there's no respawning of the process on failure nor failure detector.

#### psycopg2 requires python and postgresql dev files

The psycopg2's pip installation requires the python development files and postgresql source code.
Please refer to your distribution for fulfilling those requirements.

#### DDL replica limitations

DDL and DML mixed in the same transaction are not decoded in the right order. This can result in a replica breakage caused by a wrong jsonb descriptor if the DML change the data on the same table modified by the DDL. I know the issue and I'm working on a solution.

### Test please!


A recording of a presentation  bout pg_chameleon is available on the Brighton PostgreSQL meetup page.

Unfortunately the audio is suboptimal.

[![IMAGE ALT TEXT](http://img.youtube.com/vi/ZZeBGDpUhec/0.jpg)](http://www.youtube.com/watch?v=ZZeBGDpUhec "pg_chameleon a lightweight replication system")
