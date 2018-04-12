#!/usr/bin/env bash
set -e 
here=`dirname $0`
chameleon.py set_configuration_files
cp ${here}/test.yml ~/.pg_chameleon/configuration/ 
chameleon.py create_replica_schema --config test
chameleon.py add_source --config test --source mysql
chameleon.py add_source --config test --source pgsql
chameleon.py init_replica --config test --source mysql --debug
chameleon.py init_replica --config test --source pgsql --debug
chameleon.py start_replica --config test --source mysql
chameleon.py show_status --config test --source mysql
chameleon.py stop_replica --config test --source mysql
chameleon.py start_replica --config test --source mysql
chameleon.py stop_all_replicas --config test
chameleon.py drop_replica_schema --config test


chameleon create_replica_schema --config test
chameleon add_source --config test --source mysql
chameleon add_source --config test --source pgsql
chameleon init_replica --config test --source mysql --debug
chameleon init_replica --config test --source pgsql --debug
chameleon start_replica --config test --source mysql
chameleon show_status --config test --source mysql
chameleon stop_replica --config test --source mysql
chameleon start_replica --config test --source mysql
chameleon stop_all_replicas --config test
chameleon drop_replica_schema --config test


