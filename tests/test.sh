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
chameleon.py drop_replica_schema --config test


