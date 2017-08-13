#!/usr/bin/env bash
set -e 
here=`dirname $0`
chameleon.py set_config
cp ${here}/test.yaml ~/.pg_chameleon/config/ 
chameleon.py create_schema --config test
chameleon.py add_source --config test
chameleon.py init_replica --config test
chameleon.py start_replica --config test &
chameleon.py show_status --config test
chameleon.py stop_replica --config test
chameleon.py drop_schema --config test


