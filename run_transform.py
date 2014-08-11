#!/usr/bin/python
from pg_chameleon import *

my_db=my_db_connection('conf/my_connection.conf')
l_args=[
        my_db.ob_engine,
        my_db.ob_conn,
        my_db.ob_metadata
        ]
my_ddl=my_data_def(l_args)
my_ddl.build_tab_list()
l_args=[
            my_ddl.l_tables,
            my_ddl.l_pkeys,
            my_ddl.l_indices
        ]

pg_ddl=pg_data_def(l_args)

l_args=[
        '/tmp/db_schema.sql',
        True
        ]

pg_ddl.save_ddl(l_args)   

l_args=[
        'conf/pg_connection.conf',
        True,
        'tables'
        ]

pg_ddl.create_objects(l_args)

l_args=[
        my_db.ob_engine,
        my_db.ob_conn,
        my_db.ob_metadata,
        my_ddl.l_tables,
        '/tmp/'
        ]

my_flow=my_data_flow(l_args)
my_flow.pull_data()


l_args=[
        'conf/pg_connection.conf',
        my_flow.l_tab_file
        ]
pg_flow=pg_data_flow(l_args)
pg_flow.push_data()

l_args=[
        'conf/pg_connection.conf',
        False,
        'pkeys'
        ]

pg_ddl.create_objects(l_args)

l_args=[
        'conf/pg_connection.conf',
        False,
        'idx'
        ]

pg_ddl.create_objects(l_args)