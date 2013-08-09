from pg_chameleon import *
my_db=my_db_connection('config/my_connection.conf')
l_args=[
        my_db.ob_engine,
        my_db.ob_conn,
        my_db.ob_metadata
        ]
my_ddl=my_data_def(l_args)
my_ddl.build_tab_list()