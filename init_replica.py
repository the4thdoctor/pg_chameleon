#!/usr/bin/env python
from pg_chameleon import replica_engine
replica=replica_engine()
replica.create_service_schema(cleanup=True)
replica.create_tables(drop_tables=True)
replica.copy_table_data(table_limit=100000)
replica.create_indices()
replica.do_stream_data()
