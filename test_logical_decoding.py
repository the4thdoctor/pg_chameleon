#!/usr/bin/env python
import time
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from psycopg2.extras import  REPLICATION_LOGICAL
strconn = "dbname=db_replica user=usr_replica host=foo password=bar port=5432"  
log_conn = psycopg2.connect(strconn, connection_factory=LogicalReplicationConnection)
log_cur = log_conn.cursor()
log_cur.create_replication_slot("logical1", slot_type=REPLICATION_LOGICAL, output_plugin="test_decoding")
#log_cur.start_replication(slot_name="logical1", slot_type=REPLICATION_LOGICAL, start_lsn=0, timeline=0, options=None, decode=True)
"""while True:
	log_cur.send_feedback()
	msg = log_cur.read_message()
	if msg:
		print(msg.payload)
	time.sleep(1)
"""
log_cur.drop_replication_slot("logical1")
log_conn.close()
