#!/usr/bin/env python
import time
import psycopg2
import json
from psycopg2.extras import LogicalReplicationConnection
from psycopg2.extras import  REPLICATION_LOGICAL
strconn = "dbname=postgres user=usr_replica host=localhost password=bar port=5432 "  
log_conn = psycopg2.connect(strconn, connection_factory=LogicalReplicationConnection)
log_cur = log_conn.cursor()
rep_options = {'include-lsn':1}
#log_cur.create_replication_slot("logical1", slot_type=REPLICATION_LOGICAL, output_plugin="test_decoding")
log_cur.start_replication(slot_name="test2", slot_type=REPLICATION_LOGICAL, start_lsn=0, timeline=0, options=rep_options, decode=True)
while True:
	msg = log_cur.read_message()
	if msg:
		payload=json.loads(msg.payload)
		change = payload["change"]
		nextlsn =  payload["nextlsn"]
		print(msg.data_start, nextlsn)
		log_cur.send_feedback(flush_lsn=msg.data_start)
	
	time.sleep(1)

log_conn.close()
