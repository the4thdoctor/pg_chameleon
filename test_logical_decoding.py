#!/usr/bin/env python
import time
import psycopg2
import json
from psycopg2.extras import LogicalReplicationConnection
from psycopg2.extras import  REPLICATION_LOGICAL
strconn = "dbname=postgres user=usr_replica host=localhost password=bar port=5432 "  
log_conn = psycopg2.connect(strconn, connection_factory=LogicalReplicationConnection)
log_cur = log_conn.cursor()
#rep_options = {'include-xids':1}
rep_options = None
#log_cur.create_replication_slot("logical1", slot_type=REPLICATION_LOGICAL, output_plugin="test_decoding")
log_cur.start_replication(slot_name="test2", slot_type=REPLICATION_LOGICAL, start_lsn=0, timeline=0, options=rep_options, decode=True)

while True:
	msg = True
	flush_lsn = None
	while msg:
		msg = log_cur.read_message()
		if msg:
			print(msg.data_start, msg.payload)
			flush_lsn=msg.data_start
	if flush_lsn:
		log_cur.send_feedback(flush_lsn=flush_lsn)
	
	time.sleep(1)

log_conn.close()
