from pg_chameleon import mysql_engine, pg_engine
import yaml
import sys
import os
import time
class global_config:
	"""class to manage the mysql connection"""
	def __init__(self,config_file='config/config.yaml'):
		if not os.path.isfile(config_file):
			print "**FATAL - configuration file missing **\ncopy config/config-example.yaml to config/config.yaml and set your connection settings."
			sys.exit()
		conffile=open(config_file, 'rb')
		confdic=yaml.load(conffile.read())
		conffile.close()
		self.mysql_conn=confdic["mysql_conn"]
		self.pg_conn=confdic["pg_conn"]
		self.my_database=confdic["my_database"]
		self.my_charset=confdic["my_charset"]
		self.pg_database=confdic["pg_database"]
		self.my_server_id=confdic["my_server_id"]
		self.replica_batch_size=confdic["replica_batch_size"]
		self.tables_limit=confdic["tables_limit"]
		
class replica_engine:
	def __init__(self):
		self.my_eng=mysql_engine(global_config)
		self.pg_eng=pg_engine(global_config, self.my_eng.my_tables, self.my_eng.table_file)
		self.pg_eng.create_schema()
		
	def pull_data(self, table_limit):
		self.my_eng.pull_table_data(limit=table_limit)
		self.pg_eng.save_master_status(self.my_eng.master_status)
		
	
	def push_data(self):
		print "loading data"
		self.pg_eng.push_data(self.my_eng.table_file, self.my_eng.my_tables)
		
	def  create_tables(self, drop_tables=False):
		self.pg_eng.build_tab_ddl()
		self.pg_eng.create_tables(drop_tables)
	
	def  create_indices(self, drop_tables=False):
		self.pg_eng.build_idx_ddl()
		self.pg_eng.create_indices()
	
	def create_service_schema(self, cleanup=False):
		self.pg_eng.create_service_schema(cleanup)

	def do_stream_data(self):
		while True:
			self.my_eng.do_stream_data(self.pg_eng)
			print "stream empty sleeping 10 seconds"
			time.sleep(10)
