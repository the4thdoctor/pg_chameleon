from pg_chameleon import mysql_engine, pg_engine
import yaml
import sys
import os
import time
import logging
from datetime import datetime
class global_config:
	"""class to manage the mysql connection"""
	def __init__(self,command, config_file='config/config.yaml'):
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
		self.pg_charset=confdic["pg_charset"]
		self.pg_database=confdic["pg_database"]
		self.my_server_id=confdic["my_server_id"]
		self.replica_batch_size=confdic["replica_batch_size"]
		self.tables_limit=confdic["tables_limit"]
		self.copy_max_size=confdic["copy_max_size"]
		self.copy_mode=confdic["copy_mode"]
		dt=datetime.now()
		log_sfx=dt.strftime('%Y%m%d-%H%M%S')
		self.log_file=confdic["log_dir"]+"/"+command+"_"+log_sfx+'.log'
		
		
class replica_engine:
	def __init__(self, command):
		self.global_config=global_config(command)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		fh = logging.FileHandler(self.global_config.log_file, "w")
		fh.setLevel(logging.DEBUG)
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s: %(message)s", "%b %e %H:%M:%S")
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)
		self.my_eng=mysql_engine(self.global_config, self.logger)
		self.pg_eng=pg_engine(self.global_config, self.my_eng.my_tables, self.my_eng.table_file, self.logger)
		
	def  create_tables(self, drop_tables=False):
		self.pg_eng.create_schema()
		self.logger.info("Importing mysql schema")
		self.pg_eng.build_tab_ddl()
		self.pg_eng.create_tables(drop_tables)
	
	def  create_indices(self, drop_tables=False):
		self.pg_eng.build_idx_ddl()
		self.pg_eng.create_indices()
	
	def create_service_schema(self, cleanup=False):
		self.logger.info("Creating service schema")
		self.pg_eng.create_service_schema(cleanup)
	
	def do_stream_data(self):
		while True:
			self.my_eng.do_stream_data(self.pg_eng)
			self.logger.info("stream complete. replaying  batch data")
			self.pg_eng.process_batch()
			self.logger.info("sleeping 10 seconds")
			time.sleep(10)
			
	def copy_table_data(self):
		self.my_eng.copy_table_data(self.pg_eng, limit=self.global_config.copy_max_size)
		self.pg_eng.save_master_status(self.my_eng.master_status)
