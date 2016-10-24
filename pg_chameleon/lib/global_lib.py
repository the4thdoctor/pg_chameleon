from pg_chameleon import mysql_engine, pg_engine
import yaml
import sys
import os
import time
import logging
from datetime import datetime
class global_config:
	"""
		This class parses the configuration file which is in config/config.yaml and sets 
		the class variables used by the other libraries. 
		The constructor checks if the configuration file is present and if is missing emits an error message followed by
		the sys.exit() command. If the configuration file is successful parsed the class variables are set from the
		configuration values.
		The class sets the log output file from the parameter command.  If the log destination is stdout then the logfile is ignored
		
		:param command: the command specified on the pg_chameleon.py command line
	
	"""
	def __init__(self,command):
		"""
			Class  constructor.
		"""
		config_file='config/config.yaml'
		if not os.path.isfile(config_file):
			print "**FATAL - configuration file missing **\ncopy config/config-example.yaml to "+config_file+" and set your connection settings."
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
		self.hexify=confdic["hexify"]
		self.log_level=confdic["log_level"]
		self.log_dest=confdic["log_dest"]
		dt=datetime.now()
		log_sfx=dt.strftime('%Y%m%d-%H%M%S')
		self.log_file=confdic["log_dir"]+"/"+command+"_"+log_sfx+'.log'
		
		
class replica_engine:
	"""
		This class acts as bridge between the mysql and postgresql engines. The constructor inits the global configuration
		class  and setup the mysql and postgresql engines as class objects. 
		The class setup the logging using the configuration parameter (e.g. log level debug on stdout).
		
		:param command: the command specified on the pg_chameleon.py command line
		
		
		
	"""
	def __init__(self, command):
		self.global_config=global_config(command)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s: %(message)s", "%b %e %H:%M:%S")
		
		if self.global_config.log_dest=='stdout':
			fh=logging.StreamHandler(sys.stdout)
			
		elif self.global_config.log_dest=='file':
			fh = logging.FileHandler(self.global_config.log_file, "w")
		
		if self.global_config.log_level=='debug':
			fh.setLevel(logging.DEBUG)
		elif self.global_config.log_level=='info':
			fh.setLevel(logging.INFO)
			
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)

		self.my_eng=mysql_engine(self.global_config, self.logger)
		self.pg_eng=pg_engine(self.global_config, self.my_eng.my_tables, self.my_eng.table_file, self.logger)
		
	def  create_schema(self):
		"""
			Creates the database schema on PostgreSQL using the metadata extracted from MySQL.
		"""
		self.pg_eng.create_schema()
		self.logger.info("Importing mysql schema")
		self.pg_eng.build_tab_ddl()
		self.pg_eng.create_tables()
	

	
	def  create_indices(self):
		"""
			Creates the indices on the PostgreSQL schema using the metadata extracted from MySQL.
			
		"""
		self.pg_eng.build_idx_ddl()
		self.pg_eng.create_indices()
	
	def create_service_schema(self):
		"""
			Creates the service schema sch_chameleon on the PostgreSQL database. The service schema is required for having the replica working correctly.
	
		"""
		self.pg_eng.create_service_schema()
		
	def upgrade_service_schema(self):
		"""
			Upgrade the service schema to the latest version.
			
			:todo: everything!
		"""
		self.pg_eng.upgrade_service_schema()
		
	def drop_service_schema(self):
		"""
			Drops the service schema. The action discards any information relative to the replica.

		"""
		self.logger.info("Dropping the service schema")
		self.pg_eng.drop_service_schema()
	
	def run_replica(self):
		"""
			Runs the replica loop. 
		"""
		while True:
			self.my_eng.run_replica(self.pg_eng)
			self.logger.info("batch complete. sleeping 1 second")
			time.sleep(1)
		

			
	def copy_table_data(self):
		"""
			Copy the data for the replicated tables from mysql to postgres.
			
			After the copy the master's coordinates are saved in postgres.
		"""
		self.my_eng.copy_table_data(self.pg_eng, limit=self.global_config.copy_max_size)
		self.pg_eng.save_master_status(self.my_eng.master_status)
