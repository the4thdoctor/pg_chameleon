from pg_chameleon import mysql_engine, pg_engine
import yaml
import sys
import os
import time
import logging
from logging.handlers  import TimedRotatingFileHandler
from tabulate import tabulate
from distutils.sysconfig import get_python_lib
from shutil import copy


class config_dir(object):
	""" 
		Class used to setup the local user configuration directory.
		The class constructor sets only the class variables for the method set_config.
		The function get_python_lib() is used to determine the python library where pg_chameleon is installed.
	"""
	def __init__(self):
		"""
			Class constructor.
		"""
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')	
		local_config = "%s/config/" % cham_dir 
		local_logs = "%s/logs/" % cham_dir 
		local_pid = "%s/pid/" % cham_dir 
		self.global_config_example = '%s/pg_chameleon/config/config-example.yaml' % python_lib
		self.local_config_example = '%s/config-example.yaml' % local_config
		self.conf_dirs=[
			cham_dir, 
			local_config, 
			local_logs, 
			local_pid, 
			
		]
		
	def set_config(self):
		""" 
			The method loops the list self.conf_dirs creating it only if missing.
			
			The method checks the freshness of the config-example.yaml file and copies the new version
			from the python library determined in the class constructor with get_python_lib().
			
			If the configuration file is missing the method copies the file with a different message.
		
		"""
		for confdir in self.conf_dirs:
			if not os.path.isdir(confdir):
				print ("creating directory %s" % confdir)
				os.mkdir(confdir)
		
		if os.path.isfile(self.local_config_example):
			if os.path.getctime(self.global_config_example)>os.path.getctime(self.local_config_example):
				print ("updating config_example %s" % self.local_config_example)
				copy(self.global_config_example, self.local_config_example)
		else:
			print ("copying config_example %s" % self.local_config_example)
			copy(self.global_config_example, self.local_config_example)
		
			
			
			

class global_config(object):
	"""
		This class parses the configuration file specified by the parameter config_name and sets 
		the class variables used by the replica_engine class. 
		The constructor checks if the configuration file is present and if is missing emits an error message followed by
		the sys.exit() command. If the configuration file is successful parsed the class variables are set from the
		configuration values. 
		
		The  function get_python_lib() is used to determine the library directory where pg_chameleon is installed in order to get the
		sql files.
		The configuration files are searched in the $HOME/.pg_chameleon/config.
		Should any parameter be missing in config the class constructor emits an error message specifying the parameter with reference to config-example.yaml.
		
		:param config_name: the configuration file to use. If omitted is set to default.
	
	"""
	def __init__(self,config_name="default", debug_mode=False):
		"""
			The class  constructor.
		"""
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')	
		config_dir = '%s/config/' % cham_dir
		sql_dir = "%s/pg_chameleon/sql/" % python_lib
		
		
		if os.path.isdir(sql_dir):
				self.sql_dir = sql_dir
		else:
			print("**FATAL - sql directory %s missing "  % self.sql_dir)
			sys.exit(1)
			
		config_file = '%s/%s.yaml' % (config_dir, config_name)
		if os.path.isfile(config_file):
			self.config_name = config_name
			self.config_dir = config_dir
		else:
			print("**FATAL - could not find the configuration file %s.yaml in %s"  % (config_name, config_dir))
			sys.exit(2)
		conffile = open(config_file, 'rb')
		confdic = yaml.load(conffile.read())
		conffile.close()
		try:
			self.source_name = confdic["source_name"]
			self.dest_schema = confdic["dest_schema"]
			self.mysql_conn = confdic["mysql_conn"]
			self.pg_conn = confdic["pg_conn"]
			self.my_database = confdic["my_database"]
			self.my_charset = confdic["my_charset"]
			self.pg_charset = confdic["pg_charset"]
			self.pg_database = confdic["pg_database"]
			self.my_server_id = confdic["my_server_id"]
			self.replica_batch_size = confdic["replica_batch_size"]
			self.tables_limit = confdic["tables_limit"]
			self.copy_mode = confdic["copy_mode"]
			self.hexify = confdic["hexify"]
			self.log_level = confdic["log_level"]
			self.log_dest = confdic["log_dest"]
			self.log_days_keep = confdic["log_days_keep"]
			self.out_dir = confdic["out_dir"]
			
			self.sleep_loop = confdic["sleep_loop"]
			self.pause_on_reindex = confdic["pause_on_reindex"]
			self.sleep_on_reindex = confdic["sleep_on_reindex"]
			self.reindex_app_names = confdic["reindex_app_names"]
			self.batch_retention = confdic["batch_retention"]
			
			self.log_file = os.path.expanduser(confdic["log_dir"])+config_name+'.log'
			self.pid_file = os.path.expanduser(confdic["pid_dir"])+"/"+config_name+".pid"
			self.exit_file = os.path.expanduser(confdic["pid_dir"])+"/"+config_name+".lock"
			copy_max_memory = str(confdic["copy_max_memory"])[:-1]
			copy_scale = str(confdic["copy_max_memory"])[-1]
			try:
				int(copy_scale)
				copy_max_memory = confdic["copy_max_memory"]
			except:
				if copy_scale =='k':
					copy_max_memory = str(int(copy_max_memory)*1024)
				elif copy_scale =='M':
					copy_max_memory = str(int(copy_max_memory)*1024*1024)
				elif copy_scale =='G':
					copy_max_memory = str(int(copy_max_memory)*1024*1024*1024)
				else:
					print("**FATAL - invalid suffix in parameter copy_max_memory  (accepted values are (k)ilobytes, (M)egabytes, (G)igabytes.")
					sys.exit(3)
			self.copy_max_memory = copy_max_memory
		except KeyError as key_missing:
			print('Missing key %s in configuration file. check %s/config-example.yaml for reference' % (key_missing, self.config_dir))
			sys.exit(4)
	
	
	def get_source_name(self, config_name = 'default'):
		"""
		The method tries to set the parameter source_name determined from the configuration file.
		The value is used to query the replica catalog in order to get the source sstatus in method list_config().
		
		:param config_name: the configuration file to use. If omitted is set to default.
		"""
		
		config_file = '%s/%s.yaml' % (self.config_dir, config_name)
		self.config_name = config_name
		if os.path.isfile(config_file):
			conffile = open(config_file, 'rb')
			confdic = yaml.load(conffile.read())
			conffile.close()
			try:
				source_name=confdic["source_name"]
			except:
				print('FATAL - missing parameter source name in config file %s' % config_file)
				source_name='NOT CONFIGURED'
		return source_name
		
class replica_engine(object):
	"""
		This class is wraps the the mysql and postgresql engines in order to perform the various activities required for the replica. 
		The constructor inits the global configuration class  and setup the mysql and postgresql engines as class objects. 
		The class sets the logging using the configuration parameter.
		
	"""
	def __init__(self, config, debug_mode=False):
		"""
			Class constructor
			:param stdout: forces the logging to stdout even if the logging destination is file
		"""
		self.debug_mode = debug_mode
		self.lst_yes= ['yes',  'Yes', 'y', 'Y']
		self.global_config=global_config(config)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s (%(lineno)s): %(message)s", "%b %e %H:%M:%S")
		
		if self.global_config.log_dest=='stdout' or self.debug_mode:
			fh=logging.StreamHandler(sys.stdout)
			
		elif self.global_config.log_dest=='file':
			fh = TimedRotatingFileHandler(self.global_config.log_file, when="d",interval=1,backupCount=self.global_config.log_days_keep)
		
		if self.global_config.log_level=='debug' or self.debug_mode:
			fh.setLevel(logging.DEBUG)
		elif self.global_config.log_level=='info':
			fh.setLevel(logging.INFO)
			
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)

		self.my_eng=mysql_engine(self.global_config, self.logger)
		self.pg_eng=pg_engine(self.global_config, self.my_eng.my_tables, self.my_eng.table_file, self.logger, self.global_config.sql_dir)
		self.sleep_loop=self.global_config.sleep_loop
		
		self.pid_file = self.global_config.pid_file
		self.exit_file = self.global_config.exit_file
	
	def detach_replica(self):
		"""
			The method terminates the replica, remove the source from the table t_sources and resets the sequences in 
			the postgresql database, leaving the replica snapshot capable to continue the activity directly on PostgreSQL
		"""
		source_name = self.global_config.source_name
		drp_msg = 'Detaching the replica will remove any reference for the source %s.\n Are you sure? YES/No\n'  % source_name
		fk_metadata = self.my_eng.get_fk_metadata()
			
		if sys.version_info[0] == 3:
			drop_src = input(drp_msg)
		else:
			drop_src = raw_input(drp_msg)
		if drop_src == 'YES':
			self.stop_replica(allow_restart=False)
			self.pg_eng.reset_sequences(self.global_config.source_name)
			self.pg_eng.add_foreign_keys(self.global_config.source_name, fk_metadata)
			self.pg_eng.drop_source(self.global_config.source_name)
		elif drop_src in  self.lst_yes:
			print('Please type YES all uppercase to confirm')
		sys.exit()
		
		print('replica detached')
	
	
	def init_replica(self):
		"""
			The method initialise a replica.
		
			It calls the pg_engine methods set_source_id which sets the source identifier and change the source status.
			
			The pg_engine method clean_batch_data is used to remove any unreplayed row in the tables t_log_replica_1(2).
			
			The class methods create_schema, copy_table_data and create_indices are called in sequence to initialise the replica.
			
		"""
		self.pg_eng.set_source_id('initialising')
		self.pg_eng.clean_batch_data()
		self.create_schema()
		self.copy_table_data()
		self.create_indices()
		self.pg_eng.set_source_id('initialised')

	def wait_for_replica_end(self):
		""" 
			The method is used to wait for the replica's end monitoring the pid file.
			The replica status is determined using the check_running method passing the write_pid=false.
			
			There is a 5 seconds sleep between each check.
			
		"""
		self.logger.info("waiting for replica process to stop")
		while True:
			replica_running=self.check_running(write_pid=False)
			if not replica_running:
				break
			time.sleep(5)
		
		self.logger.info("replica process stopped")
	

	def stop_replica(self, allow_restart=True):
		"""
			the method writes the exit file in the pid directory and waits for the replica process's end.
			If allow_restart is true the exit file is removed.
			
			:param allow_restart: determines whether the exit file is removed or not in order to allow the replica to start again.
		"""
		exit=open(self.exit_file, 'w')
		exit.close()
		self.wait_for_replica_end()
		if allow_restart:
			os.remove(self.exit_file)
	
	def enable_replica(self):
		"""
			The  method removes the exit file in order to let the replica start again.
		"""
		try:
			os.remove(self.exit_file)
			self.logger.info("Replica enabled")
		except:
			self.logger.info("Replica already enabled")
			
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
			
		"""
		self.pg_eng.upgrade_service_schema()
		
	def drop_service_schema(self):
		"""
			Drops the service schema. The action discards any replica data.

		"""
		self.logger.info("Dropping the service schema")
		self.pg_eng.drop_service_schema()
		
	def check_file_exit(self):
		process_exit=False
		"""checks for the exit file and terminate the replica if the file is present. If there is the pid file is removed before  the function's return """
		if os.path.isfile(self.exit_file):
			if os.path.isfile(self.pid_file):
				self.logger.info("exit file detected, removing the pid file and terminating the replica process")
				os.remove(self.pid_file)
			else:
				self.logger.info("you shall remove the file %s before starting the replica process " % self.exit_file)
			process_exit=True
		return process_exit
	
	
	def check_running(self, write_pid=False):
		""" 
			checks if the process is running. 
			
			:param write_pid: determines whether the pid file is written or not. Used if we just need to check if the replica is running.
		"""
		
		process_running=False 
		try:
			file_pid=open(self.pid_file,'r')
			pid=file_pid.read()
			file_pid.close()
			os.kill(int(pid),0)
			process_running=True
		except:
			if write_pid:
				pid=os.getpid()
				file_pid=open(self.pid_file,'w')
				file_pid.write(str(pid))
				file_pid.close()
			process_running=False
		return process_running
		
	def run_replica(self):
		"""
			The method starts and run the replica loop. 
			Before starting the loop checks if the replica is already running with check_running with write_pid=True.
			
			It also checks whether the exit file is present or not. 
			If present skips the replica start, otherwise start the while loop which runs the mysql_engine method run_replica.
			
			When the mysql_engine.run_replica() completes it checks if there is the exit file and eventually exit the loop.
			Otherwise sleeps for the amount or seconds set in sleep_loop.
			
		"""
		already_running = self.check_running(write_pid=True)
		exit_request = self.check_file_exit()
		
		if already_running:
			sys.exit()
		if exit_request:
			self.pg_eng.set_source_id('stopped')
			sys.exit()
		
		self.pg_eng.set_source_id('running')
		while True:
			if self.debug_mode:
				self.my_eng.run_replica(self.pg_eng)
			else:
				try:
					self.my_eng.run_replica(self.pg_eng)
				except :
					self.pg_eng.set_source_id('error')
					self.logger.error("An error occurred during the replica. %s" % (sys.exc_info(), ))
					exit=open(self.exit_file, 'w')
					exit.close()
					sys.exit(5)
			self.logger.info("batch complete. sleeping %s second(s)" % (self.sleep_loop, ))
			if self.check_file_exit():
				break
			time.sleep(self.sleep_loop)
		self.pg_eng.set_source_id('stopped')
	
	def list_config(self):
		"""
			List the available configurations stored in ~/.pg_chameleon/config/
		"""
		list_config = (os.listdir(self.global_config.config_dir))
		tab_headers = ['Config file',  'Source name',  'Status']
		tab_body = []
		
		for file in list_config:
			lst_file = file.split('.')
			file_name = lst_file[0]
			file_ext = lst_file[1]
			if file_ext == 'yaml' and file_name!='config-example':
				source_name = self.global_config.get_source_name(file_name)
				source_status = self.pg_eng.get_source_status(source_name)
				tab_row = [file_name, source_name, source_status]
				tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
	
	def show_status(self):
		"""
			list the replica status using the configuration files and the replica catalogue
		"""
		source_status=self.pg_eng.get_status()
		tab_headers = ['Config file',  'Destination schema',  'Status' ,  'Lag',  'Last received event']
		tab_body = []
			
		for status in source_status:
			source_name = status[0]
			dest_schema = status[1]
			source_status = status[2]
			lag = status[3]
			last_received_event = status[4]
			tab_row = [source_name, dest_schema, source_status, lag, last_received_event ]
			tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
		
	def add_source(self):
		"""
			register the configuration source in the replica catalogue
		"""
		source_name=self.global_config.source_name
		dest_schema=self.global_config.dest_schema
		self.pg_eng.add_source(source_name, dest_schema)

	def drop_source(self):
		"""
			remove the configuration source and all the replica informations associated with the source from the replica catalogue
		"""
		source_name = self.global_config.source_name
		drp_msg = 'Dropping the source %s will remove drop any replica reference.\n Are you sure? YES/No\n'  % source_name
		if sys.version_info[0] == 3:
			drop_src = input(drp_msg)
		else:
			drop_src = raw_input(drp_msg)
		if drop_src == 'YES':
			self.pg_eng.drop_source(self.global_config.source_name)
		elif drop_src in  self.lst_yes:
			print('Please type YES all uppercase to confirm')
		sys.exit()
		
	def copy_table_data(self, truncate_tables=False):
		"""
			Copy the data for the replicated tables from mysql to postgres.
			After the copy the master's coordinates are saved in postgres.
			
			:param truncate_tables: determines whether the existing tables should be truncated before running a copy table data
		"""
		if truncate_tables:
			self.pg_eng.truncate_tables()
		self.my_eng.copy_table_data(self.pg_eng, self.global_config.copy_max_memory)
		self.pg_eng.save_master_status(self.my_eng.master_status, cleanup=True)

	def sync_replica(self, table):
		"""
			syncronise the table data without destroying them.
			The process is very similar to the init_replica except for the fact the tables are not dropped.
			The existing indices are dropped and created after the data load in order to speed up the process.
			Is possible to restrict the sync to a limited set of tables.
			
			:param table: comma separated list of table names to synchronise
		"""
		self.stop_replica(allow_restart=False)
		self.pg_eng.table_limit=table.split(',')
		self.pg_eng.set_source_id('initialising')
		self.pg_eng.get_index_def()
		self.pg_eng.drop_src_indices()
		self.pg_eng.truncate_tables()
		self.copy_table_data()
		self.pg_eng.create_src_indices()
		self.pg_eng.set_source_id('initialised')
		self.enable_replica()
		
