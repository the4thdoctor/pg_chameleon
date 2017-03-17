from pg_chameleon import mysql_engine, pg_engine
import yaml
import sys
import os
import time
import logging
import smtplib
from distutils.sysconfig import get_python_lib
from shutil import copy


class config_dir(object):
	def __init__(self):
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')	
		local_config = "%s/config/" % cham_dir 
		local_logs = "%s/logs/" % cham_dir 
		local_pid = "%s/pid/" % cham_dir 
		self.global_config_example = '%s/pg_chameleon/config/config-example.yaml' % python_lib
		self.local_config_example = '%s/config-example.yaml' % local_config
		#global_sql = '%s/sql' % python_lib
		self.conf_dirs=[
			cham_dir, 
			local_config, 
			local_logs, 
			local_pid, 
			
		]
		
	def set_config(self):
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
		This class parses the configuration file which is in config/config.yaml and sets 
		the class variables used by the other libraries. 
		The constructor checks if the configuration file is present and if is missing emits an error message followed by
		the sys.exit() command. If the configuration file is successful parsed the class variables are set from the
		configuration values.
		The class sets the log output file from the parameter command.  If the log destination is stdout then the logfile is ignored
		
		:param command: the command specified on the pg_chameleon.py command line
	
	"""
	def __init__(self,config_name="default"):
		"""
			Class  constructor.
		"""
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')	
		config_dir = '%s/config/' % cham_dir
		sql_dir = "%s/pg_chameleon/sql/" % python_lib
		
		
		config_missing = True
		
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
			self.log_append = confdic["log_append"]
			
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
					sys.exit()
			self.copy_max_memory = copy_max_memory
		except KeyError as key_missing:
			print('Missing key %s in configuration file. check %s/config-example.yaml for reference' % (key_missing, self.config_dir))
			sys.exit()

	def get_source_name(self, config_name = 'default'):
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
		This class is a bridge between the mysql and postgresql engines. The constructor inits the global configuration
		class  and setup the mysql and postgresql engines as class objects. 
		The class setup the logging using the configuration parameter (e.g. log level debug on stdout).
		
		:param command: the command specified on the pg_chameleon.py command line
		
		
		
	"""
	def __init__(self, config, stdout=False):
		self.global_config=global_config(config)
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.logger.propagate = False
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s (%(lineno)s): %(message)s", "%b %e %H:%M:%S")
		
		if self.global_config.log_dest=='stdout' or stdout:
			fh=logging.StreamHandler(sys.stdout)
			
		elif self.global_config.log_dest=='file':
			if self.global_config.log_append:
				file_mode='a'
			else:
				file_mode='w'
			fh = logging.FileHandler(self.global_config.log_file, file_mode)
		
		if self.global_config.log_level=='debug':
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
	
	def init_replica(self):
		self.pg_eng.set_source_id('initialising')
		self.pg_eng.clean_batch_data()
		self.create_schema()
		self.copy_table_data()
		self.create_indices()
		self.pg_eng.set_source_id('initialised')

	def wait_for_replica_end(self):
		""" waiting for replica end"""
		self.logger.info("waiting for replica process to stop")
		while True:
			replica_running=self.check_running(write_pid=False)
			if not replica_running:
				break
			time.sleep(5)
		
		self.logger.info("replica process stopped")
	

	def stop_replica(self, allow_restart=True):
		exit=open(self.exit_file, 'w')
		exit.close()
		self.wait_for_replica_end()
		if allow_restart:
			os.remove(self.exit_file)
	
	def enable_replica(self):
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
			Drops the service schema. The action discards any information relative to the replica.

		"""
		self.logger.info("Dropping the service schema")
		self.pg_eng.drop_service_schema()
		
	def check_file_exit(self):
		process_exit=False
		"""checks for the exit file and terminate the replica if the file is present """
		if os.path.isfile(self.exit_file):
			if os.path.isfile(self.pid_file):
				self.logger.info("exit file detected, removing the pid file and terminating the replica process")
				os.remove(self.pid_file)
			else:
				self.logger.info("you shall remove the file %s before starting the replica process " % self.exit_file)
			process_exit=True
		return process_exit
	
	
	def check_running(self, write_pid=False):
		""" checks if the process is running. saves the pid file if not """
		
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
			Runs the replica loop. 
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
			self.my_eng.run_replica(self.pg_eng)
			self.logger.info("batch complete. sleeping %s second(s)" % (self.sleep_loop, ))
			if self.check_file_exit():
				break
			time.sleep(self.sleep_loop)
		self.pg_eng.set_source_id('stopped')
	
	def list_config(self):
		list_config = (os.listdir(self.global_config.config_dir))
		print ("Available configurations")
		print ("Config file\t\t\tName\t\tStatus\t\t" )
		print ("==================================================================" )
		for file in list_config:
			lst_file = file.split('.')
			file_name = lst_file[0]
			file_ext = lst_file[1]
			if file_ext == 'yaml' and file_name!='config-example':
				source_name = self.global_config.get_source_name(file_name)
				source_status = self.pg_eng.get_source_status(source_name)
				
				
				print ("%s.yaml\t\t\t%s\t\t%s\t\t" % (file_name, source_name, source_status ))
	
	def show_status(self):
		source_status=self.pg_eng.get_status()
		print ("Config file\t\tDest schema\t\tStatus\t\tLag\t\tLast event" )
		print ("=============================================================================================================" )
			
		for status in source_status:
			source_name = status[0]
			dest_schema = status[1]
			source_status = status[2]
			seconds_behind_master = status[3]
			last_received_event = status[4]
			print ("%s.yaml\t\t%s\t\t%s\t\t%s\t\t%s" % (source_name, dest_schema, source_status, seconds_behind_master, last_received_event ))
	def add_source(self):
		source_name=self.global_config.source_name
		dest_schema=self.global_config.dest_schema
		self.pg_eng.add_source(source_name, dest_schema)

	def drop_source(self):
		lst_yes= ['yes',  'Yes', 'y', 'Y']
		source_name = self.global_config.source_name
		drp_msg = 'Dropping the source %s will remove drop any replica reference.\n Are you sure? YES/No\n'  % source_name
		if sys.version_info[0] == 3:
			drop_src = input(drp_msg)
		else:
			drop_src = raw_input(drp_msg)
		if drop_src == 'YES':
			self.pg_eng.drop_source(self.global_config.source_name)
		elif drop_src in  lst_yes:
			print('Please type YES all uppercase to confirm')
		sys.exit()
		
	def copy_table_data(self, truncate_tables=False):
		"""
			Copy the data for the replicated tables from mysql to postgres.
			
			After the copy the master's coordinates are saved in postgres.
		"""
		if truncate_tables:
			self.pg_eng.truncate_tables()
		self.my_eng.copy_table_data(self.pg_eng, self.global_config.copy_max_memory)
		self.pg_eng.save_master_status(self.my_eng.master_status, cleanup=True)

	def sync_replica(self, table):
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
		
class email_lib(object):
	"""
		class to manage email alerts sent in specific events.
	"""
	def __init__(self, config, logger):
		self.config=config
		self.smtp_server=None
		self.logger=logger
	
	def connect_smtp(self):
		self.logger.info("establishing connection with to SMTP server")
		try:
			self.smtp_server = smtplib.SMTP(self.config["smtp_host"], self.config["smtp_port"])
			if self.config["smtp_tls"]:
				self.smtp_server.starttls()
			if self.config["smtp_login"]:
				self.smtp_server.login(self.config["smtp_username"], self.config["smtp_password"])
			
		except:
			self.logger.error("could not connect to the SMTP server")
			self.smtp_server=None
	
		
	def disconnect_smtp(self):
		if self.smtp_server:
			self.logger.info("disconnecting from SMTP server")
			self.smtp_server.quit()
	
	def send_restarted_replica(self):
		"""
			sends the email when restarting the replica process
		"""
		self.connect_smtp()
		self.disconnect_smtp()
	
