import yaml
import os
import sys
from shutil import copy
from distutils.sysconfig import get_python_lib
from tabulate import tabulate
from pg_chameleon import pg_engine
import logging
from logging.handlers  import TimedRotatingFileHandler



class replica_engine(object):
	"""
		This class is wraps the the mysql and postgresql engines in order to perform the various activities required for the replica. 
		The constructor inits the global configuration class  and setup the mysql and postgresql engines as class objects. 
		The class sets the logging using the configuration parameter.
		
	"""
	def __init__(self, args):
		"""
			Class constructor.
		"""
		python_lib=get_python_lib()
		cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')	
		
		
		local_conf = "%s/configuration/" % cham_dir 
		self.global_conf_example = '%s/pg_chameleon/configuration/config-example.yml' % python_lib
		self.local_conf_example = '%s/config-example.yml' % local_conf
		
		local_logs = "%s/logs/" % cham_dir 
		local_pid = "%s/pid/" % cham_dir 
		
		self.conf_dirs=[
			cham_dir, 
			local_conf, 
			local_logs, 
			local_pid, 
			
		]
		self.args = args
		self.set_configuration_files()
		self.args = args
		self.load_config()
		self.pg_engine = pg_engine()
		self.pg_engine.dest_conn = self.config["pg_conn"]
		self.logger = self.init_logger()
		self.pg_engine.logger = self.logger
		
	def set_configuration_files(self):
		""" 
			The method loops the list self.conf_dirs creating them only if they are missing.
			
			The method checks the freshness of the config-example.yaml and connection-example.yml 
			copies the new version from the python library determined in the class constructor with get_python_lib().
			
			If the configuration file is missing the method copies the file with a different message.
		
		"""

		for confdir in self.conf_dirs:
			if not os.path.isdir(confdir):
				print ("creating directory %s" % confdir)
				os.mkdir(confdir)
		
				
		if os.path.isfile(self.local_conf_example):
			if os.path.getctime(self.global_conf_example)>os.path.getctime(self.local_conf_example):
				print ("updating configuration example with %s" % self.local_conf_example)
				copy(self.global_conf_example, self.local_conf_example)
		else:
			print ("copying configuration  example in %s" % self.local_conf_example)
			copy(self.global_conf_example, self.local_conf_example)
	
	def load_config(self):
		""" 
			The method loads the configuration from the file specified in the args.config parameter.
		"""
		local_confdir = "%s/.pg_chameleon/configuration/" % os.path.expanduser('~')	
		self.config_file = '%s/%s.yml'%(local_confdir, self.args.config)
		
		if not os.path.isfile(self.config_file):
			print("**FATAL - configuration file missing. Please ensure the file %s is present." % (self.config_file))
			sys.exit()
		
		config_file = open(self.config_file, 'r')
		self.config = yaml.load(config_file.read())
		config_file.close()
		
	
	def show_sources(self):
		"""
			The method shows the sources available in the configuration file.
		"""
		for item in self.config["sources"]:
			print("\n")
			print (tabulate([], headers=["Source %s" % item]))
			tab_headers = ['Parameter', 'Value']
			tab_body = []
			source = self.config["sources"][item]
			config_list = [param for param in source if param not in ['db_conn']]
			connection_list = [param for param  in source["db_conn"] if param not in ['password']]
			for parameter in config_list:
				tab_row = [parameter, source[parameter]]
				tab_body.append(tab_row)
			for param in connection_list:
				tab_row = [param, source["db_conn"][param]]
				tab_body.append(tab_row)
		
			print(tabulate(tab_body, headers=tab_headers))
		
	def show_config(self):
		"""
			The method loads the current configuration and displays the status in tabular output
		"""
		config_list = [item for item in self.config if item not in ['pg_conn', 'sources']]
		connection_list = [item for item in self.config["pg_conn"] if item not in ['password']]
		tab_body = []
		tab_headers = ['Parameter', 'Value']
		for item in config_list:
			tab_row = [item, self.config[item]]
			tab_body.append(tab_row)
		for item in connection_list:
			tab_row = [item, self.config["pg_conn"][item]]
			tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
		self.show_sources()
		
	def create_replica_schema(self):
		"""
			The method creates the replica schema in the destination database.
		"""
		self.logger.info("Trying to create replica schema")
		self.pg_engine.create_replica_schema()
		
		
		
	def init_logger(self):
		log_dir = self.config["log_dir"] 
		log_level = self.config["log_level"] 
		log_dest = self.config["log_dest"] 
		log_days_keep = self.config["log_days_keep"] 
		log_name = self.args.config
		debug_mode = self.args.debug

		log_file = os.path.expanduser('%s/%s.log' % (log_dir,log_name))
		logger = logging.getLogger(__name__)
		logger.setLevel(logging.DEBUG)
		logger.propagate = False
		formatter = logging.Formatter("%(asctime)s: [%(levelname)s] - %(filename)s (%(lineno)s): %(message)s", "%b %e %H:%M:%S")
		
		if log_dest=='stdout' or debug_mode:
			fh=logging.StreamHandler(sys.stdout)
			
		elif log_dest=='file':
			fh = TimedRotatingFileHandler(log_file, when="d",interval=1,backupCount=log_days_keep)
		
		if log_level=='debug' or debug_mode:
			fh.setLevel(logging.DEBUG)
		elif log_level=='info':
			fh.setLevel(logging.INFO)
			
		fh.setFormatter(formatter)
		logger.addHandler(fh)
		return logger
