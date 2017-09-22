import yaml
import os
from shutil import copy
from distutils.sysconfig import get_python_lib


from tabulate import tabulate

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
		
		local_conn = "%s/connection/" % cham_dir 
		self.global_conn_example = '%s/pg_chameleon/connection/connection-example.yml' % python_lib
		self.local_conn_example = '%s/connection-example.yml' % local_conn
		
		local_conf = "%s/configuration/" % cham_dir 
		self.global_conf_example = '%s/pg_chameleon/configuration/config-example.yml' % python_lib
		self.local_conf_example = '%s/config-example.yml' % local_conf
		
		local_logs = "%s/logs/" % cham_dir 
		local_pid = "%s/pid/" % cham_dir 
		
		self.conf_dirs=[
			cham_dir, 
			local_conn, 
			local_conf, 
			local_logs, 
			local_pid, 
			
		]
		self.args = args
		self.set_config()
		self.args = args
	
	def set_config(self):
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
		
		if os.path.isfile(self.local_conn_example):
			if os.path.getctime(self.global_conn_example)>os.path.getctime(self.local_conn_example):
				print ("updating connection example with %s" % self.local_conn_example)
				copy(self.global_conn_example, self.local_conn_example)
		else:
			print ("copying connection  example in %s" % self.local_conn_example)
			copy(self.global_conn_example, self.local_conn_example)
		
		if os.path.isfile(self.local_conf_example):
			if os.path.getctime(self.global_conf_example)>os.path.getctime(self.local_conf_example):
				print ("updating configuration example with %s" % self.local_conf_example)
				copy(self.global_conf_example, self.local_conf_example)
		else:
			print ("copying configuration  example in %s" % self.local_conf_example)
			copy(self.global_conf_example, self.local_conf_example)
	
	def show_config(self):
		"""
			The method loads the current configuration and displays the status in tabular output
		"""
		tab_body = []
		tab_headers = ['Parameter', 'Value']
		tab_row = ['foo', 'bar']
		tab_body.append(tab_row)
		print(tabulate(tab_body, headers=tab_headers))
		
		
		
