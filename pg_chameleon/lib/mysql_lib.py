import io
import pymysql
class mysql_source(object):
	def __init__(self):
		"""
			Class constructor, the method sets the class variables and configure the
			operating parameters from the args provided to the class.
		"""
		self.my_tables = []
		
	
	def disconnect_db(self):
		"""
			The method disconnects any active connection to the mysql database.
		"""
	
	def __del__(self):
		"""
			Class destructor, tries to disconnect the postgresql connection.
		"""
		self.disconnect_db()
