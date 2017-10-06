import io
import pymysql
class mysql_source(object):
	def __init__(self):
		"""
			Class constructor, the method sets the class variables and configure the
			operating parameters from the args provided to the class.
		"""
		self.my_tables = []
		
	def connect_db_buffered(self):
		"""
			The method creates a new connection to the mysql database.
			The connection is made using the dictionary type cursor factory, which is buffered.
		"""
	
	def disconnect_db_buffered(self):
		"""
			The method disconnects any connection  with dictionary type cursor from the mysql database.
			
		"""
	
	def connect_db_unbuffered(self):
		"""
			The method creates a new connection to the mysql database.
			The connection is made using the unbuffered cursor factory.
		"""
	
	def disconnect_db_unbuffered(self):
		"""
			The method disconnects any unbuffered connection from the mysql database.
		"""
	
	def __del__(self):
		"""
			Class destructor, tries to disconnect the postgresql connection.
		"""
		self.disconnect_db_unbuffered()
		self.disconnect_db_buffered()
		
		
	def init_replica(self):
		"""
			The method performs a full init replica for the given sources
		"""
		self.logger.debug("starting init replica for source %s" % self.source)
		self.source_config = self.sources[self.source]
		
