import io
import pymysql
class mysql_source(object):
	def __init__(self):
		"""
			Class constructor, the method sets the class variables and configure the
			operating parameters from the args provided to the class.
		"""
		self.my_tables = []
	
	def __del__(self):
		"""
			Class destructor, tries to disconnect the mysql connection.
		"""
		self.disconnect_db_unbuffered()
		self.disconnect_db_buffered()
	
	def connect_db_buffered(self):
		"""
			The method creates a new connection to the mysql database.
			The connection is made using the dictionary type cursor factory, which is buffered.
		"""
		db_conn = self.source_config["db_conn"]
		self.conn_buffered=pymysql.connect(
			host = db_conn["host"],
			user = db_conn["user"],
			password = db_conn["password"],
			charset = db_conn["charset"],
			cursorclass=pymysql.cursors.DictCursor
		)
		self.cursor_buffered = self.conn_buffered.cursor()
		self.cursor_buffered_fallback = self.conn_buffered.cursor()
	
	def disconnect_db_buffered(self):
		"""
			The method disconnects any connection  with dictionary type cursor from the mysql database.
			
		"""
		try:
			self.logger.debug("Trying to disconnect the buffered connection from the destination database.")
			self.conn_buffered.close()
		except:
			self.logger.debug("There is no database connection to disconnect.")

	
	def connect_db_unbuffered(self):
		"""
			The method creates a new connection to the mysql database.
			The connection is made using the unbuffered cursor factory.
		"""
		db_conn = self.source_config["db_conn"]
		self.conn_buffered=pymysql.connect(
			host = db_conn["host"],
			user = db_conn["user"],
			password = db_conn["password"],
			charset = db_conn["charset"],
			cursorclass=pymysql.cursors.SSCursor
		)
		self.cursor_unbuffered = self.conn_buffered.cursor()
		
		
	def disconnect_db_unbuffered(self):
		"""
			The method disconnects any unbuffered connection from the mysql database.
		"""
		try:
			self.logger.debug("Trying to disconnect the unbuffered connection from the destination database.")
			self.conn_unbuffered.close()
		except:
			self.logger.debug("There is no database connection to disconnect.")


		
		
	def init_replica(self):
		"""
			The method performs a full init replica for the given sources
		"""
		self.logger.debug("starting init replica for source %s" % self.source)
		self.source_config = self.sources[self.source]
		self.connect_db_buffered()
		self.connect_db_unbuffered()




