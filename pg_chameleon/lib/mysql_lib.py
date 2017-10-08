import io
import pymysql
class mysql_source(object):
	def __init__(self):
		"""
			Class constructor, the method sets the class variables and configure the
			operating parameters from the args provided t the class.
		"""
		self.schema_tables = {}
	
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

	def build_table_exceptions(self):
		"""
			The method builds two dictionaries from the limit_tables and skip tables values set for the source.
			The dictionaries are intended to be used in the get_table_list to cleanup the list of tables per schema.
		"""
		self.limit_tables = {}
		self.skip_tables = {}
		limit_tables = self.source_config["limit_tables"]
		skip_tables = self.source_config["skip_tables"]
		if limit_tables:
			table_limit = [table.split('.') for table in limit_tables]
			for table_list in table_limit:
				list_exclude = []
				try:
					list_exclude = self.limit_tables[table_list[0]] 
					list_exclude.append(table_list[1])
				except KeyError:
					list_exclude.append(table_list[1])
				self.limit_tables[table_list[0]]  = list_exclude
		if skip_tables:
			table_skip = [table.split('.') for table in skip_tables]		
			for table_list in table_skip:
				list_exclude = []
				try:
					list_exclude = self.skip_tables[table_list[0]] 
					list_exclude.append(table_list[1])
				except KeyError:
					list_exclude.append(table_list[1])
				self.skip_tables[table_list[0]]  = list_exclude
				

	def get_table_list(self):
		"""
			The method pulls the table list from the information_schema. 
			The list is stored in a dictionary  which key is the table's schema.
		"""
		sql_tables="""
			SELECT 
				table_name
			FROM 
				information_schema.TABLES 
			WHERE 
					table_type='BASE TABLE' 
				AND table_schema=%s
			;
		"""
		for schema in self.schema_list:
			self.cursor_buffered.execute(sql_tables, (schema))
			table_list = [table["table_name"] for table in self.cursor_buffered.fetchall()]
			try:
				limit_tables = self.limit_tables[schema]
				if len(limit_tables) > 0:
					table_list = [table for table in table_list if table in limit_tables]
			except KeyError:
				pass
			try:
				skip_tables = self.skip_tables[schema]
				if len(skip_tables) > 0:
					table_list = [table for table in table_list if table not in skip_tables]
			except KeyError:
				pass
			
			self.schema_tables[schema] = table_list
	
	def create_loading_schemas(self):
		"""
			Creates the loading schemas in the destination database and associated tables listed in the dictionary
			self.schema_tables.
			The method builds a dictionary which associates the destination schema to the loading schema. 
			The loading_schema is named after the destination schema plus with the prefix _ and the _tmp suffix.
			As postgresql allows, by default up to 64  characters for an identifier, the original schema is truncated to 59 characters,
			in order to fit the maximum identifier's length.
			
		"""
		for schema in self.schema_list:
			loading_schema = "_%s_tmp" % schema[0:59]
			print (schema, loading_schema)
		
	def init_replica(self):
		"""
			The method performs a full init replica for the given sources
		"""
		self.logger.debug("starting init replica for source %s" % self.source)
		self.source_config = self.sources[self.source]
		self.connect_db_buffered()
		self.pg_engine.connect_db()
		self.schema_list = self.pg_engine.get_schema_list()
		self.build_table_exceptions()
		self.get_table_list()
		self.create_loading_schemas()


