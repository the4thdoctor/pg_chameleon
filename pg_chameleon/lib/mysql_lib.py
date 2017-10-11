import io
import pymysql
class mysql_source(object):
	def __init__(self):
		"""
			Class constructor, the method sets the class variables and configure the
			operating parameters from the args provided t the class.
		"""
		self.schema_tables = {}
		self.schema_mappings = {}
		self.schema_loading = {}
		self.schema_list = []
	
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
	
	def create_destination_schemas(self):
		"""
			Creates the loading schemas in the destination database and associated tables listed in the dictionary
			self.schema_tables.
			The method builds a dictionary which associates the destination schema to the loading schema. 
			The loading_schema is named after the destination schema plus with the prefix _ and the _tmp suffix.
			As postgresql allows, by default up to 64  characters for an identifier, the original schema is truncated to 59 characters,
			in order to fit the maximum identifier's length.
			The mappings are stored in the class dictionary schema_loading.
		"""
		for schema in self.schema_list:
			destination_schema = self.schema_mappings[schema]
			loading_schema = "_%s_tmp" % destination_schema[0:59]
			self.schema_loading[schema] = {'destination':destination_schema, 'loading':loading_schema}
			self.logger.debug("Creating the schema %s." % loading_schema)
			self.pg_engine.create_database_schema(loading_schema)
			self.logger.debug("Creating the schema %s." % destination_schema)
			self.pg_engine.create_database_schema(destination_schema)
			
	def drop_loading_schemas(self):
		"""
			The method drops the loading schemas from the destination database.
			The drop is performed on the schemas generated in create_destination_schemas. 
			The method assumes the class dictionary schema_loading is correctly set.
		"""
		for schema in self.schema_loading:
			loading_schema = self.schema_loading[schema]["loading"]
			self.logger.debug("Dropping the schema %s." % loading_schema)
			self.pg_engine.drop_database_schema(loading_schema, True)

	def get_table_metadata(self, table, schema):
		"""
			The method builds the table's metadata querying the information_schema.
			The data is returned as a dictionary.
			
			:param table: The table name
			:param schema: The table's schema
			:return: table's metadata as a cursor dictionary
			:rtype: dictionary
		"""
		sql_metadata="""
			SELECT 
				column_name,
				column_default,
				ordinal_position,
				data_type,
				column_type,
				character_maximum_length,
				extra,
				column_key,
				is_nullable,
				numeric_precision,
				numeric_scale,
				CASE 
					WHEN data_type="enum"
				THEN	
					SUBSTRING(COLUMN_TYPE,5)
				END AS enum_list
			FROM 
				information_schema.COLUMNS 
			WHERE 
					table_schema=%s
				AND	table_name=%s
			ORDER BY 
				ordinal_position
			;
		"""
		self.cursor_buffered.execute(sql_metadata, (schema, table))
		table_metadata=self.cursor_buffered.fetchall()
		return table_metadata


	def create_destination_tables(self):
		"""
			The method creates the destination tables in the loading schema.
			The tables names are looped using the values stored in the class dictionary schema_tables.
		"""
		for schema in self.schema_tables:
			loading_schema = self.schema_loading[schema]["loading"]
			destination_schema = self.schema_loading[schema]["destination"]
			table_list = self.schema_tables[schema]
			for table in table_list:
				table_metadata = self.get_table_metadata(table, schema)
				self.pg_engine.create_table(table_metadata, table, loading_schema)
		
	def init_replica(self):
		"""
			The method performs a full init replica for the given sources
		"""
		self.logger.debug("starting init replica for source %s" % self.source)
		self.source_config = self.sources[self.source]
		self.connect_db_buffered()
		self.pg_engine.connect_db()
		self.schema_mappings = self.pg_engine.get_schema_mappings()
		self.schema_list = [schema for schema in self.schema_mappings]
		self.build_table_exceptions()
		self.get_table_list()
		self.create_destination_schemas()
		self.create_destination_tables()
		self.drop_loading_schemas()


