import time
import sys
import io
import pymysql
import codecs
import binascii
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent
from pymysqlreplication.event import RotateEvent
from pg_chameleon import sql_token
from os import remove

class mysql_source(object):
	def __init__(self):
		"""
			Class constructor, the method sets the class variables and configure the
			operating parameters from the args provided t the class.
		"""
		self.statement_skip = ['BEGIN', 'COMMIT']
		self.schema_tables = {}
		self.schema_mappings = {}
		self.schema_loading = {}
		self.schema_list = []
		self.hexify_always = ['blob', 'tinyblob', 'mediumblob','longblob','binary','varbinary','geometry']
		self.schema_only = {}
	
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
		db_conn = {key:str(value) for key, value in db_conn.items()}
		db_conn["port"] = int(db_conn["port"])
		db_conn["connect_timeout"] = int(db_conn["connect_timeout"])
		
		self.conn_buffered=pymysql.connect(
			host = db_conn["host"],
			user = db_conn["user"],
			port = db_conn["port"],
			password = db_conn["password"],
			charset = db_conn["charset"],
			connect_timeout = db_conn["connect_timeout"], 
			cursorclass=pymysql.cursors.DictCursor
		)
		self.charset = db_conn["charset"]
		self.cursor_buffered = self.conn_buffered.cursor()
		self.cursor_buffered_fallback = self.conn_buffered.cursor()
	
	def disconnect_db_buffered(self):
		"""
			The method disconnects any connection  with dictionary type cursor from the mysql database.
			
		"""
		try:
			self.conn_buffered.close()
		except:
			pass
	
	def connect_db_unbuffered(self):
		"""
			The method creates a new connection to the mysql database.
			The connection is made using the unbuffered cursor factory.
		"""
		db_conn = self.source_config["db_conn"]
		db_conn = {key:str(value) for key, value in db_conn.items()}
		db_conn["port"] = int(db_conn["port"])
		db_conn["connect_timeout"] = int(db_conn["connect_timeout"])
		self.conn_unbuffered=pymysql.connect(
			host = db_conn["host"],
			user = db_conn["user"],
			port = db_conn["port"],
			password = db_conn["password"],
			charset = db_conn["charset"],
			connect_timeout = db_conn["connect_timeout"], 
			cursorclass=pymysql.cursors.SSCursor
		)
		self.charset = db_conn["charset"]
		self.cursor_unbuffered = self.conn_unbuffered.cursor()
		
		
	def disconnect_db_unbuffered(self):
		"""
			The method disconnects any unbuffered connection from the mysql database.
		"""
		try:
			self.conn_unbuffered.close()
		except:
			pass

	def build_table_exceptions(self):
		"""
			The method builds two dictionaries from the limit_tables and skip tables values set for the source.
			The dictionaries are intended to be used in the get_table_list to cleanup the list of tables per schema.
			The method manages the particular case of when the class variable self.tables is set.
			In that case only the specified tables in self.tables will be synced. Should limit_tables be already 
			set, then the resulting list is the intersection of self.tables and limit_tables.
		"""
		self.limit_tables = {}
		self.skip_tables = {}
		limit_tables = self.source_config["limit_tables"]
		skip_tables = self.source_config["skip_tables"]
		
		if self.tables !='*':
			tables = [table.strip() for table in self.tables.split(',')]
			if limit_tables:
				limit_tables = [table for table in tables if table in limit_tables]
			else:
				limit_tables = tables
			self.schema_only = {table.split('.')[0] for table in limit_tables}
			
		
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

	def get_foreign_keys_metadata(self):
		"""
			The method collects the foreign key metadata for the detach replica process.
			Currently doesn't get the ON UPDATE/ON DELETE triggers
		"""
		self.init_sync()
		schema_replica = "'%s'"  % "','".join([schema.strip() for schema in self.sources[self.source]["schema_mappings"]])
		self.logger.info("retrieving foreign keys metadata for schemas %s" % schema_replica)
		sql_fkeys = """ 
			SELECT
				table_name,
				table_schema,
				constraint_name,
				referenced_table_name,
				referenced_table_schema,
				GROUP_CONCAT(concat('"',column_name,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as fk_cols,
				GROUP_CONCAT(concat('"',REFERENCED_COLUMN_NAME,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as ref_columns
			FROM
				information_schema.key_column_usage
			WHERE
					table_schema in (%s)
				AND referenced_table_name IS NOT NULL
				AND referenced_table_schema in (%s)
			GROUP BY
				table_name,
				constraint_name,
				referenced_table_name
			ORDER BY
				table_name
			;

		""" % (schema_replica, schema_replica)
		self.cursor_buffered.execute(sql_fkeys)
		fkey_list=self.cursor_buffered.fetchall()
		self.disconnect_db_buffered()
		return fkey_list

	def create_destination_tables(self):
		"""
			The method creates the destination tables in the loading schema.
			The tables names are looped using the values stored in the class dictionary schema_tables.
		"""
		for schema in self.schema_tables:
			table_list = self.schema_tables[schema]
			for table in table_list:
				table_metadata = self.get_table_metadata(table, schema)
				self.pg_engine.create_table(table_metadata, table, schema)
	
	
	def generate_select_statements(self, schema, table):
		"""
			The generates the csv output and the statements output for the given schema and table.
			The method assumes there is a buffered database connection active.
			
			:param schema: the origin's schema
			:param table: the table name 
			:return: the select list statements for the copy to csv and  the fallback to inserts.
			:rtype: dictionary
		"""
		select_columns = {}
		sql_select="""
			SELECT 
				CASE
					WHEN 
						data_type IN ('"""+"','".join(self.hexify)+"""')
					THEN
						concat('hex(',column_name,')')
					WHEN 
						data_type IN ('bit')
					THEN
						concat('cast(`',column_name,'` AS unsigned)')
					WHEN 
						data_type IN ('datetime','timestamp','date')
					THEN
						concat('nullif(`',column_name,'`,"0000-00-00 00:00:00")')

				ELSE
					concat('cast(`',column_name,'` AS char CHARACTER SET """+ self.charset +""")')
				END
				AS select_csv,
				CASE
					WHEN 
						data_type IN ('"""+"','".join(self.hexify)+"""')
					THEN
						concat('hex(',column_name,') AS','`',column_name,'`')
					WHEN 
						data_type IN ('bit')
					THEN
						concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
					WHEN 
						data_type IN ('datetime','timestamp','date')
					THEN
						concat('nullif(`',column_name,'`,"0000-00-00 00:00:00") AS `',column_name,'`')
					
				ELSE
					concat('cast(`',column_name,'` AS char CHARACTER SET """+ self.charset +""") AS','`',column_name,'`')
					
				END
				AS select_stat,
				column_name
			FROM 
				information_schema.COLUMNS 
			WHERE 
				table_schema=%s
				AND 	table_name=%s
			ORDER BY 
				ordinal_position
			;
		"""
		self.cursor_buffered.execute(sql_select, (schema, table))
		select_data = self.cursor_buffered.fetchall()
		select_csv = ["COALESCE(REPLACE(%s, '\"', '\"\"'),'NULL') " % statement["select_csv"] for statement in select_data]
		select_stat = [statement["select_csv"] for statement in select_data]
		column_list = ['"%s"' % statement["column_name"] for statement in select_data]
		select_columns["select_csv"] = "REPLACE(CONCAT('\"',CONCAT_WS('\",\"',%s),'\"'),'\"NULL\"','NULL')" % ','.join(select_csv)
		select_columns["select_stat"]  = ','.join(select_stat)
		select_columns["column_list"]  = ','.join(column_list)
		return select_columns
		
	
	def lock_table(self, schema, table):
		"""
			The method flushes the given table with read lock.
			The method assumes there is a database connection active.
			
			:param schema: the origin's schema
			:param table: the table name 
			
		"""
		self.logger.debug("locking the table `%s`.`%s`" % (schema, table) )
		sql_lock = "FLUSH TABLES `%s`.`%s` WITH READ LOCK;" %(schema, table)
		self.logger.debug("collecting the master's coordinates for table `%s`.`%s`" % (schema, table) )
		self.cursor_buffered.execute(sql_lock)
		
	def get_master_coordinates(self):
		"""
			The method gets the master's coordinates and return them stored in a dictionary.
			The method assumes there is a database connection active.
			
			:return: the master's log coordinates for the given table
			:rtype: dictionary
		"""
		sql_master = "SHOW MASTER STATUS;" 
		self.cursor_buffered.execute(sql_master)
		master_status = self.cursor_buffered.fetchall()
		return master_status
		
	def copy_data(self, schema, table):
		"""
			The method copy the data between the origin and destination table.
			The method locks the table read only mode and  gets the log coordinates which are returned to the calling method.
			
			:param schema: the origin's schema
			:param table: the table name 
			:return: the log coordinates for the given table
			:rtype: dictionary
		"""
		slice_insert = []
		loading_schema = self.schema_loading[schema]["loading"]
		self.connect_db_buffered()
		
		self.logger.debug("estimating rows in %s.%s" % (schema , table))
		sql_rows = """ 
			SELECT 
				table_rows,
				CASE
					WHEN avg_row_length>0
					then
						round(({}/avg_row_length))
				ELSE
					0
				END as copy_limit
			FROM 
				information_schema.TABLES 
			WHERE 
					table_schema=%s 
				AND	table_type='BASE TABLE'
				AND table_name=%s 
			;
		"""
		sql_rows = sql_rows.format(self.copy_max_memory)
		self.cursor_buffered.execute(sql_rows, (schema, table))
		count_rows = self.cursor_buffered.fetchone()
		total_rows = count_rows["table_rows"]
		copy_limit = int(count_rows["copy_limit"])
		if copy_limit == 0:
			copy_limit=1000000
		num_slices=int(total_rows//copy_limit)
		range_slices=list(range(num_slices+1))
		total_slices=len(range_slices)
		slice=range_slices[0]
		self.logger.debug("The table %s.%s will be copied in %s  estimated slice(s) of %s rows"  % (schema, table, total_slices, copy_limit))

		out_file='%s/%s_%s.csv' % (self.out_dir, schema, table )
		self.lock_table(schema, table)
		master_status = self.get_master_coordinates()
		select_columns = self.generate_select_statements(schema, table)
		csv_data = ""
		sql_csv = "SELECT %s as data FROM `%s`.`%s`;" % (select_columns["select_csv"], schema, table)
		column_list = select_columns["column_list"]
		self.logger.debug("Executing query for table %s.%s"  % (schema, table ))
		self.connect_db_unbuffered()
		self.cursor_unbuffered.execute(sql_csv)
		while True:
			csv_results = self.cursor_unbuffered.fetchmany(copy_limit)
			if len(csv_results) == 0:
				break
			csv_data="\n".join(d[0] for d in csv_results )
			
			if self.copy_mode == 'direct':
				csv_file = io.StringIO()
				csv_file.write(csv_data)
				csv_file.seek(0)

			if self.copy_mode == 'file':
				csv_file = codecs.open(out_file, 'wb', self.charset)
				csv_file.write(csv_data)
				csv_file.close()
				csv_file = open(out_file, 'rb')
			try:
				self.pg_engine.copy_data(csv_file, loading_schema, table, column_list)
			except:
				self.logger.info("Table %s.%s error in PostgreSQL copy, saving slice number for the fallback to insert statements " %  (loading_schema, table ))
				slice_insert.append(slice)
				
			self.print_progress(slice+1,total_slices, schema, table)
			slice+=1

			csv_file.close()
		self.cursor_unbuffered.close()
		self.disconnect_db_unbuffered()
		if len(slice_insert)>0:
			ins_arg={}
			ins_arg["slice_insert"] = slice_insert
			ins_arg["table"] = table
			ins_arg["schema"] = schema
			ins_arg["select_stat"] = select_columns["select_stat"]
			ins_arg["column_list"] = column_list
			ins_arg["copy_limit"] = copy_limit
			self.insert_table_data(ins_arg)
		
		
		self.logger.debug("unlocking the table `%s`.`%s`" % (schema, table) )
		sql_unlock = "UNLOCK TABLES;" 
		self.cursor_buffered.execute(sql_unlock)
		self.disconnect_db_buffered()
		
		try:
			remove(out_file)
		except:
			pass
		return master_status
	
	def insert_table_data(self, ins_arg):
		"""
			This method is a fallback procedure whether copy_table_data fails.
			The ins_args is a list with the informations required to run the select for building the insert
			statements and the slices's start and stop.
			The process is performed in memory and can take a very long time to complete.
			
			:param pg_engine: the postgresql engine
			:param ins_arg: the list with the insert arguments (slice_insert, schema, table, select_stat,column_list, copy_limit)
		"""
		slice_insert= ins_arg["slice_insert"]
		table = ins_arg["table"]
		schema = ins_arg["schema"]
		select_stat = ins_arg["select_stat"]
		column_list = ins_arg["column_list"]
		copy_limit = ins_arg["copy_limit"] 
		self.connect_db_unbuffered()
		loading_schema = self.schema_loading[schema]["loading"]
		num_insert = 1
		for slice in slice_insert:
			self.logger.info("Executing inserts in %s.%s. Slice %s. Rows per slice %s." %  (loading_schema, table, num_insert, copy_limit ,   ))
			offset = slice*copy_limit
			sql_fallback = "SELECT %s FROM `%s`.`%s` LIMIT %s, %s;" % (select_stat, schema, table, offset, copy_limit)
			self.cursor_unbuffered.execute(sql_fallback)
			insert_data =  self.cursor_unbuffered.fetchall()
			self.pg_engine.insert_data(loading_schema, table, insert_data , column_list)
			num_insert +=1
		self.disconnect_db_unbuffered()
		
	
	def print_progress (self, iteration, total, schema, table):
		"""
			Print the copy progress in slices and estimated total slices. 
			In order to reduce noise when the log level is info only the tables copied in multiple slices
			get the print progress.
			
			:param iteration: The slice number currently processed
			:param total: The estimated total slices
			:param table_name: The table name
		"""
		if iteration>=total:
			total = iteration
		if total>1:
			self.logger.info("Table %s.%s copied %s slice of %s" % (schema, table, iteration, total))
		else:
			self.logger.debug("Table %s.%s copied %s slice of %s" % (schema, table, iteration, total))
			
	def create_indices(self, schema, table):
		"""
			The method copy the data between the origin and destination table.
			The method locks the table read only mode and  gets the log coordinates which are returned to the calling method.
			
			:param schema: the origin's schema
			:param table: the table name 
			:return: the table and schema name with the primary key.
			:rtype: dictionary
		"""
		loading_schema = self.schema_loading[schema]["loading"]
		self.connect_db_buffered()
		self.logger.debug("Creating indices on table %s.%s " % (schema, table))
		sql_index = """
			SELECT 
				index_name,
				non_unique,
				GROUP_CONCAT(column_name ORDER BY seq_in_index) as index_columns
			FROM
				information_schema.statistics
			WHERE
					table_schema=%s
				AND 	table_name=%s
				AND	index_type = 'BTREE'
			GROUP BY 
				table_name,
				non_unique,
				index_name
			;
		"""
		self.cursor_buffered.execute(sql_index, (schema, table))
		index_data = self.cursor_buffered.fetchall()
		table_pkey = self.pg_engine.create_indices(loading_schema, table, index_data)
		self.disconnect_db_buffered()
		return table_pkey
		
		
	def copy_tables(self):
		"""
			The method copies the data between tables, from the mysql schema to the corresponding
			postgresql loading schema. Before the copy starts the table is locked and then the lock is released.
		"""
		
		
		for schema in self.schema_tables:
			loading_schema = self.schema_loading[schema]["loading"]
			destination_schema = self.schema_loading[schema]["destination"]
			table_list = self.schema_tables[schema]
			for table in table_list:
				self.logger.info("Copying the source table %s into %s.%s" %(table, loading_schema, table) )
				master_status = self.copy_data(schema, table)
				table_pkey = self.create_indices(schema, table)
				self.pg_engine.store_table(destination_schema, table, table_pkey, master_status)
				
	
	def set_copy_max_memory(self):
		"""
			The method sets the class variable self.copy_max_memory using the value stored in the 
			source setting.

		"""
		copy_max_memory = str(self.source_config["copy_max_memory"])[:-1]
		copy_scale = str(self.source_config["copy_max_memory"])[-1]
		try:
			int(copy_scale)
			copy_max_memory = self.source_config["copy_max_memory"]
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
	
	def __init_read_replica(self):
		"""
			The method calls the pre-steps required by the read replica method.
			
		"""
		self.replica_conn = {}
		self.source_config = self.sources[self.source]
		self.my_server_id = self.source_config["my_server_id"]
		self.limit_tables = self.source_config["limit_tables"]
		self.skip_tables = self.source_config["skip_tables"]
		self.replica_batch_size = self.source_config["replica_batch_size"]
		self.hexify = [] + self.hexify_always
		self.connect_db_buffered()
		self.pg_engine.connect_db()
		self.schema_mappings = self.pg_engine.get_schema_mappings()
		self.schema_replica = [schema for schema in self.schema_mappings]
		db_conn = self.source_config["db_conn"]
		self.replica_conn["host"] = str(db_conn["host"])
		self.replica_conn["user"] = str(db_conn["user"])
		self.replica_conn["passwd"] = str(db_conn["password"])
		self.replica_conn["port"] = int(db_conn["port"])
			
		
	
	def init_sync(self):
		"""
			The method calls the common steps required to initialise the database connections and
			class attributes within sync_tables,refresh_schema and init_replica.
		"""
		self.source_config = self.sources[self.source]
		self.out_dir = self.source_config["out_dir"]
		self.copy_mode = self.source_config["copy_mode"]
		self.pg_engine.lock_timeout = self.source_config["lock_timeout"]
		self.pg_engine.grant_select_to = self.source_config["grant_select_to"]
		self.set_copy_max_memory()
		self.hexify = [] + self.hexify_always
		self.connect_db_buffered()
		self.pg_engine.connect_db()
		self.schema_mappings = self.pg_engine.get_schema_mappings()
		self.pg_engine.schema_tables = self.schema_tables
		
	
	def refresh_schema(self):
		"""
			The method performs a sync for an entire schema within a source.
			The method works in a similar way like init_replica.
			The swap happens in a single transaction.
		"""
		self.logger.debug("starting sync schema for source %s" % self.source)
		self.logger.debug("The schema affected is %s" % self.schema)
		self.init_sync()
		self.pg_engine.set_source_status("syncing")
		self.build_table_exceptions()
		self.schema_list = [self.schema]
		self.get_table_list()
		self.create_destination_schemas()
		self.pg_engine.schema_loading = self.schema_loading
		self.pg_engine.schema_tables = self.schema_tables
		self.create_destination_tables()
		self.disconnect_db_buffered()
		try:
			self.copy_tables()
			self.pg_engine.grant_select()
			self.pg_engine.swap_schemas()
			self.drop_loading_schemas()
			self.pg_engine.set_source_status("initialised")
		except:
			self.drop_loading_schemas()
			self.pg_engine.set_source_status("error")
			raise
	
	
	
	def sync_tables(self):
		"""
			The method performs a sync for specific tables.
			The method works in a similar way like init_replica except when swapping the relations.
			The tables are loaded into a temporary schema and the log coordinates are stored with the table
			in the replica catalogue. When the load is complete the method drops the existing table and changes the
			schema for the loaded tables to the destination schema. 
			The swap happens in a single transaction.
		"""
		self.logger.debug("starting sync tables for source %s" % self.source)
		self.logger.debug("The tables affected are %s" % self.tables)
		self.init_sync()
		self.pg_engine.set_source_status("syncing")
		self.build_table_exceptions()
		self.schema_list = [schema for schema in self.schema_mappings if schema in self.schema_only]
		self.get_table_list()
		self.create_destination_schemas()
		self.pg_engine.schema_loading = self.schema_loading
		self.pg_engine.schema_tables = self.schema_tables
		self.create_destination_tables()
		self.disconnect_db_buffered()
		self.copy_tables()
		self.pg_engine.grant_select()
		try:
			self.pg_engine.swap_tables()
			self.drop_loading_schemas()
			self.pg_engine.set_source_status("synced")
		except:
			self.drop_loading_schemas()
			self.pg_engine.set_source_status("error")
			raise
	
	def get_table_type_map(self):
		"""
			The method builds a dictionary with a key per each schema replicated.
			Each key maps a dictionary with the schema's tables stored as keys and the column/type mappings.
			The dictionary is used in the read_replica method, to determine whether a field requires hexadecimal conversion.
		"""
		table_type_map = {}
		table_map = {}
		self.logger.debug("collecting table type map")
		for schema in self.schema_replica:
			sql_tables = """	
				SELECT 
					table_schema,
					table_name
				FROM 
					information_schema.TABLES 
				WHERE 
						table_type='BASE TABLE' 
					AND	table_schema=%s
				;
			"""
			self.cursor_buffered.execute(sql_tables, (schema, ))
			table_list = self.cursor_buffered.fetchall()
			
			for table in table_list:
				column_type = {}
				sql_columns = """
					SELECT 
						column_name,
						data_type
					FROM 
						information_schema.COLUMNS 
					WHERE 
							table_schema=%s
						AND table_name=%s
					ORDER BY 
						ordinal_position
					;
				"""
				self.cursor_buffered.execute(sql_columns, (table["table_schema"], table["table_name"]))
				column_data = self.cursor_buffered.fetchall()
				for column in column_data:
					column_type[column["column_name"]] = column["data_type"]
				table_map[table["table_name"]] = column_type
			table_type_map[schema] = table_map
			table_map = {}
		return table_type_map
	
	
	def read_replica_stream(self, batch_data):
		"""
		Stream the replica using the batch data. This method evaluates the different events streamed from MySQL 
		and manages them accordingly. The BinLogStreamReader function is called with the only_event parameter which
		restricts the event type received by the streamer.
		The events managed are the following.
		RotateEvent which happens whether mysql restarts or the binary log file changes.
		QueryEvent which happens when a new row image comes in (BEGIN statement) or a DDL is executed.
		The BEGIN is always skipped. The DDL is parsed using the sql_token class. 
		[Write,Update,Delete]RowEvents are the row images pulled from the mysql replica.
		
		The RotateEvent and the QueryEvent cause the batch to be closed.
		
		The for loop reads the row events, builds the dictionary carrying informations like the destination schema,
		the 	binlog coordinates and store them into the group_insert list.
		When the number of events exceeds the replica_batch_size the group_insert is written into PostgreSQL.
		The batch is not closed in that case and the method exits only if there are no more rows available in the stream.
		Therefore the replica_batch_size is just the maximum size of the single insert and the size of replayed batch on PostgreSQL.
		The binlog switch or a captured DDL determines whether a batch is closed and processed.
		
		The update row event stores in a separate key event_before the row image before the update. This is required
		to allow updates where the primary key is updated as well.
		
		Each row event is scanned for data types requiring conversion to hex string.
		
		:param batch_data: The list with the master's batch data.
		:return: the batch's data composed by binlog name, binlog position and last event timestamp read from the mysql replica stream.
		:rtype: dictionary
		"""
		sql_tokeniser = sql_token()
		table_type_map = self.get_table_type_map()	
		inc_tables = self.pg_engine.get_inconsistent_tables()
		close_batch = False
		master_data = {}
		group_insert = []
		
		id_batch = batch_data[0][0]
		log_file = batch_data[0][1]
		log_position = batch_data[0][2]
		log_table = batch_data[0][3]
		
		my_stream = BinLogStreamReader(
			connection_settings = self.replica_conn, 
			server_id = self.my_server_id, 
			only_events = [RotateEvent, DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent], 
			log_file = log_file, 
			log_pos = log_position, 
			resume_stream = True, 
			only_schemas = self.schema_replica, 
			only_tables = self.limit_tables, 
			ignored_tables = self.skip_tables, 
		)
		self.logger.debug("log_file %s, log_position %s. id_batch: %s " % (log_file, log_position, id_batch))
		
		for binlogevent in my_stream:
			if isinstance(binlogevent, RotateEvent):
				event_time = binlogevent.timestamp
				binlogfile = binlogevent.next_binlog
				position = binlogevent.position
				self.logger.debug("ROTATE EVENT - binlogfile %s, position %s. " % (binlogfile, position))
				if log_file != binlogfile:
					close_batch = True
				if close_batch:
					if log_file!=binlogfile:
						master_data["File"]=binlogfile
						master_data["Position"]=position
						master_data["Time"]=event_time
					if len(group_insert)>0:
						self.pg_engine.write_batch(group_insert)
						group_insert=[]
					my_stream.close()
					return [master_data, close_batch]
			elif isinstance(binlogevent, QueryEvent):
				event_time = binlogevent.timestamp
				try:
					schema_query = binlogevent.schema.decode()
				except:
					schema_query = binlogevent.schema
				destination_schema = self.schema_mappings[schema_query]
				if binlogevent.query.strip().upper() not in self.statement_skip and schema_query in self.schema_mappings: 
					log_position = binlogevent.packet.log_pos
					master_data["File"] = binlogfile
					master_data["Position"] = log_position
					master_data["Time"] = event_time
					if len(group_insert)>0:
						self.pg_engine.write_batch(group_insert)
						group_insert=[]
					self.logger.info("QUERY EVENT - binlogfile %s, position %s.\n--------\n%s\n-------- " % (binlogfile, log_position, binlogevent.query))
					sql_tokeniser.parse_sql(binlogevent.query)
					for token in sql_tokeniser.tokenised:
						write_ddl = True
						table_name = token["name"] 
						table_key_dic = "%s.%s" % (destination_schema, table_name)
						if table_key_dic in inc_tables:
							write_ddl = False
							log_seq = int(log_file.split('.')[1])
							log_pos = int(log_position)
							table_dic = inc_tables[table_key_dic]
							if log_seq > table_dic["log_seq"]:
								write_ddl = True
							elif log_seq == table_dic["log_seq"] and log_pos >= table_dic["log_pos"]:
								write_ddl = True
							if write_ddl:
								self.logger.info("CONSISTENT POINT FOR TABLE %s REACHED  - binlogfile %s, position %s" % (table_key_dic, binlogfile, log_position))
								self.pg_engine.set_consistent_table(table_name, destination_schema)
								inc_tables = self.pg_engine.get_inconsistent_tables()
						if write_ddl:
							table_metadata = self.get_table_metadata(table_name, schema_query)
							event_time = binlogevent.timestamp
							self.logger.debug("TOKEN: %s" % (token))
							
							if len(token)>0:
								query_data={
									"binlog":log_file, 
									"logpos":log_position, 
									"schema": destination_schema, 
									"batch_id":id_batch, 
									"log_table":log_table
								}
								self.pg_engine.write_ddl(token, query_data, table_metadata, destination_schema)
								close_batch=True
							
						
					sql_tokeniser.reset_lists()
					if close_batch:
						my_stream.close()
						return [master_data, close_batch]
			else:
				size_insert=0
				for row in binlogevent.rows:
					add_row = True
					log_file=binlogfile
					log_position=binlogevent.packet.log_pos
					table_name=binlogevent.table
					event_time=binlogevent.timestamp
					schema_row = binlogevent.schema
					destination_schema = self.schema_mappings[schema_row]
					table_key_dic = "%s.%s" % (destination_schema, table_name)
					if table_key_dic in inc_tables:
						table_consistent = False
						log_seq = int(log_file.split('.')[1])
						log_pos = int(log_position)
						table_dic = inc_tables[table_key_dic]
						if log_seq > table_dic["log_seq"]:
							table_consistent = True
						elif log_seq == table_dic["log_seq"] and log_pos >= table_dic["log_pos"]:
							table_consistent = True
							self.logger.debug("CONSISTENT POINT FOR TABLE %s REACHED  - binlogfile %s, position %s" % (table_key_dic, binlogfile, log_position))
						if table_consistent:
							add_row = True
							self.pg_engine.set_consistent_table(table_name, destination_schema)
							inc_tables = self.pg_engine.get_inconsistent_tables()
						else:
							add_row = False
					
					column_map=table_type_map[schema_row][table_name]
					global_data={
										"binlog":log_file, 
										"logpos":log_position, 
										"schema": destination_schema, 
										"table": table_name, 
										"batch_id":id_batch, 
										"log_table":log_table, 
										"event_time":event_time
									}
					event_after={}
					event_before={}
					if add_row:
						event_insert = {}
						if isinstance(binlogevent, DeleteRowsEvent):
							global_data["action"] = "delete"
							event_after=row["values"]
						elif isinstance(binlogevent, UpdateRowsEvent):
							global_data["action"] = "update"
							event_after=row["after_values"]
							event_before=row["before_values"]
						elif isinstance(binlogevent, WriteRowsEvent):
							global_data["action"] = "insert"
							event_after=row["values"]
						for column_name in event_after:
							column_type=column_map[column_name]
							if column_type in self.hexify and event_after[column_name]:
								event_after[column_name]=binascii.hexlify(event_after[column_name]).decode()
							elif column_type in self.hexify and isinstance(event_after[column_name], bytes):
								event_after[column_name] = ''
						for column_name in event_before:
							column_type=column_map[column_name]
							if column_type in self.hexify and event_before[column_name]:
								event_before[column_name]=binascii.hexlify(event_before[column_name]).decode()
							elif column_type in self.hexify and isinstance(event_before[column_name], bytes):
								event_before[column_name] = ''
						event_insert={"global_data":global_data,"event_after":event_after,  "event_before":event_before}
						size_insert += len(str(event_insert))
						group_insert.append(event_insert)
						
					master_data["File"]=log_file
					master_data["Position"]=log_position
					master_data["Time"]=event_time
					
					if len(group_insert)>=self.replica_batch_size:
						self.logger.debug("Max rows per batch reached. Writing %s. rows. Size in bytes: %s " % (len(group_insert), size_insert))
						self.logger.debug("Master coordinates: %s" % (master_data, ))
						self.pg_engine.write_batch(group_insert)
						size_insert=0
						group_insert=[]
						close_batch=True
						
						
						
		my_stream.close()
		if len(group_insert)>0:
			self.logger.debug("writing the last %s events" % (len(group_insert), ))
			self.pg_engine.write_batch(group_insert)
			close_batch=True
		
		return [master_data, close_batch]
	
	
	def read_replica(self):
		"""
			The method gets the batch data from PostgreSQL. 
			If the batch data is not empty then method read_replica_stream is executed to get the rows from 
			the mysql replica stored into the PostgreSQL database.
			When the method exits the replica_data list is decomposed in the master_data (log name, position and last event's timestamp).
			If the flag close_batch is set then the master status is saved in PostgreSQL the batch id  returned by the method is 
			is saved in the class variable id_batch.
			This variable is used to determine whether the old batch should be closed or not. 
			If the variable is not empty then the previous batch gets closed with a simple update of the processed flag.
		
		"""
		self.__init_read_replica()
		self.pg_engine.set_source_status("running")
		
		batch_data = self.pg_engine.get_batch_data()
		if len(batch_data)>0:
			id_batch=batch_data[0][0]
			replica_data=self.read_replica_stream(batch_data)
			master_data=replica_data[0]
			close_batch=replica_data[1]
			if close_batch:
				self.master_status=[]
				self.master_status.append(master_data)
				self.logger.debug("trying to save the master data...")
				next_id_batch=self.pg_engine.save_master_status(self.master_status)
				if next_id_batch:
					self.logger.debug("new batch created, saving id_batch %s in class variable" % (id_batch))
					self.id_batch=id_batch
				else:
					self.logger.debug("batch not saved. using old id_batch %s" % (self.id_batch))
				if self.id_batch:
					self.logger.debug("updating processed flag for id_batch %s", (id_batch))
					self.pg_engine.set_batch_processed(id_batch)
					self.id_batch=None
		self.pg_engine.check_source_consistent()

		self.disconnect_db_buffered()
		
		
	def init_replica(self):
		"""
			The method performs a full init replica for the given sources
		"""
		self.logger.debug("starting init replica for source %s" % self.source)
		self.init_sync()
		master_batch = self.get_master_coordinates()
		self.pg_engine.set_source_status("initialising")
		self.pg_engine.cleanup_source_tables()
		self.schema_list = [schema for schema in self.schema_mappings]
		self.build_table_exceptions()
		self.get_table_list()
		self.create_destination_schemas()
		try:
			self.pg_engine.insert_source_timings()
			self.pg_engine.schema_loading = self.schema_loading
			self.create_destination_tables()
			self.disconnect_db_buffered()
			self.copy_tables()
			self.pg_engine.grant_select()
			self.pg_engine.swap_schemas()
			self.pg_engine.clean_batch_data()
			self.pg_engine.save_master_status(master_batch)
			self.pg_engine.set_source_highwatermark(master_batch, consistent=False)
			self.drop_loading_schemas()
			self.pg_engine.set_source_status("initialised")
		except:
			self.drop_loading_schemas()
			self.pg_engine.set_source_status("error")
			raise
		
		
		


