import psycopg2
from psycopg2 import sql
import sys
import json
import datetime
import decimal
import time
import base64
import os
from distutils.sysconfig import get_python_lib

class pg_encoder(json.JSONEncoder):
	def default(self, obj):
		if 		isinstance(obj, datetime.time) or \
				isinstance(obj, datetime.datetime) or  \
				isinstance(obj, datetime.date) or \
				isinstance(obj, decimal.Decimal) or \
				isinstance(obj, datetime.timedelta) or \
				isinstance(obj, set):
					
			return str(obj)
		return json.JSONEncoder.default(self, obj)

class pg_engine(object):
	def __init__(self):
		python_lib=get_python_lib()
		self.sql_dir = "%s/pg_chameleon/sql/" % python_lib
		self.table_ddl={}
		self.idx_ddl={}
		self.type_ddl={}
		self.idx_sequence=0
		self.type_dictionary = {
			'integer':'integer',
			'mediumint':'bigint',
			'tinyint':'integer',
			'smallint':'integer',
			'int':'integer',
			'bigint':'bigint',
			'varchar':'character varying',
			'character varying':'character varying',
			'text':'text',
			'char':'character',
			'datetime':'timestamp without time zone',
			'date':'date',
			'time':'time without time zone',
			'timestamp':'timestamp without time zone',
			'tinytext':'text',
			'mediumtext':'text',
			'longtext':'text',
			'tinyblob':'bytea',
			'mediumblob':'bytea',
			'longblob':'bytea',
			'blob':'bytea', 
			'binary':'bytea', 
			'varbinary':'bytea', 
			'decimal':'numeric', 
			'double':'double precision', 
			'double precision':'double precision', 
			'float':'double precision', 
			'bit':'integer', 
			'year':'integer', 
			'enum':'enum', 
			'set':'text', 
			'json':'text', 
			'bool':'boolean', 
			'boolean':'boolean', 
			'geometry':'bytea',
		}
		self.dest_conn = None
		self.pgsql_conn = None
		self.logger = None
		self.idx_sequence = 0
		
	def __del__(self):
		"""
			Class destructor, tries to disconnect the postgresql connection.
		"""
		self.disconnect_db()
		
	def connect_db(self):
		"""
			Connects to PostgreSQL using the parameters stored in self.dest_conn. The dictionary is built using the parameters set via adding the key dbname to the self.pg_conn dictionary.
			This method's connection and cursors are widely used in the procedure except for the replay process which uses a 
			dedicated connection and cursor.
		"""
		if self.dest_conn and not self.pgsql_conn:
			strconn = "dbname=%(database)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % self.dest_conn
			self.pgsql_conn = psycopg2.connect(strconn)
			self.pgsql_conn .set_client_encoding(self.dest_conn["charset"])
			self.pgsql_conn.set_session(autocommit=True)
			self.pgsql_cur = self.pgsql_conn .cursor()
			
		else:
			self.logger.error("There is no database connection available.")
			sys.exit()

	def disconnect_db(self):
		"""
			The method disconnects the postgres connection if there is any active. Otherwise ignore it.
		"""
		if self.pgsql_conn:
			self.pgsql_conn.close()
		else:
			pass

	def create_replica_schema(self):
		"""
			The method installs the replica schema sch_chameleon if not already  present.
		"""
		self.logger.debug("Trying to connect to the destination database.")
		self.connect_db()
		num_schema = self.check_replica_schema()[0]
		if num_schema == 0:
			self.logger.debug("Creating the replica schema.")
			file_schema = open(self.sql_dir+"create_schema.sql", 'rb')
			sql_schema = file_schema.read()
			file_schema.close()
			self.pgsql_cur.execute(sql_schema)
		
		else:
			self.logger.warning("The replica schema is already present.")
	
	def drop_replica_schema(self):
		"""
			The method removes the service schema discarding all the replica references.
			The replicated tables are kept in place though.
		"""
		self.logger.debug("Trying to connect to the destination database.")
		self.connect_db()
		file_schema = open(self.sql_dir+"drop_schema.sql", 'rb')
		sql_schema = file_schema.read()
		file_schema.close()
		self.pgsql_cur.execute(sql_schema)
	
	def check_replica_schema(self):
		"""
			The method checks if the sch_chameleon exists
			
			:return: count from information_schema.schemata
			:rtype: integer
		"""
		sql_check="""
			SELECT 
				count(*)
			FROM 
				information_schema.schemata  
			WHERE 
				schema_name='sch_chameleon'
		"""
			
		self.pgsql_cur.execute(sql_check)
		num_schema = self.pgsql_cur.fetchone()
		return num_schema
	
	def check_schema_mappings(self):
		"""
			The method checks if there is already a destination schema in the stored schema mappings.
			As each schema should be managed by one mapping only, if the method returns None  then
			the source can be store safely. Otherwise the action. The method doesn't take any decision
			leaving this to the calling methods.
			The method assumes there is a database connection active.
		"""
		schema_mappings = json.dumps(self.sources[self.source]["schema_mappings"])
		sql_check = """
			WITH t_check  AS
			(
					SELECT 
						(jsonb_each_text(jsb_schema_mappings)).value AS dest_schema
					FROM 
						sch_chameleon.t_sources
				UNION ALL
					SELECT 
						value AS dest_schema 
					FROM 
						json_each_text(%s::json) 
			)
		SELECT 
			count(dest_schema),
			dest_schema 
		FROM 
			t_check 
		GROUP BY 
			dest_schema
		HAVING 
			count(dest_schema)>1
		;
		"""
		self.pgsql_cur.execute(sql_check, (schema_mappings, ))
		check_mappings = self.pgsql_cur.fetchone()
		return check_mappings
		
	def check_source(self):
		"""
			The method checks if the source name stored in the class variable self.source is already present.
			As this method is used in both add and drop source it just retuns the count of the sources.
			Any decision about the source is left to the calling method.
			The method assumes there is a database connection active.
			
		"""
		sql_check = """
			SELECT 
				count(*) 
			FROM 
				sch_chameleon.t_sources 
			WHERE 
				t_source=%s;
		"""
		self.pgsql_cur.execute(sql_check, (self.source, ))
		num_sources = self.pgsql_cur.fetchone()
		return num_sources[0]
	
	def add_source(self):
		"""
			The method adds a new source to the replication catalog.
			The method calls the function fn_refresh_parts() which generates the log tables used by the replica.
			If the source is already present a warning is issued and no other action is performed.
		"""
		self.logger.debug("Checking if the source %s already exists" % self.source)
		self.connect_db()
		num_sources = self.check_source()
		
		if num_sources == 0:
			check_mappings = self.check_schema_mappings()
			if check_mappings:
				self.logger.error("Could not register the source %s. There is a duplicate destination schema in the schema mappings." % self.source)
			else:
				self.logger.debug("Adding source %s " % self.source)
				schema_mappings = json.dumps(self.sources[self.source]["schema_mappings"])
				log_table_1 = "t_log_replica_%s_1" % self.source
				log_table_2 = "t_log_replica_%s_2" % self.source
				sql_add = """
					INSERT INTO sch_chameleon.t_sources 
						( 
							t_source,
							jsb_schema_mappings,
							v_log_table
						) 
					VALUES 
						(
							%s,
							%s,
							ARRAY[%s,%s]
						)
					; 
				"""
				self.pgsql_cur.execute(sql_add, (self.source, schema_mappings, log_table_1, log_table_2))
				
				sql_parts = """SELECT sch_chameleon.fn_refresh_parts() ;"""
				self.pgsql_cur.execute(sql_parts)
		else:
			self.logger.warning("The source %s already exists" % self.source)

	def drop_source(self):
		"""
			The method deletes the source from the replication catalogue.
			The log tables are dropped as well, discarding any replica reference for the source.
		"""
		self.logger.debug("Deleting the source %s " % self.source)
		self.connect_db()
		num_sources = self.check_source()
		if num_sources == 1:
			sql_delete = """ DELETE FROM sch_chameleon.t_sources 
						WHERE  t_source=%s
						RETURNING v_log_table
						; """
			self.pgsql_cur.execute(sql_delete, (self.source, ))
			source_drop = self.pgsql_cur.fetchone()
			for log_table in source_drop[0]:
				sql_drop = """DROP TABLE sch_chameleon."%s"; """ % (log_table)
				try:
					self.pgsql_cur.execute(sql_drop)
				except:
					self.logger.debug("Could not drop the table sch_chameleon.%s you may need to remove it manually." % log_table)
		else:
			self.logger.debug("There is no source %s registered in the replica catalogue" % self.source)
			
	def get_schema_list(self):
		"""
			The method gets the list of source schemas for the given source.
			The list is generated using the mapping in sch_chameleon.t_sources. 
			Any change in the configuration file is ignored
			The method assumes there is a database connection active.
		"""
		self.logger.debug("Collecting schema list for source %s" % self.source)
		sql_get_schema = """
			SELECT 
				(jsonb_each_text(jsb_schema_mappings)).key
			FROM 
				sch_chameleon.t_sources
			WHERE 
				t_source=%s;
			
		"""
		self.pgsql_cur.execute(sql_get_schema, (self.source, ))
		schema_list = [schema[0] for schema in self.pgsql_cur.fetchall()]
		self.logger.debug("Found origin's replication schemas %s" % ', '.join(schema_list))
		return schema_list
	
	def build_create_table(self, table_metadata,table_name,  schema):
		"""
			The method builds the create table statement with any enumeration associated.
			The returned value is a dictionary with the optional enumeration's ddl and the create table without indices or primary keys.
			on the destination schema specified by destination_schema.
			The method assumes there is a database connection active.
			
			:param table_metadata: the column dictionary extracted from the source's information_schema or builty by the sql_parser class
			:param table_name: the table name 
			:param destination_schema: the schema where the table belongs
			:return: a dictionary with the optional create statements for enumerations and the create table
			:rtype: dictionary
		"""
		destination_schema = self.schema_loading[schema]["loading"]
		ddl_head = 'CREATE TABLE "%s"."%s" (' % (destination_schema, table_name)
		ddl_tail = ");"
		ddl_columns = []
		ddl_enum=[]
		table_ddl = {}
		for column in table_metadata:
			if column["is_nullable"]=="NO":
					col_is_null="NOT NULL"
			else:
				col_is_null="NULL"
			column_type = self.get_data_type(column, schema, table_name)
			if column_type == "enum":
				enum_type = '"%s"."enum_%s_%s"' % (destination_schema, table_name[0:20], column["column_name"][0:20])
				sql_drop_enum = 'DROP TYPE IF EXISTS %s CASCADE;' % enum_type
				sql_create_enum = 'CREATE TYPE %s AS ENUM %s;' % ( enum_type,  column["enum_list"])
				ddl_enum.append(sql_drop_enum)
				ddl_enum.append(sql_create_enum)
				column_type=enum_type
			if column_type == "character varying" or column_type == "character":
				column_type="%s (%s)" % (column_type, str(column["character_maximum_length"]))
			if column_type == 'numeric':
				column_type="%s (%s,%s)" % (column_type, str(column["numeric_precision"]), str(column["numeric_scale"]))
			if column["extra"] == "auto_increment":
				column_type = "bigserial"
			ddl_columns.append(  ' "%s" %s %s   ' %  (column["column_name"], column_type, col_is_null ))
		def_columns=str(',').join(ddl_columns)
		table_ddl["enum"] = ddl_enum
		table_ddl["table"] = (ddl_head+def_columns+ddl_tail)
		return table_ddl

		
		
	def get_data_type(self, column, schema,  table):
		""" 
			The method determines whether the specified type has to be overridden or not.
			
			:todo: check the table is correctly matched against the schema.
			
			:param column: the column dictionary extracted from the information_schema or built in the sql_parser class
			:param schema: the schema name 
			:param table: the table name 
			:return: the postgresql converted column type
			:rtype: string
		"""
		try:
			
			table_full = "%s.%s" % (schema, table)
			type_override = self.type_override[column["column_type"]]
			override_to = type_override["override_to"]
			override_tables = type_override["override_tables"]
			if override_tables[0] == '*' or table_full in override_tables:
				column_type = override_to
			else:
				column_type = self.type_dictionary[column["data_type"]]
		except KeyError:
			column_type = self.type_dictionary[column["data_type"]]
		return column_type
	
	def create_table(self,  table_metadata,table_name,  schema):
		"""
			Executes the create table returned by build_create_table on the destination_schema.
			
			:param table_metadata: the column dictionary extracted from the source's information_schema or builty by the sql_parser class
			:param table_name: the table name 
			:param destination_schema: the schema where the table belongs
		"""
		table_ddl = self.build_create_table( table_metadata,table_name,  schema)
		enum_ddl = table_ddl["enum"] 
		table_ddl = table_ddl["table"] 
		for enum_statement in enum_ddl:
			self.pgsql_cur.execute(enum_statement)
		self.pgsql_cur.execute(table_ddl)
	
	def get_schema_mappings(self):
		"""
			The method gets the schema mappings for the given source.
			The list is the one stored in the table sch_chameleon.t_sources. 
			Any change in the configuration file is ignored
			The method assumes there is a database connection active.
		"""
		self.logger.debug("Collecting schema mappings for source %s" % self.source)
		sql_get_schema = """
			SELECT 
				jsb_schema_mappings
			FROM 
				sch_chameleon.t_sources
			WHERE 
				t_source=%s;
			
		"""
		self.pgsql_cur.execute(sql_get_schema, (self.source, ))
		schema_mappings = self.pgsql_cur.fetchone()
		return schema_mappings[0]
	
	def set_source_status(self, source_status):
		"""
			Sets the source status for the source_name and sets the two class attributes i_id_source and dest_schema.
			The method assumes there is a database connection active.
			
			:param source_status: The source status to be set.
			
		"""
		sql_source = """
			UPDATE sch_chameleon.t_sources
			SET
				enm_status=%s
			WHERE
				t_source=%s
			RETURNING i_id_source
				;
			"""
		self.pgsql_cur.execute(sql_source, (source_status, self.source, ))
		source_data = self.pgsql_cur.fetchone()
		

		try:
			self.i_id_source = source_data[0]
		except:
			print("Source %s is not registered." % self.source)
			sys.exit()
	
	def clean_batch_data(self):
		"""
			This method removes all the batch data for the source id stored in the class varible self.i_id_source.
			
			The method assumes there is a database connection active.
		"""
		sql_cleanup = """
			DELETE FROM sch_chameleon.t_replica_batch WHERE i_id_source=%s;
		"""
		self.pgsql_cur.execute(sql_cleanup, (self.i_id_source, ))
	
	def save_master_status(self, master_status):
		"""
			This method saves the master data determining which log table should be used in the next batch.
			The method assumes there is a database connection active.
			
			:param master_status: the master data with the binlogfile and the log position
			:return: the batch id or none if no batch has been created
			:rtype: integer
		"""
		next_batch_id = None
		master_data = master_status[0]
		binlog_name = master_data["File"]
		binlog_position = master_data["Position"]
		try:
			event_time = master_data["Time"]
		except:
			event_time = None
		
		sql_master = """
			INSERT INTO sch_chameleon.t_replica_batch
				(
					i_id_source,
					t_binlog_name, 
					i_binlog_position
				)
			VALUES 
				(
					%s,
					%s,
					%s
				)
			RETURNING i_id_batch
			;
		"""
		
		try:
			self.pgsql_cur.execute(sql_master, (self.i_id_source, binlog_name, binlog_position))
			results =self.pgsql_cur.fetchone()
			next_batch_id=results[0]
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(self.pgsql_cur.mogrify(sql_master, (self.i_id_source, binlog_name, binlog_position)))
		
		return next_batch_id

	
	def store_table(self, schema, table, table_pkey, master_status):
		"""
			The method saves the table name along with the primary key definition in the table t_replica_tables.
			This is required in order to let the replay procedure which primary key to use replaying the update and delete.
			If the table is without primary key is not stored. 
			A table without primary key is copied and the indices are create like any other table. 
			However the replica doesn't work for the tables without primary key.
			
			If the class variable master status is set then the master's coordinates are saved along with the table.
			This happens in general when a table is added to the replica or the data is refreshed with sync_tables.
			
			:param schema: the schema name to store in the table  t_replica_tables
			:param table: the table name to store in the table  t_replica_tables
			:param table_pkey: a list with the primary key's columns. empty if there's no pkey
			:param master_status: the master status data .
		"""
		if master_status:
			master_data = master_status[0]
			binlog_file = master_data["File"]
			binlog_pos = master_data["Position"]
		else:
			binlog_file = None
			binlog_pos = None
			
		
		if len(table_pkey) > 0:
			sql_insert = """ 
				INSERT INTO sch_chameleon.t_replica_tables 
					(
						i_id_source,
						v_table_name,
						v_schema_name,
						v_table_pkey,
						t_binlog_name,
						i_binlog_position
					)
				VALUES 
					(
						%s,
						%s,
						%s,
						%s,
						%s,
						%s
					)
				ON CONFLICT (i_id_source,v_table_name,v_schema_name)
					DO UPDATE 
						SET 
							v_table_pkey=EXCLUDED.v_table_pkey,
							t_binlog_name = EXCLUDED.t_binlog_name,
							i_binlog_position = EXCLUDED.i_binlog_position
									;
							"""
			self.pgsql_cur.execute(sql_insert, (
				self.i_id_source, 
				table, 
				schema, 
				table_pkey, 
				binlog_file, 
				binlog_pos
				)
			)
		else:
			self.logger.warning("Missing primary key. The table %s will not be replicated." % (schema, table,))
			sql_delete = """
				DELETE FROM sch_chameleon.t_replica_tables
				WHERE
						i_id_source=%s
					AND	v_table_name=%s
					AND	v_schema_name=%s
				;
			"""
			self.pgsql_cur.execute(sql_delete, (
				self.i_id_source, 
				table, 
				schema)
				)
		

	
	def copy_data(self, csv_file, schema, table, column_list):
		"""
			The method copy the data into postgresql using psycopg2's copy_expert.
			The csv_file is a file like object which can be either a  csv file or a string io object, accordingly with the 
			configuration parameter copy_mode.
			The method assumes there is a database connection active.
			
			:param csv_file: file like object with the table's data stored in CSV format
			:param schema: the schema used in the COPY FROM command
			:param table: the table name used in the COPY FROM command
			:param column_list: A string with the list of columns to use in the COPY FROM command already quoted and comma separated
		"""
		sql_copy='COPY "%s"."%s" (%s) FROM STDIN WITH NULL \'NULL\' CSV QUOTE \'"\' DELIMITER \',\' ESCAPE \'"\' ; ' % (schema, table, column_list)		
		self.pgsql_cur.copy_expert(sql_copy,csv_file)
		
	def insert_data(self, schema, table, insert_data , column_list):
		"""
			The method is a fallback procedure for when the copy method fails.
			The procedure performs a row by row insert, very slow but capable to skip the rows with problematic data (e.g. encoding issues).
			
			:param schema: the schema name where table belongs
			:param table: the table name where the data should be inserted
			:param insert_data: a list of records extracted from the database using the unbuffered cursor
			:param column_list: the list of column names quoted  for the inserts
		"""
		sample_row = insert_data[0]
		column_marker=','.join(['%s' for column in sample_row])
		
		sql_head='INSERT INTO "%s"."%s"(%s) VALUES (%s);' % (schema, table, column_list, column_marker)
		for data_row in insert_data:
			try:
				self.pgsql_cur.execute(sql_head,data_row)	
			except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(self.pgsql_cur.mogrify(sql_head,data_row))
			except:
				self.logger.error("unexpected error when processing the row")
				self.logger.error(" - > Table: %s.%s" % (schema, table))
				self.logger.error(" - > Insert list: %s" % (column_list))
				self.logger.error(" - > Insert values: %s" % (','.join(data_row)) )
	
	def create_indices(self, schema, table, index_data):
		"""
			The method loops odver the list index_data and creates the indices on the table 
			specified with schema and table parameters.
			The method assumes there is a database connection active.
			
			:param schema: the schema name where table belongs
			:param table: the table name where the data should be inserted
			:param index_data: a list of dictionaries with the index metadata for the given table.
			:return: a list with the eventual column(s) used as primary key
			:rtype: list
		"""
		idx_ddl = {}
		table_primary = []
		for index in index_data:
				table_timestamp = str(int(time.time()))
				indx = index["index_name"]
				self.logger.debug("Building DDL for index %s" % (indx))
				index_columns = index["index_columns"].split(',')
				non_unique = index["non_unique"]
				if indx =='PRIMARY':
					pkey_name = "pk_%s_%s_%s " % (table[0:10],table_timestamp,  self.idx_sequence)
					pkey_def = 'ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" PRIMARY KEY (%s) ;' % (schema, table, pkey_name, ','.join(index_columns))
					idx_ddl[pkey_name] = pkey_def
					table_primary = index_columns
				else:
					if non_unique == 0:
						unique_key = 'UNIQUE'
					else:
						unique_key = ''
					index_name='idx_%s_%s_%s_%s' % (indx[0:10], table[0:10], table_timestamp, self.idx_sequence)
					idx_def='CREATE %s INDEX "%s" ON "%s"."%s" (%s);' % (unique_key, index_name, schema, table, ','.join(index_columns) )
					idx_ddl[index_name] = idx_def
				self.idx_sequence+=1
		for index in idx_ddl:
			self.logger.info("Building index %s on %s.%s" % (index, schema, table))
			self.pgsql_cur.execute(idx_ddl[index])	
			
		return table_primary	
		
	def swap_schemas(self):
		"""
			The method  loops over the schema_loading class dictionary and 
			swaps the loading with the destination schemas performing a double rename.
			The method assumes there is a database connection active.
		"""
		for schema in self.schema_loading:
			schema_loading = self.schema_loading[schema]["loading"]
			schema_destination = self.schema_loading[schema]["destination"]
			schema_temporary = "_rename_%s" % self.schema_loading[schema]["destination"]
			sql_dest_to_tmp = sql.SQL("ALTER SCHEMA {} RENAME TO {};").format(sql.Identifier(schema_destination), sql.Identifier(schema_temporary))
			sql_load_to_dest = sql.SQL("ALTER SCHEMA {} RENAME TO {};").format(sql.Identifier(schema_loading), sql.Identifier(schema_destination))
			sql_tmp_to_load = sql.SQL("ALTER SCHEMA {} RENAME TO {};").format(sql.Identifier(schema_temporary), sql.Identifier(schema_loading))
			self.logger.info("Swapping schema %s with %s" % (schema_destination, schema_loading))
			self.logger.debug("Renaming schema %s in %s" % (schema_destination, schema_temporary))
			self.pgsql_cur.execute(sql_dest_to_tmp)
			self.logger.debug("Renaming schema %s in %s" % (schema_loading, schema_destination))
			self.pgsql_cur.execute(sql_load_to_dest)
			self.logger.debug("Renaming schema %s in %s" % (schema_temporary, schema_loading))
			self.pgsql_cur.execute(sql_tmp_to_load)
	
	def create_database_schema(self, schema_name):
		"""
			The method creates a database schema.
			The create schema is issued with the clause IF NOT EXISTS.
			Should the schema be already present the create is skipped.
			
			:param schema_name: The schema name to be created. 
		"""
		sql_create = sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name))
		self.pgsql_cur.execute(sql_create)
	
	def drop_database_schema(self, schema_name, cascade):
		"""
			The method drops a database schema.
			The drop can be either schema is issued with the clause IF NOT EXISTS.
			Should the schema be already present the create is skipped.
			
			:param schema_name: The schema name to be created. 
			:param schema_name: If true the schema is dropped with the clause cascade. 
		"""
		if cascade:
			cascade_clause = "CASCADE"
		else:
			cascade_clause = ""
		sql_drop = "DROP SCHEMA IF EXISTS {} %s;" % cascade_clause
		sql_drop = sql.SQL(sql_drop).format(sql.Identifier(schema_name))
		self.pgsql_cur.execute(sql_drop)
