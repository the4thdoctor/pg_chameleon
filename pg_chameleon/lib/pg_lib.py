import psycopg2
import os
import io
import sys
import json
import datetime
import decimal
import time
import binascii
class pg_encoder(json.JSONEncoder):
	def default(self, obj):
		if 	isinstance(obj, datetime.time) or \
			isinstance(obj, datetime.datetime) or  \
			isinstance(obj, datetime.date) or \
			isinstance(obj, decimal.Decimal) or \
			isinstance(obj, datetime.timedelta) or \
			isinstance(obj, set):
				
			return str(obj)
		return json.JSONEncoder.default(self, obj)


class pg_connection(object):
	def __init__(self, global_config):
		self.global_conf=global_config
		self.pg_conn=self.global_conf.pg_conn
		self.pg_database=self.global_conf.pg_database
		self.dest_schema=self.global_conf.my_database
		self.pg_connection=None
		self.pgsql_cur=None
		self.pgsql_cur_replay=None
		self.pg_charset=self.global_conf.pg_charset
		
	
	def connect_db(self):
		"""
			Connects to PostgreSQL using the parameters stored in pg_pars built adding the key dbname to the self.pg_conn dictionary.
			This method's connection and cursors are widely used in the procedure except for the replay process which uses a 
			dedicated connection and cursor.
			The method after the connection creates a database cursor and set the session to autocommit.
		"""
		pg_pars=dict(list(self.pg_conn.items())+ list({'dbname':self.pg_database}.items()))
		strconn="dbname=%(dbname)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % pg_pars
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		self.pgsql_conn .set_client_encoding(self.pg_charset)
		self.pgsql_cur=self.pgsql_conn .cursor()
	
	def connect_replay_db(self):
		"""
			Connects to PostgreSQL using the parameters stored in pg_pars built adding the key dbname to the self.pg_conn dictionary.
			The method after the connection creates a database cursor and set the session to autocommit.
			This method creates an additional connection and cursor used by the replay process. 

		"""
		pg_pars=dict(list(self.pg_conn.items())+ list({'dbname':self.pg_database}.items()))
		strconn="dbname=%(dbname)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % pg_pars
		self.pgsql_conn_replay = psycopg2.connect(strconn)
		self.pgsql_conn_replay.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		self.pgsql_conn_replay.set_client_encoding(self.pg_charset)
		self.pgsql_cur_replay=self.pgsql_conn_replay.cursor()
		
		
	def disconnect_db(self):
		"""
			The method disconnects from the main database connection.
		"""
		self.pgsql_conn.close()
	
	def disconnect_replay_db(self):
		"""
			The method disconnects from the replay database connection.
		"""
		self.pgsql_conn_replay.close()
		
	
		

class pg_engine(object):
	"""
		The class pg_engine manages the replica initialisation and execution on the PostgreSQL side.
		
		The class connects to the database when instantiated and setup several class attributes used by the replica.
		In particular the class dictionary type_dictionary is used to map the MySQL types to the equivalent PostgreSQL types.
		Unlike pgloader, which allows the type mapping configuration, the dictionary is hardcoded as the mapping is an effort to keep the replica running as smooth as possible.
		The class manages the replica catalogue upgrade using the current catalogue version self.cat_version and the list of migrations self.cat_sql.
		
		If the catalogue version, stored in sch_chameleon.v_version is different from the value stored in self.cat_version then the method upgrade_service_schema() is executed.
		
	"""
	def __init__(self, global_config, table_metadata, table_file, logger, sql_dir='sql/'):
		self.sleep_on_reindex = global_config.sleep_on_reindex
		self.reindex_app_names = global_config.reindex_app_names
		self.batch_retention = global_config.batch_retention
		self.type_override = global_config.type_override
		self.logger = logger
		self.sql_dir = sql_dir
		self.idx_sequence = 0
		self.pg_conn = pg_connection(global_config)
		self.pg_conn.connect_db()
		self.table_metadata = table_metadata
		self.table_file = table_file
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
		self.table_ddl = {}
		self.idx_ddl = {}
		self.type_ddl = {}
		self.pg_charset = self.pg_conn.pg_charset
		self.cat_version = '1.7'
		self.cat_sql = [
			{'version':'base','script': 'create_schema.sql'}, 
			{'version':'0.1','script': 'upgrade/cat_0.1.sql'}, 
			{'version':'0.2','script': 'upgrade/cat_0.2.sql'}, 
			{'version':'0.3','script': 'upgrade/cat_0.3.sql'}, 
			{'version':'0.4','script': 'upgrade/cat_0.4.sql'}, 
			{'version':'0.5','script': 'upgrade/cat_0.5.sql'}, 
			{'version':'0.6','script': 'upgrade/cat_0.6.sql'}, 
			{'version':'0.7','script': 'upgrade/cat_0.7.sql'}, 
			{'version':'0.8','script': 'upgrade/cat_0.8.sql'}, 
			{'version':'0.9','script': 'upgrade/cat_0.9.sql'}, 
			{'version':'1.0','script': 'upgrade/cat_1.0.sql'}, 
			{'version':'1.1','script': 'upgrade/cat_1.1.sql'}, 
			{'version':'1.2','script': 'upgrade/cat_1.2.sql'}, 
			{'version':'1.3','script': 'upgrade/cat_1.3.sql'}, 
			{'version':'1.4','script': 'upgrade/cat_1.4.sql'},
			{'version':'1.5','script': 'upgrade/cat_1.5.sql'},
			{'version':'1.6','script': 'upgrade/cat_1.6.sql'},
			{'version':'1.7','script': 'upgrade/cat_1.7.sql'},
		]
		cat_version=self.get_schema_version()
		num_schema=(self.check_service_schema())[0]
		if cat_version!=self.cat_version and int(num_schema)>0:
			self.upgrade_service_schema()
		self.table_limit = ['*']
		self.master_status = None
	
	def set_application_name(self, action="", conn_type='main'):
		"""
			The method sets the application name in the replica using the variable self.pg_conn.global_conf.source_name,
			Making simpler to find the replication processes. If the source name is not set then a generic PGCHAMELEON name is used.
		"""
		if self.pg_conn.global_conf.source_name:
			app_name = "[PGCH] - source: %s, action: %s" % (self.pg_conn.global_conf.source_name, action)
		else:
			app_name = "[PGCH]"
		sql_app_name="""SET application_name=%s; """
		if conn_type == 'main':
			self.pg_conn.pgsql_cur.execute(sql_app_name, (app_name , ))
		elif conn_type == 'replay':
			self.pg_conn.pgsql_cur_replay.execute(sql_app_name, (app_name , ))
	
		
	def add_source(self, source_name, dest_schema):
		"""
			The method add a new source in the replica catalogue. 
			If the source name is already present an error message is emitted without further actions.
			:param source_name: The source name stored in the configuration parameter source_name.
			:param source_schema: The source schema on mysql. The field is not used except when migrating the catalogue to the newer version 2.0.x.
		"""
		sql_source = """
			SELECT 
				count(i_id_source)
			FROM 
				sch_chameleon.t_sources 
			WHERE 
				t_source=%s
				;
			"""
		self.pg_conn.pgsql_cur.execute(sql_source, (source_name, ))
		source_data = self.pg_conn.pgsql_cur.fetchone()
		cnt_source = source_data[0]
		if cnt_source == 0:
			sql_add = """
				INSERT INTO sch_chameleon.t_sources 
					( 
						t_source,
						t_dest_schema,
						t_source_schema
					) 
				VALUES 
					(
						%s,
						%s,
						%s
					)
				RETURNING 
					i_id_source
				; 
			"""
			self.pg_conn.pgsql_cur.execute(sql_add, (source_name, dest_schema, self.source_schema ))
			source_add = self.pg_conn.pgsql_cur.fetchone()
			sql_update = """
				UPDATE sch_chameleon.t_sources
					SET v_log_table=ARRAY[
						't_log_replica_1_src_%s',
						't_log_replica_2_src_%s'
					]
				WHERE i_id_source=%s
				;
			"""
			self.pg_conn.pgsql_cur.execute(sql_update,  (source_add[0],source_add[0], source_add[0] ))
			
			sql_parts = """SELECT sch_chameleon.fn_refresh_parts() ;"""
			self.pg_conn.pgsql_cur.execute(sql_parts)
			
		else:
			print("Source %s already registered." % source_name)
		sys.exit()
	
	def get_source_status(self, source_name):
		"""
			Gets the source status usin the source name.
			Possible values are:

			ready : the source is registered but the init_replica is not yet done.
			
			initialising: init_replica is initialising
			
			initialised: init_replica finished and the replica process is ready to start
			
			stopped: the replica process is stopped
			
			running: the replica process is running
				
			:param source_name: The source name stored in the configuration parameter source_name.
			:type source_name: string
			:return: source_status extracted from PostgreSQL
			:rtype: string
		"""
		sql_source = """
					SELECT 
						enm_status
					FROM 
						sch_chameleon.t_sources 
					WHERE 
						t_source=%s
				;
			"""
		self.pg_conn.pgsql_cur.execute(sql_source, (source_name, ))
		source_data = self.pg_conn.pgsql_cur.fetchone()
		if source_data:
			source_status = source_data[0]
		else:
			source_status = 'Not registered'
		return source_status
		
	def drop_source(self, source_name):
		"""
			Drops the source from the replication catalogue discarding any replica reference.
			:param source_name: The source name stored in the configuration parameter source_name.
		"""
		sql_delete = """ DELETE FROM sch_chameleon.t_sources 
					WHERE  t_source=%s
					RETURNING v_log_table
					; """
		self.pg_conn.pgsql_cur.execute(sql_delete, (source_name, ))
		source_drop = self.pg_conn.pgsql_cur.fetchone()
		for log_table in source_drop[0]:
			sql_drop = """DROP TABLE sch_chameleon."%s"; """ % (log_table)
			self.pg_conn.pgsql_cur.execute(sql_drop)
	
		
	
	def set_source_id(self, source_status):
		"""
			Sets the source status for the source_name and sets the two class attributes i_id_source and dest_schema.
			
			:param source_status: The source status to be set.
			
		"""
		sql_source = """
			UPDATE sch_chameleon.t_sources
			SET
				enm_status=%s,
				t_source_schema=%s
			WHERE
				t_source=%s
			RETURNING i_id_source,t_dest_schema
				;
			"""
		source_name = self.pg_conn.global_conf.source_name
		self.pg_conn.pgsql_cur.execute(sql_source, (source_status, self.source_schema,  source_name))
		source_data = self.pg_conn.pgsql_cur.fetchone()
		try:
			self.i_id_source = source_data[0]
			self.dest_schema = source_data[1]
			self.source_name = source_name
		except:
			print("Source %s is not registered." % source_name)
			sys.exit()
	
			
	def clean_batch_data(self):
		"""
			Removes the replica batch data for the given source id.
			The method is used to cleanup incomplete batch data in case of crash or replica's unclean restart
		"""
		sql_delete = """
			DELETE FROM sch_chameleon.t_replica_batch 
			WHERE i_id_source=%s;
		"""
		self.pg_conn.pgsql_cur.execute(sql_delete, (self.i_id_source, ))
		
		
	def create_schema(self):
		"""
			The method drops and creates the destination schema.
			It also set the search_path for the cursor to the destination schema.
		"""
		sql_drop="""DROP SCHEMA IF EXISTS "%s" CASCADE;""" % self.dest_schema
		sql_create="""CREATE SCHEMA IF NOT EXISTS "%s";""" % self.dest_schema
		self.pg_conn.pgsql_cur.execute(sql_drop)
		self.pg_conn.pgsql_cur.execute(sql_create)
		self.set_search_path()
	
	def store_table(self, table_name):
		"""
			The method saves the table name along with the primary key definition in the table t_replica_tables.
			This is required in order to let the replay procedure which primary key to use replaying the update and delete.
			If the table is without primary key is not stored. 
			A table without primary key is copied and the indices are create like any other table. 
			However the replica doesn't work for the tables without primary key.
			
			If the class variable master status is set then the master's coordinates are saved along with the table.
			This happens in general when a table is added to the replica or the data is refreshed with sync_tables.
			
			:param table_name: the table name to store in the table  t_replica_tables
		"""
		if self.master_status:
			master_data = self.master_status[0]
			binlog_file = master_data["File"]
			binlog_pos = master_data["Position"]
		else:
			binlog_file = None
			binlog_pos = None
		table_data=self.table_metadata[table_name]
		table_no_pk = True
		for index in table_data["indices"]:
			if index["index_name"]=="PRIMARY":
				table_no_pk = False
				sql_insert=""" 
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
							ARRAY[%s],
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
				self.pg_conn.pgsql_cur.execute(sql_insert, (
					self.i_id_source, 
					table_name, 
					self.dest_schema, 
					index["index_columns"].strip(), 
					binlog_file, 
					binlog_pos
					)
				)
		if table_no_pk:
			self.logger.warning("Missing primary key. The table %s will not be replicated." % (table_name,))
			sql_delete = """
				DELETE FROM sch_chameleon.t_replica_tables
				WHERE
						i_id_source=%s
					AND	v_table_name=%s
					AND	v_schema_name=%s
				;
			"""
			self.pg_conn.pgsql_cur.execute(sql_delete, (
				self.i_id_source, 
				table_name, 
				self.dest_schema)
				)
		
	def unregister_table(self, table_name):
		"""
			This method is used when a table have the primary key dropped on MySQL. 
			The table name is removed from the replicatoin catalogue and the table is renamed.
			This way any dependency (e.g. views, functions) to the table is preserved but the replica is stopped.

			:param table_name: the table name to remove from t_replica_tables
		"""
		self.logger.info("unregistering table %s from the replica catalog" % (table_name,))
		sql_delete=""" DELETE FROM sch_chameleon.t_replica_tables 
									WHERE
											v_table_name=%s
										AND	v_schema_name=%s
								RETURNING i_id_table
								;
						"""
		self.pg_conn.pgsql_cur.execute(sql_delete, (table_name, self.dest_schema))	
		removed_id=self.pg_conn.pgsql_cur.fetchone()
		table_id=removed_id[0]
		self.logger.info("renaming table %s to %s_%s" % (table_name, table_name, table_id))
		sql_rename="""ALTER TABLE IF EXISTS "%s"."%s" rename to "%s_%s"; """ % (self.dest_schema, table_name, table_name, table_id)
		self.logger.debug(sql_rename)
		self.pg_conn.pgsql_cur.execute(sql_rename)	
	
	
	def set_search_path(self):
		"""
			The method sets the search path for the connection.
		"""
		sql_path=""" SET search_path="%s";""" % (self.dest_schema, )
		self.pg_conn.pgsql_cur.execute(sql_path)
		
	
	def drop_tables(self):
		"""
			The method drops the tables present in the table_ddl
		"""
		self.set_search_path()
		for table in self.table_ddl:
			self.logger.debug("dropping table %s " % (table, ))
			sql_drop = """DROP TABLE IF EXISTS "%s"  CASCADE;""" % (table, )
			self.pg_conn.pgsql_cur.execute(sql_drop)
			
	
	def create_tables(self):
		"""
			The method loops trough the list table_ddl and executes the creation scripts.
			No index is created in this method
		"""
		self.set_search_path()
		for table in self.table_ddl:
			self.logger.debug("creating table %s " % (table, ))
			try:
				ddl_enum=self.type_ddl[table]
				for sql_type in ddl_enum:
					self.pg_conn.pgsql_cur.execute(sql_type)
			except psycopg2.Error as e:
				self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
				self.logger.error(sql_type)
				
			sql_create=self.table_ddl[table]
			try:
				self.pg_conn.pgsql_cur.execute(sql_create)
			except psycopg2.Error as e:
				self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
				self.logger.error(sql_create)
				
			self.store_table(table)

	def create_indices(self):
		"""
			The method creates the indices using the DDL stored in the class list self.idx_ddl.
		"""
		self.logger.info("creating the indices")
		for table in self.idx_ddl:
			idx_ddl= self.idx_ddl[table]
			self.logger.debug("processing table %s" % (table, ))
			for sql_idx in idx_ddl:
				self.pg_conn.pgsql_cur.execute(sql_idx)
				
	
	def copy_data(self, table,  csv_file,  my_tables={}):
		"""
			The method copy the data into postgresql using psycopg2's copy_expert.
			The csv_file is a file like object which can be either a  csv file or a string io object, accordingly with the 
			configuration parameter copy_mode.
			
			:param table: the table name, used to get the table's metadata out of my_tables
			:param csv_file: file like object with the table's data stored in CSV format
			:param my_tables: table's metadata dictionary 
		"""
		column_copy=[]
		for column in my_tables[table]["columns"]:
			column_copy.append('"'+column["column_name"]+'"')
		sql_copy="COPY "+'"'+self.dest_schema+'"'+"."+'"'+table+'"'+" ("+','.join(column_copy)+") FROM STDIN WITH NULL 'NULL' CSV QUOTE '\"' DELIMITER',' ESCAPE '\"' ; "
		self.pg_conn.pgsql_cur.copy_expert(sql_copy,csv_file)
		
	def insert_data(self, table,  insert_data,  my_tables={}):
		"""
			The method is a fallback procedure for when the copy method fails.
			The procedure performs a row by row insert, very slow but capable to skip the rows with problematic data (e.g. enchoding issues).
			
			:param table: the table name, used to get the table's metadata out of my_tables
			:param csv_file: file like object with the table's data stored in CSV format
			:param my_tables: table's metadata dictionary 
		"""
		column_copy=[]
		column_marker=[]
		
		for column in my_tables[table]["columns"]:
			column_copy.append('"'+column["column_name"]+'"')
			column_marker.append('%s')
		sql_head="INSERT INTO "+'"'+self.dest_schema+'"'+"."+'"'+table+'"'+" ("+','.join(column_copy)+") VALUES ("+','.join(column_marker)+");"
		for data_row in insert_data:
			column_values=[]
			for column in my_tables[table]["columns"]:
				column_values.append(data_row[column["column_name"]])
			try:
				self.pg_conn.pgsql_cur.execute(sql_head,column_values)	
			except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(self.pg_conn.pgsql_cur.mogrify(sql_head,column_values))
			except ValueError:
				self.logger.warning("character mismatch when inserting the data, trying to cleanup the row data")
				column_values = [str(item).replace("\x00", "") for item in column_values]
				try:	
					self.pg_conn.pgsql_cur.execute(sql_head,column_values)	
				except:
					self.logger.error("error when inserting the row, skipping the row")
				
			except:
				self.logger.error("unexpected error when processing the row")
				self.logger.error(" - > Table: %s" % table)
							
	
	def build_idx_ddl(self):
		""" 
			The method loops over the list l_pkeys and builds a new list with the statements for pkeys 
		"""
		for table_name in self.table_metadata:
			table=self.table_metadata[table_name]
			
			table_name=table["name"]
			indices=table["indices"]
			table_idx=[]
			for index in indices:
				table_timestamp = str(int(time.time()))
				indx=index["index_name"]
				index_columns=index["index_columns"]
				non_unique=index["non_unique"]
				if indx=='PRIMARY':
					pkey_name="pk_"+table_name[0:10]+"_"+table_timestamp+"_"+str(self.idx_sequence)
					pkey_def='ALTER TABLE "'+table_name+'" ADD CONSTRAINT "'+pkey_name+'" PRIMARY KEY ('+index_columns+') ;'
					table_idx.append(pkey_def)
				else:
					if non_unique==0:
						unique_key='UNIQUE'
					else:
						unique_key=''
					index_name='"idx_'+indx[0:10]+table_name[0:10]+"_"+table_timestamp+"_"+str(self.idx_sequence)+'"'
					idx_def='CREATE '+unique_key+' INDEX '+ index_name+' ON "'+table_name+'" ('+index_columns+');'
					table_idx.append(idx_def)
				self.idx_sequence+=1
					
			self.idx_ddl[table_name]=table_idx

	def build_tab_ddl(self):
		""" 
			The method iterates over the list l_tables and builds a new list with the statements for tables
		"""
		if self.table_limit[0] != '*' :
			table_metadata = {}
			for tab in self.table_limit:
				try:
					table_metadata[tab] = self.table_metadata[tab]
				except:
					pass
		else:
			table_metadata = self.table_metadata
		
		for table_name in table_metadata:
			table=self.table_metadata[table_name]
			columns=table["columns"]
			
			ddl_head="CREATE TABLE "+'"'+table["name"]+'" ('
			ddl_tail=");"
			ddl_columns=[]
			ddl_enum=[]
			for column in columns:
				if column["is_nullable"]=="NO":
					col_is_null="NOT NULL"
				else:
					col_is_null="NULL"
				column_type = self.get_data_type(column, table)
				if column_type=="enum":
					enum_type="enum_"+table["name"]+"_"+column["column_name"]
					sql_drop_enum='DROP TYPE IF EXISTS '+enum_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+enum_type+" AS ENUM "+column["enum_list"]+";"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
					column_type=enum_type
				if column_type=="character varying" or column_type=="character":
					column_type=column_type+"("+str(column["character_maximum_length"])+")"
				if column_type=='numeric':
					column_type=column_type+"("+str(column["numeric_precision"])+","+str(column["numeric_scale"])+")"
				if column["extra"]=="auto_increment":
					column_type="bigserial"
				ddl_columns.append('"'+column["column_name"]+'" '+column_type+" "+col_is_null )
			def_columns=str(',').join(ddl_columns)
			self.type_ddl[table["name"]]=ddl_enum
			self.table_ddl[table["name"]]=ddl_head+def_columns+ddl_tail
	

	def get_data_type(self, column, table):
		""" 
			The method determines whether the specified type has to be overridden or not.
			
			:param column: the column dictionary extracted from the information_schema or build in the sql_parser class
			:param table: the table name 
			:return: the postgresql converted column type
			:rtype: string
		"""
		try:
			type_override = self.type_override[column["column_type"]]
			override_to = type_override["override_to"]
			override_tables = type_override["override_tables"]
			
			if override_tables[0] == '*' or table in override_tables:
				column_type = override_to
			else:
				column_type = self.type_dictionary[column["data_type"]]
		except:
			column_type = self.type_dictionary[column["data_type"]]
		return column_type
	
	def get_schema_version(self):
		"""
			The method gets the service schema version querying the view sch_chameleon.v_version.
			The try-except is used in order to get a valid value "base" if the view is missing.
			This happens only if the schema upgrade is performed from very early pg_chamelon's versions.
			
			:return: the catalogg version
			:rtype: string
		"""
		sql_check="""
			SELECT 
				t_version
			FROM 
				sch_chameleon.v_version 
			;
		"""
		try:
			self.pg_conn.pgsql_cur.execute(sql_check)
			value_check=self.pg_conn.pgsql_cur.fetchone()
			cat_version=value_check[0]
		except:
			cat_version='base'
		return cat_version
		
	def upgrade_service_schema(self):
		"""
			The method upgrades the service schema to the latest version using the upgrade files if required.
			
			The method uses the install_script flag to determine whether an upgrade file should be applied.
			The variable cat_version stores the schema version. Each element in the class list cat_sql 
			stores the scripts in the upgrade directory along with the catalogue version associated with the install script.
			
			If the current catalogue version stored in cat_version is equal to the script version the install is skipped but the variable
			install_script is set to true. This way any following install script is executed to upgrade the catalogue to the higher version.
			
			The hardcoded 0.7 version is required because that version introduced the multi source feature.
			As initially the destination schema were not stored in the migration catalogue, the post migration update 
			is required to save this information in the replica catalogue.
		"""
		
		self.logger.info("Upgrading the service schema")
		install_script=False
		cat_version=self.get_schema_version()
			
		for install in self.cat_sql:
				script_ver=install["version"]
				script_schema=install["script"]
				self.logger.info("script schema %s, detected schema version %s - install_script:%s " % (script_ver, cat_version, install_script))
				if install_script==True:
					sql_view="""
					CREATE OR REPLACE VIEW sch_chameleon.v_version 
						AS
							SELECT %s::TEXT t_version
					;"""
					self.logger.info("Installing file version %s" % (script_ver, ))
					file_schema=open(self.sql_dir+script_schema, 'rb')
					sql_schema=file_schema.read()
					file_schema.close()
					self.pg_conn.pgsql_cur.execute(sql_schema)
					self.pg_conn.pgsql_cur.execute(sql_view, (script_ver, ))
					
					if script_ver=='0.7':
						sql_update="""
							UPDATE sch_chameleon.t_sources
							SET
								t_dest_schema=%s 
							WHERE i_id_source=(
												SELECT 
													i_id_source
												FROM
													sch_chameleon.t_sources
												WHERE
													t_source='default'
													AND t_dest_schema='default'
											)
							;
						"""
						self.pg_conn.pgsql_cur.execute(sql_update, (self.dest_schema, ))
				
				
				if script_ver==cat_version and not install_script:
					self.logger.info("enabling install script")
					install_script=True
		
	def check_service_schema(self):
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
			
		self.pg_conn.pgsql_cur.execute(sql_check)
		num_schema=self.pg_conn.pgsql_cur.fetchone()
		return num_schema
	
	def create_service_schema(self):
		"""
			The method installs the service replica service schema sch_chameleon.
		"""
		
		num_schema=self.check_service_schema()
		if num_schema[0]==0:
			for install in self.cat_sql:
				script_ver=install["version"]
				script_schema=install["script"]
				if script_ver=='base':
					self.logger.info("Installing service schema %s" % (script_ver, ))
					file_schema=open(self.sql_dir+script_schema, 'rb')
					sql_schema=file_schema.read()
					file_schema.close()
					self.pg_conn.pgsql_cur.execute(sql_schema)
		else:
			self.logger.error("The service schema is already created")
			
	def get_status(self):
		"""
			The metod lists the sources with the running status and the eventual lag 
			
			:return: psycopg2 fetchall results 
			:rtype: psycopg2 tuple
		"""
		sql_status="""
			SELECT
				t_source,
				t_dest_schema,
				enm_status,
				date_trunc('seconds',now())-ts_last_received lag,
				ts_last_received,
				ts_last_received-ts_last_replay,
				ts_last_replay,
				coalesce(t_source_schema,'')
			FROM 
				sch_chameleon.t_sources
			ORDER BY 
				t_source
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_status)
		results = self.pg_conn.pgsql_cur.fetchall()
		return results
		
	def drop_service_schema(self):
		"""
			The method removes the service schema discarding all the replica references.
			The replicated tables are kept in place though.
		"""
		file_schema=open(self.sql_dir+"drop_schema.sql", 'rb')
		sql_schema=file_schema.read()
		file_schema.close()
		self.pg_conn.pgsql_cur.execute(sql_schema)
	
	def save_master_status(self, master_status, cleanup=False):
		"""
			This method saves the master data determining which log table should be used in the next batch.
			
			The method performs also a cleanup for the logged events the cleanup parameter is true.
			
			:param master_status: the master data with the binlogfile and the log position
			:param cleanup: if true cleans the not replayed batches. This is useful when resyncing a replica.
		"""
		next_batch_id=None
		master_data = master_status[0]
		binlog_name = master_data["File"]
		binlog_position = master_data["Position"]
		try:
			event_time = master_data["Time"]
		except:
			event_time = None
		
		sql_master="""
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
						
		sql_event="""
			UPDATE sch_chameleon.t_sources 
			SET 
				ts_last_received=to_timestamp(%s),
				v_log_table=ARRAY[v_log_table[2],v_log_table[1]]
				
			WHERE 
				i_id_source=%s
			RETURNING 
				v_log_table[1],
				ts_last_received
			; 
		"""
		
		
		
		
		try:
			if cleanup:
				self.logger.info("cleaning not replayed batches for source %s", self.i_id_source)
				sql_cleanup=""" DELETE FROM sch_chameleon.t_replica_batch WHERE i_id_source=%s AND NOT b_replayed; """
				self.pg_conn.pgsql_cur.execute(sql_cleanup, (self.i_id_source, ))
			self.pg_conn.pgsql_cur.execute(sql_master, (self.i_id_source, binlog_name, binlog_position))
			results=self.pg_conn.pgsql_cur.fetchone()
			next_batch_id=results[0]
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(self.pg_conn.pgsql_cur.mogrify(sql_master, (self.i_id_source, binlog_name, binlog_position)))
		try:
			self.pg_conn.pgsql_cur.execute(sql_event, (event_time, self.i_id_source, ))
			results = self.pg_conn.pgsql_cur.fetchone()
			log_table_name = results[0]
			db_event_time = results[1]
			self.logger.info("Saved master data for source: %s" %(self.source_name, ) )
			self.logger.debug("Binlog file: %s" % (binlog_name, ))
			self.logger.debug("Binlog position:%s" % (binlog_position, ))
			self.logger.debug("Last event: %s" % (db_event_time, ))
			self.logger.debug("Next log table name: %s" % ( log_table_name, ))
		
		
			
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.pg_conn.pgsql_cur.mogrify(sql_event, (event_time, self.i_id_source, ))
		
		return next_batch_id
		
	def get_batch_data(self):
		"""
			The method updates the batch status to started for the given source_id and returns the 
			batch informations.
			
			:return: psycopg2 fetchall results without any manipulation
			:rtype: psycopg2 tuple
			
		"""
		sql_batch="""
			WITH t_created AS
				(
					SELECT 
						max(ts_created) AS ts_created
					FROM 
						sch_chameleon.t_replica_batch  
					WHERE 
							NOT b_processed
						AND	NOT b_replayed
						AND	i_id_source=%s
				)
			UPDATE sch_chameleon.t_replica_batch
			SET 
				b_started=True
			FROM 
				t_created
			WHERE
					t_replica_batch.ts_created=t_created.ts_created
				AND	i_id_source=%s
			RETURNING
				i_id_batch,
				t_binlog_name,
				i_binlog_position,
				(SELECT v_log_table[1] from sch_chameleon.t_sources WHERE i_id_source=%s) as v_log_table
				
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_batch, (self.i_id_source, self.i_id_source, self.i_id_source, ))
		return self.pg_conn.pgsql_cur.fetchall()
	
	def insert_batch(self,group_insert):
		"""
			Fallback method for the batch insert. Each row event is processed
			individually and any problematic row is discarded into the table t_discarded_rows.
			The row is encoded in base64 in order to prevent any encoding or type issue.
			
			:param group_insert: the event data built in mysql_engine
		"""
		
		self.logger.debug("starting insert loop")
		for row_data in group_insert:
			global_data = row_data["global_data"]
			event_data = row_data["event_data"]
			event_update = row_data["event_update"]
			log_table = global_data["log_table"]
			event_time = global_data["event_time"]
			sql_insert="""
				INSERT INTO sch_chameleon."""+log_table+"""
					(
						i_id_batch, 
						v_table_name, 
						v_schema_name, 
						enm_binlog_event, 
						t_binlog_name, 
						i_binlog_position, 
						jsb_event_data,
						jsb_event_update,
						i_my_event_time
					)
					VALUES 
						(
							%s,
							%s,
							%s,
							%s,
							%s,
							%s,
							%s,
							%s,
							%s
						)
				;						
			"""
			try:
				self.pg_conn.pgsql_cur.execute(sql_insert,(
						global_data["batch_id"], 
						global_data["table"],  
						global_data["schema"], 
						global_data["action"], 
						global_data["binlog"], 
						global_data["logpos"], 
						json.dumps(event_data, cls=pg_encoder), 
						json.dumps(event_update, cls=pg_encoder), 
						event_time
					)
				)
			except psycopg2.Error as e:
				if e.pgcode == "22P05":
					self.logger.warning("%s - %s. Trying to cleanup the row" % (e.pgcode, e.pgerror))
					event_data = {key: str(value).replace("\x00", "") for key, value in event_data.items()}
					event_update = {key: str(value).replace("\x00", "") for key, value in event_update.items()}
					try:
						self.pg_conn.pgsql_cur.execute(sql_insert,(
								global_data["batch_id"], 
								global_data["table"],  
								global_data["schema"], 
								global_data["action"], 
								global_data["binlog"], 
								global_data["logpos"], 
								json.dumps(event_data, cls=pg_encoder), 
								json.dumps(event_update, cls=pg_encoder), 
								event_time
							)
						)
					except:
						self.logger.error("Cleanup unsuccessful. Saving the discarded row")
						self.save_discarded_row(row_data,global_data["batch_id"])
						
			except:
				self.logger.error("error when storing event data. saving the discarded row")
				self.save_discarded_row(row_data,global_data["batch_id"])
	
	def save_discarded_row(self,row_data,batch_id):
		"""
			The method saves the discarded row in the table t_discarded_row along with the id_batch.
			The row is encoded in base64 as the t_row_data is a text field.
			
			:param row_data: the row data dictionary
			:param batch_id: the id batch where the row belongs
		"""
		str_data = '%s' %(row_data, )
		hex_row = binascii.hexlify(str_data.encode())
		sql_save="""
			INSERT INTO sch_chameleon.t_discarded_rows
				(
					i_id_batch, 
					t_row_data
				)
			VALUES 
				(
					%s,
					%s
				);
		"""
		self.pg_conn.pgsql_cur.execute(sql_save,(batch_id,hex_row))
	
	def write_batch(self, group_insert):
		"""
			Main method for adding the batch data in the log tables. 
			The row data from group_insert are mogrified in CSV format and stored in
			the string like object csv_file.
			
			psycopg2's copy expert is used to store the event data in PostgreSQL.
			
			Should any error occur the procedure fallsback to insert_batch.
			
			:param group_insert: the event data built in mysql_engine
		"""
		csv_file=io.StringIO()
		self.set_application_name("writing batch")
		insert_list=[]
		for row_data in group_insert:
			global_data=row_data["global_data"]
			event_data=row_data["event_data"]
			event_update=row_data["event_update"]
			log_table=global_data["log_table"]
			insert_list.append(self.pg_conn.pgsql_cur.mogrify("%s,%s,%s,%s,%s,%s,%s,%s,%s" ,  (
						global_data["batch_id"], 
						global_data["table"],  
						self.dest_schema, 
						global_data["action"], 
						global_data["binlog"], 
						global_data["logpos"], 
						json.dumps(event_data, cls=pg_encoder), 
						json.dumps(event_update, cls=pg_encoder), 
						global_data["event_time"], 
						
					)
				)
			)
											
		csv_data=b"\n".join(insert_list ).decode()
		csv_file.write(csv_data)
		csv_file.seek(0)
		try:
			
			sql_copy="""
				COPY "sch_chameleon"."""+log_table+""" 
					(
						i_id_batch, 
						v_table_name, 
						v_schema_name, 
						enm_binlog_event, 
						t_binlog_name, 
						i_binlog_position, 
						jsb_event_data,
						jsb_event_update,
						i_my_event_time
					) 
				FROM 
					STDIN 
					WITH NULL 'NULL' 
					CSV QUOTE '''' 
					DELIMITER ',' 
					ESCAPE '''' 
				;
			"""
			self.pg_conn.pgsql_cur.copy_expert(sql_copy,csv_file)
		except psycopg2.Error as e:
			self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
			self.logger.error(csv_data)
			self.logger.error("fallback to inserts")
			self.insert_batch(group_insert)
		self.set_application_name("idle")
			
		
	def set_batch_processed(self, id_batch):
		"""
			The method updates the flag b_processed and sets the processed timestamp for the given batch id.
			The event ids are aggregated into the table t_batch_events used by the replay function.
			
			:param id_batch: the id batch to set as processed
		"""
		self.logger.debug("updating batch %s to processed" % (id_batch, ))
		sql_update=""" 
			UPDATE sch_chameleon.t_replica_batch
				SET
					b_processed=True,
					ts_processed=now()
			WHERE
				i_id_batch=%s
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_update, (id_batch, ))
		self.logger.debug("collecting events id for batch %s " % (id_batch, ))
		sql_collect_events = """
			INSERT INTO
				sch_chameleon.t_batch_events
				(
					i_id_batch,
					i_id_event
				)
			SELECT
				i_id_batch,
				array_agg(i_id_event)
			FROM
			(
				SELECT 
					i_id_batch,
					i_id_event,
					ts_event_datetime
				FROM 
					sch_chameleon.t_log_replica 
				WHERE i_id_batch=%s
				ORDER BY ts_event_datetime
			) t_event
			GROUP BY
					i_id_batch
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_collect_events, (id_batch, ))
		
	def process_batch(self, replica_batch_size):
		"""
			The method calls the function fn_process_batch with the parameters batch size and the id_source.
			
			The plpgsql function returns true if there are still rows to process. When all rows are replayed 
			the method exits.
			
			:param replica_batch_size: the max rows to process in a single function call. 
			
			
		"""
		batch_loop=True
		sql_process="""SELECT sch_chameleon.fn_process_batch(%s,%s);"""
		self.logger.info("Replaying batch for source %s replay size %s rows" % ( self.source_name, replica_batch_size))
		
		while batch_loop:
			try:
				self.set_application_name("replay batch", "replay")
			except:
				self.pg_conn.connect_replay_db()
				
			self.pg_conn.pgsql_cur_replay.execute(sql_process, (replica_batch_size, self.i_id_source))
			batch_result=self.pg_conn.pgsql_cur_replay.fetchone()
			batch_loop=batch_result[0]
			
			if batch_loop:
				self.logger.info("Still working on batch for source  %s replay size %s rows" % (self.source_name, replica_batch_size ))
			else:
				self.logger.info("Batch replay for source %s is complete" % (self.source_name))
		
		self.set_application_name("cleanup batch", "replay")
		self.logger.debug("Cleanup for replayed batches older than %s for source %s" % (self.batch_retention,  self.source_name))
		sql_cleanup="""
			DELETE FROM 
				sch_chameleon.t_replica_batch
			WHERE
					b_started
				AND b_processed
				AND b_replayed
				AND now()-ts_replayed>%s::interval
				AND i_id_source=%s
			;
		"""
		self.pg_conn.pgsql_cur_replay.execute(sql_cleanup, (self.batch_retention, self.i_id_source ))
		self.set_application_name("idle", "replay")

	def add_foreign_keys(self, source_name, fk_metadata):
		"""
			the method  creates the foreign keys extracted from the mysql catalog
			the keys are created initially as invalid then validated. If an error happens
			is displayed on the log destination
			
			:param source_name: the source name, required to determine the destination schema
			:param fk_metadata: the foreign keys metadata extracted from mysql's information schema
		"""
		fk_list = []
		sql_schema="""
			SELECT
				t_dest_schema 
			FROM
				sch_chameleon.t_sources 
			WHERE
				t_source=%s
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_schema, (source_name, ))
		dschema=self.pg_conn.pgsql_cur.fetchone()
		destination_schema = dschema[0]
		self.logger.info("creating the not validated foreign keys in schema %s" % destination_schema)
		fk_counter = 0
		for foreign_key in fk_metadata:
			table_name = foreign_key["table_name"]
			fk_name = foreign_key["constraint_name"][0:20] + "_" + str(fk_counter)
			fk_cols = foreign_key["fk_cols"]
			referenced_table_name = foreign_key["referenced_table_name"]
			ref_columns = foreign_key["ref_columns"]
			fk_list.append({'fkey_name':fk_name, 'table_name':table_name})
			sql_fkey = ("""ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s"."%s" (%s) NOT VALID;""" % 
				(
					destination_schema, 
					table_name, 
					fk_name, 
					fk_cols, 
					destination_schema, 
					referenced_table_name, 
					ref_columns
				)
				)
			fk_counter+=1
			self.logger.debug("creating %s on %s" % (fk_name, table_name))
			try:
				self.pg_conn.pgsql_cur.execute(sql_fkey)
			except psycopg2.Error as e:
					self.logger.error("could not create the foreign key %s on table %s" % (fk_name, table_name))
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error("STATEMENT: %s " % (self.pg_conn.pgsql_cur.mogrify(sql_fkey)))
					
		self.logger.info("validating the foreign keys in schema %s" % destination_schema)
		for fkey in fk_list:
			sql_validate = 'ALTER TABLE "%s"."%s" VALIDATE CONSTRAINT "%s";' % (destination_schema, fkey["table_name"], fkey["fkey_name"])
			try:
				self.pg_conn.pgsql_cur.execute(sql_validate)
			except psycopg2.Error as e:
					self.logger.error("could not validate the foreign key %s on table %s" % (fkey["table_name"], fkey["fkey_name"]))
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error("STATEMENT: %s " % (self.pg_conn.pgsql_cur.mogrify(sql_validate)))
		
	def reset_sequences(self, source_name):
		""" 
			the method resets the sequences to the max value available the associated table 
			:param source_name: the source name, required to determine the destination schema
			
		"""
		sql_schema="""
			SELECT
				t_dest_schema 
			FROM
				sch_chameleon.t_sources 
			WHERE
				t_source=%s
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_schema, (source_name, ))
		dschema=self.pg_conn.pgsql_cur.fetchone()
		destination_schema = dschema[0]
		self.logger.info("resetting the sequences in schema %s" % destination_schema)
		sql_gen_reset=""" 
			SELECT 
				format('SELECT setval(%%L::regclass,(select max(%%I) FROM %%I.%%I));',
					replace(replace(column_default,'nextval(''',''),'''::regclass)',''),
					column_name,
					table_schema,
					table_name
				),
				replace(replace(column_default,'nextval(''',''),'''::regclass)','') as  seq_name
			FROM 
					information_schema.columns
			WHERE 
						table_schema=%s
					AND	column_default like 'nextval%%'
		;"""
		self.pg_conn.pgsql_cur.execute(sql_gen_reset, (destination_schema, ))
		results=self.pg_conn.pgsql_cur.fetchall()
		try:
			for statement in results:
				self.logger.info("resetting the sequence  %s" % statement[1])
				self.pg_conn.pgsql_cur.execute(statement[0])
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(statement)
		except:
			pass
	
	def  generate_default_statements(self, table, column, create_column=None):
		"""
			The method gets the default value associated with the table and column removing the cast.
			:param table: The table name
			:param table: The column name
			:return: the statements for dropping and creating default value on the affected table
			:rtype: dictionary
		"""
		if not create_column:
			create_column = column
		
		regclass = """ "%s"."%s" """ %(self.dest_schema, table)
		sql_def_val = """
			SELECT 
				(
					SELECT 
						split_part(substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128),'::',1)
					FROM 
						pg_catalog.pg_attrdef d
					WHERE 
							d.adrelid = a.attrelid 
						AND d.adnum = a.attnum 
						AND a.atthasdef
				) as default_value
				FROM 
					pg_catalog.pg_attribute a
				WHERE 
						a.attrelid = %s::regclass 
					AND a.attname=%s 
					AND NOT a.attisdropped
			;

		"""
		self.pg_conn.pgsql_cur.execute(sql_def_val, (regclass, column ))
		default_value = self.pg_conn.pgsql_cur.fetchone()
		if default_value[0]:
			query_drop_default = """ ALTER TABLE  "%s" ALTER COLUMN "%s" DROP DEFAULT;""" % (table, column)
			query_add_default = """ ALTER TABLE  "%s" ALTER COLUMN "%s" SET DEFAULT %s ; """ % (table, create_column, default_value[0])
		else:
			query_drop_default = ""
			query_add_default = ""
				
		return {'drop':query_drop_default, 'create':query_add_default}

	def build_enum_ddl(self, enm_dic):
		"""
			The method builds the enum DDL using the token data. 
			The postgresql system catalog  is queried to determine whether the enum exists and needs to be altered.
			The alter is not written in the replica log table but executed as single statement as PostgreSQL do not allow the alter being part of a multi command
			SQL.
			
			:param enm_dic: a dictionary with the enumeration details
			:return: a dictionary with the pre_alter and post_alter statements (e.g. pre alter create type , post alter drop type)
			:rtype: dictionary
		"""
		#enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list}
		enum_name="enum_%s_%s" % (enm_dic['table'], enm_dic['column'])
		
		sql_check_enum = """
			SELECT 
				typ.typcategory,
				typ.typname,
				sch_typ.nspname as typschema,
				CASE 
					WHEN typ.typcategory='E'
					THEN
					(
						SELECT 
							array_agg(enumlabel) 
						FROM 
							pg_enum 
						WHERE 
							enumtypid=typ.oid
					)
				END enum_list
			FROM
				pg_type typ
				INNER JOIN pg_namespace sch_typ
					ON  sch_typ.oid = typ.typnamespace

			WHERE
					sch_typ.nspname=%s
				AND	typ.typname=%s
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_check_enum, (self.dest_schema,  enum_name))
		type_data=self.pg_conn.pgsql_cur.fetchone()
		return_dic = {}
		pre_alter = ""
		post_alter = ""
		column_type = enm_dic["type"]
		self.logger.debug(enm_dic)
		if type_data:
			if type_data[0] == 'E' and enm_dic["type"] == 'enum':
				self.logger.debug('There is already the enum %s, altering the type')
				new_enums = [val.strip() for val in enm_dic["enum_list"] if val.strip() not in type_data[3]]
				sql_add = []
				for enumeration in  new_enums:
					sql_add =  """ALTER TYPE "%s"."%s" ADD VALUE '%s';""" % (type_data[2], enum_name, enumeration) 
					self.pg_conn.pgsql_cur.execute(sql_add)
				column_type = enum_name
			elif type_data[0] != 'E' and enm_dic["type"] == 'enum':
				self.logger.debug('The column will be altered in enum, creating the type')
				pre_alter = "CREATE TYPE \"%s\" AS ENUM (%s);" % (enum_name, enm_dic["enum_elements"])
				column_type = enum_name
			elif type_data[0] == 'E' and enm_dic["type"] != 'enum':
				self.logger.debug('The column is no longer an enum, dropping the type')
				post_alter = "DROP TYPE \"%s\" " % (enum_name)
		elif not type_data and enm_dic["type"] == 'enum':
				self.logger.debug('Creating a new enumeration type %s' % (enum_name))
				pre_alter = "CREATE TYPE \"%s\" AS ENUM (%s);" % (enum_name, enm_dic["enum_elements"])
				column_type = enum_name

		return_dic["column_type"] = column_type
		return_dic["pre_alter"] = pre_alter
		return_dic["post_alter"]  = post_alter
		return return_dic
		
	def build_alter_table(self, token):
		""" 
			The method builds the alter table statement from the token data.
			The function currently supports the following statements.
			DROP TABLE
			ADD COLUMN 
			CHANGE
			MODIFY
			
			The change and modify are potential source of breakage for the replica because of 
			the mysql implicit fallback data types. 
			For better understanding please have a look to 
			
			http://www.cybertec.at/why-favor-postgresql-over-mariadb-mysql/
			
			:param token: A dictionary with the tokenised sql statement
			:return: query the DDL query in the PostgreSQL dialect
			:rtype: string
			
		"""
		alter_cmd = []
		ddl_pre_alter = []
		ddl_post_alter = []
		query_cmd=token["command"]
		table_name=token["name"]
		for alter_dic in token["alter_cmd"]:
			if alter_dic["command"] == 'DROP':
				alter_cmd.append("%(command)s %(name)s CASCADE" % alter_dic)
			elif alter_dic["command"] == 'ADD':
				
				column_type=self.get_data_type(alter_dic, table_name)
				column_name = alter_dic["name"]
				enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
				enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
				enm_alter = self.build_enum_ddl(enm_dic)
				ddl_pre_alter.append(enm_alter["pre_alter"])
				ddl_post_alter.append(enm_alter["post_alter"])
				column_type= enm_alter["column_type"]
				if 	column_type in ["character varying", "character", 'numeric', 'bit', 'float']:
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				if alter_dic["default"]:
					default_value = "DEFAULT %s" % alter_dic["default"]
				else:
					default_value=""
				alter_cmd.append("%s \"%s\" %s NULL %s" % (alter_dic["command"], column_name, column_type, default_value))	
			elif alter_dic["command"] == 'CHANGE':
				sql_rename = ""
				sql_type = ""
				old_column=alter_dic["old"]
				new_column=alter_dic["new"]
				column_name = old_column
				enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
				
				column_type=self.get_data_type(alter_dic, table_name)
				default_sql = self.generate_default_statements(table_name, old_column, new_column)
				enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
				enm_alter = self.build_enum_ddl(enm_dic)

				ddl_pre_alter.append(enm_alter["pre_alter"])
				ddl_pre_alter.append(default_sql["drop"])
				ddl_post_alter.append(enm_alter["post_alter"])
				ddl_post_alter.append(default_sql["create"])
				column_type= enm_alter["column_type"]
				
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				sql_type = """ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s  USING "%s"::%s ;;""" % (table_name, old_column, column_type, old_column, column_type)
				if old_column != new_column:
					sql_rename="""ALTER TABLE  "%s" RENAME COLUMN "%s" TO "%s" ;""" % (table_name, old_column, new_column)
					
				query = ' '.join(ddl_pre_alter)
				query += sql_type+sql_rename
				query += ' '.join(ddl_post_alter)
				return query

			elif alter_dic["command"] == 'MODIFY':
				column_type=self.get_data_type(alter_dic, table_name)
				column_name = alter_dic["name"]
				
				enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
				default_sql = self.generate_default_statements(table_name, column_name)
				enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
				enm_alter = self.build_enum_ddl(enm_dic)

				ddl_pre_alter.append(enm_alter["pre_alter"])
				ddl_pre_alter.append(default_sql["drop"])
				ddl_post_alter.append(enm_alter["post_alter"])
				ddl_post_alter.append(default_sql["create"])
				column_type= enm_alter["column_type"]
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				query = ' '.join(ddl_pre_alter)
				query +=  """ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s USING "%s"::%s ;""" % (table_name, column_name, column_type, column_name, column_type)
				query += ' '.join(ddl_post_alter)
				return query
		query = ' '.join(ddl_pre_alter)
		query +=  query_cmd + ' '+ table_name+ ' ' +', '.join(alter_cmd)+" ;"
		query += ' '.join(ddl_post_alter)
		return query


	def truncate_tables(self):
		"""
			The method truncate the tables listed in t_index_def. In order to minimise the risk of lock chain
			the truncate is prepended by a set lock_timeout = 10 seconds. If the lock is not acquired in that time
			the procedure fallsback to a delete and vacuum. 
		"""
		sql_clean=""" 
			SELECT DISTINCT
				format('SET lock_timeout=''10s'';TRUNCATE TABLE %%I.%%I CASCADE;',v_schema,v_table) v_truncate,
				format('DELETE FROM %%I.%%I;',v_schema,v_table) v_delete,
				format('VACUUM %%I.%%I;',v_schema,v_table) v_vacuum,
				format('%%I.%%I',v_schema,v_table) as v_tab,
				v_table
			FROM
				sch_chameleon.t_index_def 
			WHERE
				i_id_source=%s
			ORDER BY 
				v_table
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_clean, (self.i_id_source, ))
		tab_clean=self.pg_conn.pgsql_cur.fetchall()
		for stat_clean in tab_clean:
			st_truncate=stat_clean[0]
			st_delete=stat_clean[1]
			st_vacuum=stat_clean[2]
			tab_name=stat_clean[3]
			try:
				self.logger.info("truncating table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_truncate)
				
			except:
				self.logger.info("truncate failed, fallback to delete on table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_delete)
				self.logger.info("running vacuum on table %s" % (tab_name,))
				self.pg_conn.pgsql_cur.execute(st_vacuum)

	def drop_src_indices(self):
		"""
			The method executes the index drop statements read from the table t_index_def.
			The method is used when resyncing the replica for removing the indices before the bulk load.
		"""
		sql_idx="""
			SELECT 
				t_drop 
			FROM  
				sch_chameleon.t_index_def 
			WHERE 
				i_id_source=%s;
		"""
		self.pg_conn.pgsql_cur.execute(sql_idx, (self.i_id_source, ))
		idx_drop=self.pg_conn.pgsql_cur.fetchall()
		for drop_stat in idx_drop:
			self.pg_conn.pgsql_cur.execute(drop_stat[0])
			
	def create_src_indices(self):
		"""
			The method executes the index DDL read from the table t_index_def.
			The method is used when resyncing the replica for recreating the indices after the bulk load.
		"""
		sql_idx="""
			SELECT 
				t_create 
			FROM  
				sch_chameleon.t_index_def 
			WHERE 
				i_id_source=%s;
		"""
		self.pg_conn.pgsql_cur.execute(sql_idx, (self.i_id_source, ))
		idx_create=self.pg_conn.pgsql_cur.fetchall()
		for create_stat in idx_create:
			self.pg_conn.pgsql_cur.execute(create_stat[0])
		

	def get_index_def(self):
		"""
			The method inserts in the table t_index_def the create and drop statements for the tables affected by 
			the resync replica.
		"""
		table_limit = ''
		if self.table_limit[0] != '*':
			table_limit = self.pg_conn.pgsql_cur.mogrify("""WHERE table_name IN  (SELECT unnest(%s))""",(self.table_limit, )).decode()
		
		sql_get_idx=""" 
			DELETE FROM sch_chameleon.t_index_def WHERE i_id_source=%s;
			INSERT INTO sch_chameleon.t_index_def
				(
					i_id_source,
					v_schema,
					v_table,
					v_index,
					t_create,
					t_drop
				)
			SELECT 
				i_id_source,
				schema_name,
				table_name,
				index_name,
				CASE
					WHEN indisprimary
					THEN
						format('ALTER TABLE %%I.%%I ADD CONSTRAINT %%I %%s',
							schema_name,
							table_name,
							index_name,
							pg_get_constraintdef(const_id)
						)
						
					ELSE
						pg_get_indexdef(index_id)    
				END AS t_create,
				CASE
					WHEN indisprimary
					THEN
						format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I',
							schema_name,
							table_name,
							index_name
							
						)
						
					ELSE
						format('DROP INDEX %%I.%%I',
							schema_name,
							index_name
							
						)
				END AS  t_drop
				
			FROM

			(
			SELECT 
				tab.relname AS table_name,
				indx.relname AS index_name,
				idx.indexrelid index_id,
				indisprimary,
				sch.nspname schema_name,
				src.i_id_source,
				cns.oid as const_id
				
			FROM
				pg_index idx
				INNER JOIN pg_class indx
				ON
					idx.indexrelid=indx.oid
				INNER JOIN pg_class tab
				INNER JOIN pg_namespace sch
				ON 
					tab.relnamespace=sch.oid
				
				ON
					idx.indrelid=tab.oid
				INNER JOIN sch_chameleon.t_sources src
				ON sch.nspname=src.t_dest_schema
				LEFT OUTER JOIN pg_constraint cns
				ON 
						indx.relname=cns.conname
					AND cns.connamespace=sch.oid
				
			WHERE
				sch.nspname=%s
			) idx
			
		""" + table_limit
		
		self.pg_conn.pgsql_cur.execute(sql_get_idx, (self.i_id_source,  self.dest_schema, ))

	def drop_primary_key(self, token):
		"""
			The method drops the primary key for the table.
			As tables without primary key cannot be replicated the method calls unregister_table
			to remove the table from the replica set.
			The drop constraint statement is not built from the token but generated from the information_schema.
			
			
			:param token: the tokenised query for drop primary key
		"""
		self.logger.info("dropping primary key for table %s" % (token["name"],))
		sql_gen="""
			SELECT  DISTINCT
				format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I;',
				table_schema,
				table_name,
				constraint_name
				)
			FROM 
				information_schema.key_column_usage 
			WHERE 
					table_schema=%s 
				AND table_name=%s
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_gen, (self.dest_schema, token["name"]))
		value_check=self.pg_conn.pgsql_cur.fetchone()
		if value_check:
			sql_drop=value_check[0]
			self.pg_conn.pgsql_cur.execute(sql_drop)
			self.unregister_table(token["name"])
		

	def gen_query(self, token):
		""" 
			The method builds the DDL using the tokenised SQL stored in token.
			The supported commands are 
			RENAME TABLE
			DROP TABLE
			TRUNCATE
			CREATE TABLE
			ALTER TABLE
			DROP PRIMARY KEY
			
			:param token: A dictionary with the tokenised sql statement
			:return: query the DDL query in the PostgreSQL dialect
			:rtype: string
			
		"""
		query=""
		if token["command"] =="RENAME TABLE":
			query = """ALTER TABLE "%s" RENAME TO "%s" """ % (token["name"], token["new_name"])	
			try:
				self.table_metadata[token["new_name"]]
				self.store_table(token["new_name"])
			except KeyError:
				try:
					self.table_metadata[token["new_name"]] = self.table_metadata[token["name"]]
					self.store_table(token["new_name"])
				except KeyError:
					query = ""
			
		elif token["command"] =="DROP TABLE":
			query=" %(command)s IF EXISTS \"%(name)s\";" % token
		elif token["command"] =="TRUNCATE":
			query=" %(command)s TABLE \"%(name)s\" CASCADE;" % token
		elif token["command"] =="CREATE TABLE":
			table_metadata={}
			table_metadata["columns"]=token["columns"]
			table_metadata["name"]=token["name"]
			table_metadata["indices"]=token["indices"]
			self.table_metadata={}
			self.table_metadata[token["name"]]=table_metadata
			self.build_tab_ddl()
			self.build_idx_ddl()
			query_type=' '.join(self.type_ddl[token["name"]])
			query_table=self.table_ddl[token["name"]]
			query_idx=' '.join(self.idx_ddl[token["name"]])
			query=query_type+query_table+query_idx
			self.store_table(token["name"])
		elif token["command"] == "ALTER TABLE":
			query=self.build_alter_table(token)
		elif token["command"] == "DROP PRIMARY KEY":
			self.drop_primary_key(token)
		return query 


	def write_ddl(self, token, query_data):
		"""
			The method writes the DDL built from the tokenised sql into PostgreSQL.
			
			:param token: the tokenised query
			:param query_data: query's metadata (schema,binlog, etc.)
		"""
		sql_path=""" SET search_path="%s";""" % self.dest_schema
		pg_ddl=sql_path+self.gen_query(token)
		log_table=query_data["log_table"]
		insert_vals=(	query_data["batch_id"], 
								token["name"],  
								query_data["schema"], 
								query_data["binlog"], 
								query_data["logpos"], 
								pg_ddl
							)
		sql_insert="""
			INSERT INTO sch_chameleon."""+log_table+"""
				(
					i_id_batch, 
					v_table_name, 
					v_schema_name, 
					enm_binlog_event, 
					t_binlog_name, 
					i_binlog_position, 
					t_query
				)
			VALUES
				(
					%s,
					%s,
					%s,
					'ddl',
					%s,
					%s,
					%s
				)
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_insert, insert_vals)
		
		
	def check_reindex(self):
		"""
			the function checks if there is any reindex running and holds for  the given number of seconds 
		"""
		sql_check="""
			SELECT 
				count(*) 
			FROM 
				pg_stat_activity 
			WHERE 
					datname=current_database() 
				AND	application_name = ANY(%s) ;
		"""
		while True:
			self.pg_conn.pgsql_cur.execute(sql_check, (self.reindex_app_names, ))
			reindex_tup = self.pg_conn.pgsql_cur.fetchone()
			reindex_cnt = reindex_tup[0]
			if reindex_cnt == 0:
				break;
			self.logger.info("reindex detected, sleeping %s second(s)" % (self.sleep_on_reindex,))
			time.sleep(self.sleep_on_reindex)
	
	def set_consistent_table(self, table):
		"""
			The method set to NULL the  binlog name and position for the given table.
			When the table is marked consistent the read replica loop reads and saves the table's row images.
			
			:param table: the table name
		"""
		sql_set = """
			UPDATE sch_chameleon.t_replica_tables
				SET 
					t_binlog_name = NULL,
					i_binlog_position = NULL
			WHERE
					i_id_source = %s
				AND	v_table_name = %s
				AND	v_schema_name = %s
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_set, (self.i_id_source, table, self.dest_schema))
		
	
	def get_inconsistent_tables(self):
		"""
			The method collects the tables in not consistent state.
			The informations are stored in a dictionary which key is the table's name.
			The dictionary is used in the read replica loop to determine wheter the table's modifications
			should be ignored because in not consistent state.
			
			:return: a dictionary with the tables in inconsistent state and their snapshot coordinates.
			:rtype: dictionary
		"""
		sql_get = """
			SELECT
				v_schema_name,				
				v_table_name,
				t_binlog_name,
				i_binlog_position
			FROM
				sch_chameleon.t_replica_tables
			WHERE
				t_binlog_name IS NOT NULL
				AND i_binlog_position IS NOT NULL
				AND i_id_source = %s
		;
		"""
		inc_dic = {}
		self.pg_conn.pgsql_cur.execute(sql_get, (self.i_id_source, ))
		inc_results = self.pg_conn.pgsql_cur.fetchall()
		for table  in inc_results:
			tab_dic = {}
			tab_dic["schema"]  = table[0]
			tab_dic["table"]  = table[1]
			tab_dic["log_seq"]  = int(table[2].split('.')[1])
			tab_dic["log_pos"]  = int(table[3])
			inc_dic[table[1]] = tab_dic
		return inc_dic
		
		
	def delete_table_events(self):
		"""
			The method removes the events from the log table for specific table and source. 
			Is used to cleanup any residual event for a a synced table in the replica_engine's sync_table method.
		"""
		sql_clean = """
			DELETE FROM sch_chameleon.t_log_replica
			WHERE 
				i_id_event IN (
							SELECT 
								log.i_id_event
							FROM
								sch_chameleon.t_replica_batch bat
								INNER JOIN sch_chameleon.t_log_replica log
									ON  log.i_id_batch=bat.i_id_batch
							WHERE
									log.v_table_name=ANY(%s)
								AND 	bat.i_id_source=%s
						)
			;
		"""
		self.pg_conn.pgsql_cur.execute(sql_clean, (self.table_limit, self.i_id_source, ))
		
