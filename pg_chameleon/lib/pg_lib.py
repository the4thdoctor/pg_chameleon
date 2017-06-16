import psycopg2
import os
import io
import sys
import json
import datetime
import decimal
import time
import base64
class pg_encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.time) or isinstance(obj, datetime.datetime) or  isinstance(obj, datetime.date) or isinstance(obj, decimal.Decimal) or isinstance(obj, datetime.timedelta):
			return str(obj)
		return json.JSONEncoder.default(self, obj)


class pg_connection(object):
	def __init__(self, global_config):
		self.global_conf=global_config
		self.pg_conn=self.global_conf.pg_conn
		self.pg_database=self.global_conf.pg_database
		self.dest_schema=self.global_conf.my_database
		self.pg_connection=None
		self.pg_cursor=None
		self.pg_charset=self.global_conf.pg_charset
		
	
	def connect_db(self):
		"""
			Connects to PostgreSQL using the parameters stored in pg_pars built adding the key dbname to the self.pg_conn dictionary.
			The method after the connection creates a database cursor and set the session to autocommit.
		"""
		pg_pars=dict(list(self.pg_conn.items())+ list({'dbname':self.pg_database}.items()))
		strconn="dbname=%(dbname)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % pg_pars
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		self.pgsql_conn .set_client_encoding(self.pg_charset)
		self.pgsql_cur=self.pgsql_conn .cursor()
		
	
	def disconnect_db(self):
		"""
			The method disconnects from the database closing the connection.
		"""
		self.pgsql_conn.close()
	
	
		

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
			'decimal':'numeric', 
			'double':'double precision', 
			'double precision':'double precision', 
			'float':'float', 
			'bit':'integer', 
			'year':'integer', 
			'enum':'enum', 
			'set':'text', 
			'json':'text', 
			'bool':'boolean', 
			'boolean':'boolean', 
		}
		self.table_ddl = {}
		self.idx_ddl = {}
		self.type_ddl = {}
		self.pg_charset = self.pg_conn.pg_charset
		self.cat_version = '1.3'
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
		]
		cat_version=self.get_schema_version()
		num_schema=(self.check_service_schema())[0]
		if cat_version!=self.cat_version and int(num_schema)>0:
			self.upgrade_service_schema()
		self.table_limit = ['*']
	
	def set_application_name(self, action=""):
		"""
			The method sets the application name in the replica using the variable self.pg_conn.global_conf.source_name,
			Making simpler to find the replication processes. If the source name is not set then a generic PGCHAMELEON name is used.
		"""
		if self.pg_conn.global_conf.source_name:
			app_name = "[PGCH] - source: %s, action: %s" % (self.pg_conn.global_conf.source_name, action)
		else:
			app_name = "[PGCH]"
		sql_app_name="""SET application_name=%s; """
		self.pg_conn.pgsql_cur.execute(sql_app_name, (app_name , ))
	
		
	def add_source(self, source_name, dest_schema):
		"""
			The method add a new source in the replica catalogue. 
			If the source name is already present an error message is emitted without further actions.
			:param source_name: The source name stored in the configuration parameter source_name.
			:param dest_schema: The destination schema stored in the configuration parameter dest_schema.
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
						t_dest_schema
					) 
				VALUES 
					(
						%s,
						%s
					)
				RETURNING 
					i_id_source
				; 
			"""
			self.pg_conn.pgsql_cur.execute(sql_add, (source_name, dest_schema ))
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
				enm_status=%s
			WHERE
				t_source=%s
			RETURNING i_id_source,t_dest_schema
				;
			"""
		source_name = self.pg_conn.global_conf.source_name
		self.pg_conn.pgsql_cur.execute(sql_source, (source_status, source_name))
		source_data = self.pg_conn.pgsql_cur.fetchone()
		try:
			self.i_id_source = source_data[0]
			self.dest_schema = source_data[1]
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
		sql_drop="DROP SCHEMA IF EXISTS "+self.dest_schema+" CASCADE;"
		sql_create=" CREATE SCHEMA IF NOT EXISTS "+self.dest_schema+";"
		sql_path=" SET search_path="+self.dest_schema+";"
		self.pg_conn.pgsql_cur.execute(sql_drop)
		self.pg_conn.pgsql_cur.execute(sql_create)
		self.pg_conn.pgsql_cur.execute(sql_path)
	
	def store_table(self, table_name):
		"""
			The method saves the table name along with the primary key definition in the table t_replica_tables.
			This is required in order to let the replay procedure which primary key to use replaying the update and delete.
			If the table is without primary key is not stored. 
			A table without primary key is copied and the indices are create like any other table. 
			However the replica doesn't work for the tables without primary key.
			
			:param table_name: the table name to store in the table  t_replica_tables
		"""
		table_data=self.table_metadata[table_name]
		for index in table_data["indices"]:
			if index["index_name"]=="PRIMARY":
				sql_insert=""" 
					INSERT INTO sch_chameleon.t_replica_tables 
						(
							i_id_source,
							v_table_name,
							v_schema_name,
							v_table_pkey
						)
					VALUES 
						(
							%s,
							%s,
							%s,
							ARRAY[%s]
						)
					ON CONFLICT (i_id_source,v_table_name,v_schema_name)
						DO UPDATE 
							SET v_table_pkey=EXCLUDED.v_table_pkey
										;
								"""
				self.pg_conn.pgsql_cur.execute(sql_insert, (self.i_id_source, table_name, self.dest_schema, index["index_columns"].strip()))	
	
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
	
	def create_tables(self):
		"""
			The method loops trough the list table_ddl and executes the creation scripts.
			No index is created in this method
		"""
		for table in self.table_ddl:
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
				indx=index["index_name"]
				index_columns=index["index_columns"]
				non_unique=index["non_unique"]
				if indx=='PRIMARY':
					pkey_name="pk_"+table_name[0:20]+"_"+str(self.idx_sequence)
					pkey_def='ALTER TABLE "'+table_name+'" ADD CONSTRAINT "'+pkey_name+'" PRIMARY KEY ('+index_columns+') ;'
					table_idx.append(pkey_def)
				else:
					if non_unique==0:
						unique_key='UNIQUE'
					else:
						unique_key=''
					index_name='"idx_'+indx[0:20]+table_name[0:20]+"_"+str(self.idx_sequence)+'"'
					idx_def='CREATE '+unique_key+' INDEX '+ index_name+' ON "'+table_name+'" ('+index_columns+');'
					table_idx.append(idx_def)
				self.idx_sequence+=1
					
			self.idx_ddl[table_name]=table_idx

	def build_tab_ddl(self):
		""" 
			The method iterates over the list l_tables and builds a new list with the statements for tables
		"""
		
		for table_name in self.table_metadata:
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
				column_type=self.type_dictionary[column["data_type"]]
				if column_type=="enum":
					enum_type="enum_"+table["name"]+"_"+column["column_name"]
					sql_drop_enum='DROP TYPE IF EXISTS '+enum_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+enum_type+" AS ENUM "+column["enum_list"]+";"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
					column_type=enum_type
				if column_type=="character varying" or column_type=="character":
					column_type=column_type+"("+str(column["character_maximum_length"])+")"
				if column_type=='bit' or column_type=='float' or column_type=='numeric':
					column_type=column_type+"("+str(column["numeric_precision"])+")"
				if column["extra"]=="auto_increment":
					column_type="bigserial"
				ddl_columns.append('"'+column["column_name"]+'" '+column_type+" "+col_is_null )
			def_columns=str(',').join(ddl_columns)
			self.type_ddl[table["name"]]=ddl_enum
			self.table_ddl[table["name"]]=ddl_head+def_columns+ddl_tail
	


	
	def get_schema_version(self):
		"""
			The method gets the service schema version querying the view sch_chameleon.v_version.
			The try-except is used in order to get a valid value "base" if the view is missing.
			This happens only if the schema upgrade is performed from very early pg_chamelon's versions.
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
				 date_trunc('seconds',now())-ts_last_event lag,
				ts_last_event 
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
		event_time = master_data["Time"]
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
				ts_last_event=to_timestamp(%s),
				v_log_table=ARRAY[v_log_table[2],v_log_table[1]]
				
			WHERE 
				i_id_source=%s
			RETURNING v_log_table[1]
			; 
		"""
		
		self.logger.info("saving master data id source: %s log file: %s  log position:%s Last event: %s" % (self.i_id_source, binlog_name, binlog_position, event_time))
		
		
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
			table_file = results[0]
			self.logger.debug("master data: table file %s, log name: %s, log position: %s " % (table_file, binlog_name, binlog_position))
		
		
			
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
			global_data=row_data["global_data"]
			event_data=row_data["event_data"]
			event_update=row_data["event_update"]
			log_table=global_data["log_table"]
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
					jsb_event_update
				)
				VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
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
						json.dumps(event_update, cls=pg_encoder)
					)
				)
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
		b64_row=base64.b64encode(str(row_data))
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
		self.pg_conn.pgsql_cur.execute(sql_save,(batch_id,b64_row))
	
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
			insert_list.append(self.pg_conn.pgsql_cur.mogrify("%s,%s,%s,%s,%s,%s,%s,%s" ,  (
						global_data["batch_id"], 
						global_data["table"],  
						self.dest_schema, 
						global_data["action"], 
						global_data["binlog"], 
						global_data["logpos"], 
						json.dumps(event_data, cls=pg_encoder), 
						json.dumps(event_update, cls=pg_encoder)
					)
				)
			)
											
		csv_data=b"\n".join(insert_list ).decode()
		csv_file.write(csv_data)
		csv_file.seek(0)
		try:
			
			#self.pg_conn.pgsql_cur.execute(sql_insert)
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
						jsb_event_update
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
			
			
		
	def set_batch_processed(self, id_batch):
		"""
			The method updates the flag b_processed and sets the processed timestamp for the given batch id
			
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
		
	def process_batch(self, replica_batch_size):
		"""
			The method calls the function fn_process_batch with the parameters batch size and the id_source.
			
			The plpgsql function returns true if there are still rows to process. When all rows are replayed 
			the method exits.
			
			:param replica_batch_size: the max rows to process in a single function call. 
			
			
		"""
		self.set_application_name("replay batch")
		batch_loop=True
		sql_process="""SELECT sch_chameleon.fn_process_batch(%s,%s);"""
		while batch_loop:
			self.pg_conn.pgsql_cur.execute(sql_process, (replica_batch_size, self.i_id_source))
			batch_result=self.pg_conn.pgsql_cur.fetchone()
			batch_loop=batch_result[0]
			self.logger.debug("Batch loop value %s" % (batch_loop))
		self.logger.debug("Cleaning replayed batches older than %s for source %s" % (self.batch_retention,  self.i_id_source))
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
		self.set_application_name("cleanup old batches")
		self.pg_conn.pgsql_cur.execute(sql_cleanup, (self.batch_retention, self.i_id_source ))
		self.set_application_name("idle")

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
		alter_cmd=[]
		ddl_enum=[]
		query_cmd=token["command"]
		table_name=token["name"]
		for alter_dic in token["alter_cmd"]:
			if alter_dic["command"] == 'DROP':
				alter_cmd.append("%(command)s %(name)s CASCADE" % alter_dic)
			elif alter_dic["command"] == 'ADD':
				column_type=self.type_dictionary[alter_dic["type"]]
				if column_type=="enum":
					enum_name="enum_"+table_name+"_"+alter_dic["name"]
					column_type=enum_name
					sql_drop_enum='DROP TYPE IF EXISTS '+column_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+column_type+" AS ENUM ("+alter_dic["dimension"]+");"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				alter_cmd.append("%s \"%s\" %s NULL" % (alter_dic["command"], alter_dic["name"], column_type))	
			elif alter_dic["command"] == 'CHANGE':
				sql_rename = ""
				sql_type = ""
				old_column=alter_dic["old"]
				new_column=alter_dic["new"]
				column_type=self.type_dictionary[alter_dic["type"]]
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				sql_type = """ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s  USING "%s"::%s ;;""" % (table_name, old_column, column_type, old_column, column_type)
				if old_column != new_column:
					sql_rename="""ALTER TABLE  "%s" RENAME COLUMN "%s" TO "%s" ;""" % (table_name, old_column, new_column)
				query=sql_type+sql_rename
				return query
			elif alter_dic["command"] == 'MODIFY':
				column_type=self.type_dictionary[alter_dic["type"]]
				column_name=alter_dic["name"]
				if column_type=="enum":
					enum_name="enum_"+table_name+"_"+alter_dic["name"]
					column_type=enum_name
					sql_drop_enum='DROP TYPE IF EXISTS '+column_type+' CASCADE;'
					sql_create_enum="CREATE TYPE "+column_type+" AS ENUM ("+alter_dic["dimension"]+");"
					ddl_enum.append(sql_drop_enum)
					ddl_enum.append(sql_create_enum)
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				query = ' '.join(ddl_enum) + """ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s USING "%s"::%s ;""" % (table_name, column_name, column_type, column_name, column_type)
				return query
		query = ' '.join(ddl_enum)+" "+query_cmd + ' '+ table_name+ ' ' +', '.join(alter_cmd)+" ;"
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
		
		if token["command"] =="DROP TABLE":
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
		sql_path=" SET search_path="+self.dest_schema+";"
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
