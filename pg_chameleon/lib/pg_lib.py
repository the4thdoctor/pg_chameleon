import io
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
		self.lock_timeout = 0
		
	def __del__(self):
		"""
			Class destructor, tries to disconnect the postgresql connection.
		"""
		self.disconnect_db()
	
	def set_autocommit_db(self, auto_commit):
		"""
			The method sets the auto_commit flag for the class connection self.pgsql_conn.
			In general the connection is always autocommit but in some operations (e.g. update_schema_mappings) 
			is better to run the process in a single transaction in order to avoid inconsistencies.
			
			:param autocommit: boolean flag which sets autocommit on or off.

		"""
		self.logger.debug("Changing the autocommit flag to %s" % auto_commit)
		self.pgsql_conn.set_session(autocommit=auto_commit)

	
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
			self.set_autocommit_db(True)
			self.pgsql_cur = self.pgsql_conn .cursor()
		elif not self.dest_conn:
			self.logger.error("Undefined database connection string. Exiting now.")
			sys.exit()
		elif self.pgsql_conn:
			self.logger.debug("There is already a database connection active.")
			

	def disconnect_db(self):
		"""
			The method disconnects the postgres connection if there is any active. Otherwise ignore it.
		"""
		if self.pgsql_conn:
			self.pgsql_conn.close()
			self.pgsql_conn = None
		else:
			pass
			
	def set_lock_timeout(self):
		"""
			The method sets the lock timeout using the value stored in the class attribute lock_timeout.
		"""
		self.logger.debug("Changing the lock timeout for the session to %s." % self.lock_timeout)
		self.pgsql_cur.execute("SET LOCK_TIMEOUT =%s;",  (self.lock_timeout, ))
	
	def unset_lock_timeout(self):
		"""
			The method sets the lock timeout using the value stored in the class attribute lock_timeout.
		"""
		self.logger.debug("Disabling the lock timeout for the session." )
		self.pgsql_cur.execute("SET LOCK_TIMEOUT ='0';")
	
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
	
	def detach_replica(self):
		"""
			The method detach the replica from mysql, resets all the sequences and creates the foreign keys 
			using the dictionary extracted from mysql. The result is a stand alone set of schemas ready to work.
			
			The foreign keys are first created invalid then validated in a second time.
		"""
		self.set_source_id()
		schema_mappings = self.get_schema_mappings()
		fk_list = []
		fk_counter = 0
		sql_gen_reset = """ 
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
					table_schema IN (
						SELECT 
							(jsonb_each_text(jsb_schema_mappings)).value 
						FROM 
							sch_chameleon.t_sources 
						WHERE 
							i_id_source=%s
						)
				AND	column_default like 'nextval%%'
						
		;"""
		self.pgsql_cur.execute(sql_gen_reset, (self.i_id_source, ))
		reset_statements = self.pgsql_cur.fetchall()
		try:
			for statement in reset_statements:
				self.logger.info("resetting the sequence  %s" % statement[1])
				self.pgsql_cur.execute(statement[0])
		except psycopg2.Error as e:
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error(statement)
		except:
			raise
		
		for foreign_key in self.fk_metadata:
			table_name = foreign_key["table_name"]
			table_schema = schema_mappings[foreign_key["table_schema"]]
			fk_name = ("%s_%s") % (foreign_key["constraint_name"][0:20] ,  str(fk_counter))
			fk_cols = foreign_key["fk_cols"]
			referenced_table_name = foreign_key["referenced_table_name"]
			referenced_table_schema = schema_mappings[foreign_key["referenced_table_schema"]]
			ref_columns = foreign_key["ref_columns"]
			fk_list.append({'fkey_name':fk_name, 'table_name':table_name, 'table_schema':table_schema})
			sql_fkey = ("""ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s"."%s" (%s) NOT VALID;""" % 
					(
						table_schema, 
						table_name, 
						fk_name, 
						fk_cols, 
						referenced_table_schema, 
						referenced_table_name, 
						ref_columns
					)
				)
			fk_counter+=1
			self.logger.info("creating invalid foreign key %s on table %s.%s" % (fk_name, table_schema, table_name))
			try:
				self.pgsql_cur.execute(sql_fkey)
			except psycopg2.Error as e:
					self.logger.error("could not create the foreign key %s on table %s.%s" % (fk_name, table_schema, table_name))
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error("STATEMENT: %s " % (sql_fkey))
		
		
		for fkey in fk_list:
			self.logger.info("validating %s on table %s.%s"  % (fkey["fkey_name"], fkey["table_schema"], fkey["table_name"])) 
			sql_validate = 'ALTER TABLE "%s"."%s" VALIDATE CONSTRAINT "%s";' % (fkey["table_schema"], fkey["table_name"], fkey["fkey_name"])
			try:
				self.pgsql_cur.execute(sql_validate)
			except psycopg2.Error as e:
					self.logger.error("could not validate the foreign key %s on table %s" % (fkey["table_name"], fkey["fkey_name"]))
					self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
					self.logger.error("STATEMENT: %s " % (sql_validate))			
		self.drop_source()
		
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
		self.pgsql_cur.execute(sql_get, (self.i_id_source, ))
		inc_results = self.pgsql_cur.fetchall()
		for table  in inc_results:
			tab_dic = {}
			dic_key = "%s.%s" % (table[0], table[1])
			tab_dic["schema"]  = table[0]
			tab_dic["table"]  = table[1]
			tab_dic["log_seq"]  = int(table[2].split('.')[1])
			tab_dic["log_pos"]  = int(table[3])
			inc_dic[dic_key] = tab_dic
		return inc_dic
	
	
	def grant_select(self):
		"""
			The method grants the select permissions on all the tables on the replicated schemas to the database roles
			listed in the source's variable grant_select_to.
			In the case a role doesn't exist the method emits an error message and skips the missing user.
		"""
		if self.grant_select_to:
			for schema in  self.schema_loading:
				schema_loading = self.schema_loading[schema]["loading"]
				self.logger.info("Granting select on tables in schema %s to the role(s) %s." % (schema_loading,','.join(self.grant_select_to)))
				for db_role in self.grant_select_to:
					sql_grant_usage = sql.SQL("GRANT USAGE ON SCHEMA {} TO {};").format(sql.Identifier(schema_loading), sql.Identifier(db_role))
					sql_alter_default_privs = sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {};").format(sql.Identifier(schema_loading), sql.Identifier(db_role))
					try:
						self.pgsql_cur.execute(sql_grant_usage)
						self.pgsql_cur.execute(sql_alter_default_privs)
						for table in self.schema_tables[schema]:
							self.logger.info("Granting select on table %s.%s to the role %s." % (schema_loading, table,db_role))
							sql_grant_select = sql.SQL("GRANT SELECT ON TABLE {}.{} TO {};").format(sql.Identifier(schema_loading), sql.Identifier(table), sql.Identifier(db_role))
							try:
								self.pgsql_cur.execute(sql_grant_select)
							except psycopg2.Error as er:
								self.logger.error("SQLCODE: %s SQLERROR: %s" % (er.pgcode, er.pgerror))
					except psycopg2.Error as e:
						if e.pgcode == "42704":
							self.logger.warning("The role %s does not exist" % (db_role, ))
						else:
							self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
			
	def replay_replica(self):
		"""
			The method replays the row images in the target database using the function 
			fn_replay_mysql. The function returns a composite type.
			The first element is a boolean flag which
			is true if the batch still require replay. it's false if it doesn't.
			In that case the while loop ends.
			The second element is a, optional list of table names. If any table cause error during the replay
			the problem is captured and the table is removed from the replica. Then the name is returned by
			the function. As the function can find multiple tables with errors during a single replay run, the 
			table names are stored in a list (Actually is a postgres array, see the create_schema.sql file for more details).
			 
			 Each batch which is looped trough can also find multiple tables so we return a list of lists to the replica_engine's
			 calling method.
			
		"""
		tables_error = []
		continue_loop = True
		self.source_config = self.sources[self.source]
		self.replica_batch_size = self.source_config["replica_batch_size"]
		while continue_loop:
			sql_replay = """SELECT * FROM sch_chameleon.fn_replay_mysql(%s,%s)""";
			self.pgsql_cur.execute(sql_replay, (self.replica_batch_size, self.i_id_source, ))
			replay_status = self.pgsql_cur.fetchone()
			self.logger.debug("Replay status %s" % replay_status[0])
			continue_loop = replay_status[0]
			if replay_status[1]:
				tables_error.append(replay_status[1])
		return tables_error
			
	
	def set_consistent_table(self, table, schema):
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
		self.pgsql_cur.execute(sql_set, (self.i_id_source, table, schema))
	
	def get_table_pkey(self, schema, table):
		"""
			The method queries the table sch_chameleon.t_replica_tables and gets the primary key 
			associated with the table, if any.
			If there is no primary key the method returns None
			
			:param schema: The table schema
			:param table: The table name
			:return: the primary key associated with the table
			:rtype: list
			
		"""
		sql_pkey = """
			SELECT 
				v_table_pkey
			FROM
				sch_chameleon.t_replica_tables
			WHERE
					v_schema_name=%s
				AND	v_table_name=%s
			;
		"""
		self.pgsql_cur.execute(sql_pkey, (schema, table, ))
		table_pkey = self.pgsql_cur.fetchone()
		return table_pkey[0]
		
	def generate_ddl(self, token,  destination_schema):
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
			:param destination_schema: The ddl destination schema mapped from the mysql corresponding schema
			:return: query the DDL query in the PostgreSQL dialect
			:rtype: string
			
		"""
		query=""
		if token["command"] =="RENAME TABLE":
			old_name = token["name"]
			new_name = token["new_name"]
			query = """ALTER TABLE "%s"."%s" RENAME TO "%s" """ % (destination_schema, old_name, new_name)	
			table_pkey = self.get_table_pkey(destination_schema, old_name)
			if table_pkey:
				self.store_table(destination_schema, new_name, table_pkey, None)
		elif token["command"] == "DROP TABLE":
			query=""" DROP TABLE IF EXISTS "%s"."%s";""" % (destination_schema, token["name"])	
		elif token["command"] == "TRUNCATE":
			query=""" TRUNCATE TABLE "%s"."%s" CASCADE;""" % (destination_schema, token["name"])	
		elif token["command"] =="CREATE TABLE":
			table_metadata = token["columns"]
			table_name = token["name"]
			index_data = token["indices"]
			table_ddl = self.build_create_table(table_metadata,  table_name,  destination_schema, temporary_schema=False)
			table_enum = ''.join(table_ddl["enum"])
			table_statement = table_ddl["table"] 
			index_ddl = self.build_create_index( destination_schema, table_name, index_data)
			table_pkey = index_ddl[0]
			table_indices = ''.join([val for key ,val in index_ddl[1].items()])
			self.store_table(destination_schema, table_name, table_pkey, None)
			query = "%s %s %s " % (table_enum, table_statement,  table_indices)
		elif token["command"] == "ALTER TABLE":
			query=self.build_alter_table(destination_schema, token)
		elif token["command"] == "DROP PRIMARY KEY":
			self.drop_primary_key(destination_schema, token)
		return query 

	def build_enum_ddl(self, schema, enm_dic):
		"""
			The method builds the enum DDL using the token data. 
			The postgresql system catalog  is queried to determine whether the enum exists and needs to be altered.
			The alter is not written in the replica log table but executed as single statement as PostgreSQL do not allow the alter being part of a multi command
			SQL.
			
			:param schema: the schema where the enumeration is present
			:param enm_dic: a dictionary with the enumeration details
			:return: a dictionary with the pre_alter and post_alter statements (e.g. pre alter create type , post alter drop type)
			:rtype: dictionary
		"""
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
		self.pgsql_cur.execute(sql_check_enum, (schema,  enum_name))
		type_data=self.pgsql_cur.fetchone()
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
					self.pgsql_cur.execute(sql_add)
				
			elif type_data[0] != 'E' and enm_dic["type"] == 'enum':
				self.logger.debug('The column will be altered in enum, creating the type')
				pre_alter = """CREATE TYPE "%s"."%s" AS ENUM (%s);""" % (schema,enum_name, enm_dic["enum_elements"])
				
			elif type_data[0] == 'E' and enm_dic["type"] != 'enum':
				self.logger.debug('The column is no longer an enum, dropping the type')
				post_alter = """DROP TYPE "%s"."%s"; """ % (schema,enum_name)
			column_type = """ "%s"."%s" """ % (schema, enum_name)
		elif not type_data and enm_dic["type"] == 'enum':
				self.logger.debug('Creating a new enumeration type %s' % (enum_name))
				pre_alter = """CREATE TYPE "%s"."%s" AS ENUM (%s);""" % (schema,enum_name, enm_dic["enum_elements"])
				column_type = """ "%s"."%s" """ % (schema, enum_name)

		return_dic["column_type"] = column_type
		return_dic["pre_alter"] = pre_alter
		return_dic["post_alter"]  = post_alter
		return return_dic
	

	def build_alter_table(self, schema, token):
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
			
			:param schema: The schema where the affected table is stored on postgres.
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
				
				column_type=self.get_data_type(alter_dic, schema, table_name)
				column_name = alter_dic["name"]
				enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
				enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
				enm_alter = self.build_enum_ddl(schema, enm_dic)
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
				
				column_type=self.get_data_type(alter_dic, schema, table_name)
				default_sql = self.generate_default_statements(schema, table_name, old_column, new_column)
				enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
				enm_alter = self.build_enum_ddl(schema, enm_dic)

				ddl_pre_alter.append(enm_alter["pre_alter"])
				ddl_pre_alter.append(default_sql["drop"])
				ddl_post_alter.append(enm_alter["post_alter"])
				ddl_post_alter.append(default_sql["create"])
				column_type= enm_alter["column_type"]
				
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				sql_type = """ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DATA TYPE %s  USING "%s"::%s ;;""" % (schema, table_name, old_column, column_type, old_column, column_type)
				if old_column != new_column:
					sql_rename="""ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s" ;""" % (schema, table_name, old_column, new_column)
					
				query = ' '.join(ddl_pre_alter)
				query += sql_type+sql_rename
				query += ' '.join(ddl_post_alter)
				return query

			elif alter_dic["command"] == 'MODIFY':
				column_type=self.get_data_type(alter_dic, schema, table_name)
				column_name = alter_dic["name"]
				
				enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
				default_sql = self.generate_default_statements(schema, table_name, column_name)
				enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
				enm_alter = self.build_enum_ddl(schema, enm_dic)

				ddl_pre_alter.append(enm_alter["pre_alter"])
				ddl_pre_alter.append(default_sql["drop"])
				ddl_post_alter.append(enm_alter["post_alter"])
				ddl_post_alter.append(default_sql["create"])
				column_type= enm_alter["column_type"]
				if column_type=="character varying" or column_type=="character" or column_type=='numeric' or column_type=='bit' or column_type=='float':
						column_type=column_type+"("+str(alter_dic["dimension"])+")"
				query = ' '.join(ddl_pre_alter)
				query +=  """ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DATA TYPE %s USING "%s"::%s ;""" % (schema, table_name, column_name, column_type, column_name, column_type)
				query += ' '.join(ddl_post_alter)
				return query
		query = ' '.join(ddl_pre_alter)
		query +=  """%s "%s"."%s" %s;""" % (query_cmd , schema,  table_name,', '.join(alter_cmd))
		query += ' '.join(ddl_post_alter)
		return query

	
	def drop_primary_key(self, schema, token):
		"""
			The method drops the primary key for the table.
			As tables without primary key cannot be replicated the method calls unregister_table
			to remove the table from the replica set.
			The drop constraint statement is not built from the token but generated from the information_schema.
			
			:param schema: The table's schema
			:param token: the tokenised query for drop primary key
		"""
		self.logger.info("dropping primary key for table %s.%s" % (schema, token["name"],))
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
		self.pgsql_cur.execute(sql_gen, (schema, token["name"]))
		value_check=self.pgsql_cur.fetchone()
		if value_check:
			sql_drop=value_check[0]
			self.pgsql_cur.execute(sql_drop)
			self.unregister_table(schema, token["name"])

	def unregister_table(self, schema,  table):
		"""
			This method is used to remove a table from the replica catalogue.
			The table is just deleted from the table sch_chameleon.t_replica_tables.
			
			:param schema: the schema name where the table is stored
			:param table: the table name to remove from t_replica_tables
		"""
		self.logger.info("unregistering table %s.%s from the replica catalog" % (schema, table,))
		sql_delete=""" DELETE FROM sch_chameleon.t_replica_tables 
					WHERE
							v_table_name=%s
						AND	v_schema_name=%s
					;
						"""
		self.pgsql_cur.execute(sql_delete, (table, schema))	
	
	def cleanup_source_tables(self):
		"""
			The method cleans up the tables for active source in sch_chameleon.t_replica_tables.
			
		"""
		self.logger.info("deleting all the table references from the replica catalog for source %s " % (self.source,))
		sql_delete=""" DELETE FROM sch_chameleon.t_replica_tables 
					WHERE
						i_id_source=%s
					;
						"""
		self.pgsql_cur.execute(sql_delete, (self.i_id_source, ))	
	
	def write_ddl(self, token, query_data, table_metadata, destination_schema):
		"""
			The method writes the DDL built from the tokenised sql into PostgreSQL.
			
			:param token: the tokenised query
			:param query_data: query's metadata (schema,binlog, etc.)
			:param table_metadata: the table's metadata retrieved from mysql. is an empty tuple if the statement is a drop table
			:param destination_schema: the postgresql destination schema determined using the schema mappings.
		"""
		pg_ddl = self.generate_ddl(token, destination_schema)
		log_table = query_data["log_table"]
		insert_vals = (	
				query_data["batch_id"], 
				token["name"],  
				query_data["schema"], 
				query_data["binlog"], 
				query_data["logpos"], 
				pg_ddl
			)
		sql_insert=sql.SQL("""
			INSERT INTO "sch_chameleon".{}
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
		""").format(sql.Identifier(log_table), )
		
		self.pgsql_cur.execute(sql_insert, insert_vals)
	
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
		self.pgsql_cur.execute(sql_batch, (self.i_id_source, self.i_id_source, self.i_id_source, ))
		return self.pgsql_cur.fetchall()
	
	
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
	
	def get_catalog_version(self):
		"""
			The method returns if the replica schema's version
			
			:return: the version string selected from sch_chameleon.v_version
			:rtype: text
		"""
		schema_version = None
		sql_version = """
			SELECT 
				t_version
			FROM 
				sch_chameleon.v_version 
			;
		"""
		self.connect_db()
		try:
			self.pgsql_cur.execute(sql_version)
			schema_version = self.pgsql_cur.fetchone()
			self.disconnect_db()
			schema_version = schema_version[0]
		except:
			schema_version = None
		return schema_version
			
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
	
	def check_schema_mappings(self, exclude_current_source=False):
		"""
			
			The default is false. 
		
			The method checks if there is already a destination schema in the stored schema mappings.
			As each schema should be managed by one mapping only, if the method returns None  then
			the source can be store safely. Otherwise the action. The method doesn't take any decision
			leaving this to the calling methods.
			The method assumes there is a database connection active.
			The method returns a list or none. 
			If the list is returned then contains the count and the destination schema name 
			that are already present in the replica catalogue.
			
			:param exclude_current_source: If set to true the check excludes the current source name from the check.
			:return: the schema already mapped in the replica catalogue. 
			:rtype: list
		"""
		if exclude_current_source:
			exclude_id = self.i_id_source
		else:
			exclude_id = -1
		schema_mappings = json.dumps(self.sources[self.source]["schema_mappings"])
		sql_check = """
			WITH t_check  AS
			(
					SELECT 
						(jsonb_each_text(jsb_schema_mappings)).value AS dest_schema
					FROM 
						sch_chameleon.t_sources
					WHERE 
						i_id_source <> %s
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
		self.pgsql_cur.execute(sql_check, (exclude_id, schema_mappings, ))
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
				self.insert_source_timings()
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
	
	def build_create_table(self, table_metadata,table_name,  schema, temporary_schema=True):
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
		if temporary_schema:
			destination_schema = self.schema_loading[schema]["loading"]
		else:
			destination_schema = schema
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
	
	def build_create_index(self, schema, table, index_data):
		""" 
			The method loops over the list index_data and builds a new list with the statements for the indices.
			
			:param destination_schema: the schema where the table belongs
			:param table_name: the table name 
			:param index_data: the index dictionary used to build the create index statements
			
			:return: a list with the alter and create index for the given table
			:rtype: list
		"""
		idx_ddl = {}
		table_primary = []
		
		for index in index_data:
				table_timestamp = str(int(time.time()))
				indx = index["index_name"]
				self.logger.debug("Generating the DDL for index %s" % (indx))
				index_columns = ['"%s"' % column for column in index["index_columns"]]
				non_unique = index["non_unique"]
				if indx =='PRIMARY':
					pkey_name = "pk_%s_%s_%s " % (table[0:10],table_timestamp,  self.idx_sequence)
					pkey_def = 'ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" PRIMARY KEY (%s) ;' % (schema, table, pkey_name, ','.join(index_columns))
					idx_ddl[pkey_name] = pkey_def
					table_primary = index["index_columns"]
				else:
					if non_unique == 0:
						unique_key = 'UNIQUE'
						if table_primary == []:
							table_primary = index["index_columns"]
							
					else:
						unique_key = ''
					index_name='idx_%s_%s_%s_%s' % (indx[0:10], table[0:10], table_timestamp, self.idx_sequence)
					idx_def='CREATE %s INDEX "%s" ON "%s"."%s" (%s);' % (unique_key, index_name, schema, table, ','.join(index_columns) )
					idx_ddl[index_name] = idx_def
				self.idx_sequence+=1
		return [table_primary, idx_ddl]
	
	def get_status(self):
		"""
			The method gets the status for all sources configured in the target database.
			:return: a list with the status data retrieved from the database.
			:rtype: list
		"""
		self.connect_db()
		sql_status = """
			SELECT 
				src.i_id_source,
				src.t_source as source_name,
				src.enm_status as  source_status,
				CASE
					WHEN rec.ts_last_received IS NULL
					THEN
						'N/A'::text
					ELSE
						(date_trunc('seconds',now())-ts_last_received)::text
				END AS receive_lag,
				coalesce(rec.ts_last_received::text,''),
				
				CASE
					WHEN rep.ts_last_replayed IS NULL
					THEN
						'N/A'::text
					ELSE
						(rec.ts_last_received-rep.ts_last_replayed)::text
				END AS replay_lag,
				coalesce(rep.ts_last_replayed::text,''),
				CASE
					WHEN src.b_consistent
					THEN
						'Yes'
					ELSE
						'No'
				END as consistent_status
				
				
			FROM 
				sch_chameleon.t_sources src
				LEFT JOIN sch_chameleon.t_last_received rec
				ON	src.i_id_source = rec.i_id_source
				LEFT JOIN sch_chameleon.t_last_replayed rep
				ON	src.i_id_source = rep.i_id_source
			;
			
		"""
		self.pgsql_cur.execute(sql_status)
		configuration_status = self.pgsql_cur.fetchall()
		self.disconnect_db()
		return configuration_status
	
	def insert_source_timings(self):
		"""
			The method inserts the source timings in the tables t_last_received and t_last_replayed.
			On conflict sets the replay/receive timestamps to null.
			The method assumes there is a database connection active.
		"""
		self.set_source_id()
		sql_replay = """
			INSERT INTO sch_chameleon.t_last_replayed
				(
					i_id_source
				)
			VALUES 
				(
					%s
				)
			ON CONFLICT (i_id_source)
			DO UPDATE 
				SET 
					ts_last_replayed=NULL
			;
		"""
		sql_receive = """
			INSERT INTO sch_chameleon.t_last_received
				(
					i_id_source
				)
			VALUES 
				(
					%s
				)
			ON CONFLICT (i_id_source)
			DO UPDATE 
				SET 
					ts_last_received=NULL
			;
		"""
		self.pgsql_cur.execute(sql_replay, (self.i_id_source, ))
		self.pgsql_cur.execute(sql_receive, (self.i_id_source, ))

	def  generate_default_statements(self, schema,  table, column, create_column=None):
		"""
			The method gets the default value associated with the table and column removing the cast.
			:param schema: The schema name
			:param table: The table name
			:param column: The column name
			:return: the statements for dropping and creating default value on the affected table
			:rtype: dictionary
		"""
		if not create_column:
			create_column = column
		
		regclass = """ "%s"."%s" """ %(schema, table)
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
		self.pgsql_cur.execute(sql_def_val, (regclass, column ))
		default_value = self.pgsql_cur.fetchone()
		query_drop_default = ""
		query_add_default = ""

		if default_value:
			query_drop_default = """ ALTER TABLE  "%s"."%s" ALTER COLUMN "%s" DROP DEFAULT;""" % (schema, table, column)
			query_add_default = """ ALTER TABLE  "%s"."%s" ALTER COLUMN "%s" SET DEFAULT %s ; """ % (schema, table, create_column, default_value[0])
		
		return {'drop':query_drop_default, 'create':query_add_default}


	def get_data_type(self, column, schema,  table):
		""" 
			The method determines whether the specified type has to be overridden or not.
			
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
	
	def set_application_name(self, action=""):
		"""
			The method sets the application name in the replica using the variable self.pg_conn.global_conf.source_name,
			Making simpler to find the replication processes. If the source name is not set then a generic PGCHAMELEON name is used.
		"""
		if self.source:
			app_name = "[pg_chameleon] - source: %s, action: %s" % (self.source, action)
		else:
			app_name = "[pg_chameleon] -  action: %s" % (action) 
		sql_app_name="""SET application_name=%s; """
		self.pgsql_cur.execute(sql_app_name, (app_name , ))
		
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
			event_after=row_data["event_after"]
			event_before=row_data["event_before"]
			log_table=global_data["log_table"]
			insert_list.append(self.pgsql_cur.mogrify("%s,%s,%s,%s,%s,%s,%s,%s,%s" ,  (
						global_data["batch_id"], 
						global_data["table"],  
						global_data["schema"], 
						global_data["action"], 
						global_data["binlog"], 
						global_data["logpos"], 
						json.dumps(event_after, cls=pg_encoder), 
						json.dumps(event_before, cls=pg_encoder), 
						global_data["event_time"], 
						
					)
				)
			)
											
		csv_data=b"\n".join(insert_list ).decode()
		csv_file.write(csv_data)
		csv_file.seek(0)
		try:
			sql_copy=sql.SQL("""
				COPY "sch_chameleon".{}
					(
						i_id_batch, 
						v_table_name, 
						v_schema_name, 
						enm_binlog_event, 
						t_binlog_name, 
						i_binlog_position, 
						jsb_event_after,
						jsb_event_before,
						i_my_event_time
					) 
				FROM 
					STDIN 
					WITH NULL 'NULL' 
					CSV QUOTE '''' 
					DELIMITER ',' 
					ESCAPE '''' 
				;
			""").format(sql.Identifier(log_table))
			self.pgsql_cur.copy_expert(sql_copy,csv_file)
		except psycopg2.Error as e:
			self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror))
			self.logger.error("fallback to inserts")
			self.insert_batch(group_insert)
		self.set_application_name("idle")
	
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
			event_after= row_data["event_after"]
			event_before= row_data["event_before"]
			log_table = global_data["log_table"]
			event_time = global_data["event_time"]
			sql_insert=sql.SQL("""
				INSERT INTO sch_chameleon.{}
					(
						i_id_batch, 
						v_table_name, 
						v_schema_name, 
						enm_binlog_event, 
						t_binlog_name, 
						i_binlog_position, 
						jsb_event_after,
						jsb_event_before,
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
			""").format(sql.Identifier(log_table))
			try:
				self.pgsql_cur.execute(sql_insert,(
						global_data["batch_id"], 
						global_data["table"],  
						global_data["schema"], 
						global_data["action"], 
						global_data["binlog"], 
						global_data["logpos"], 
						json.dumps(event_after, cls=pg_encoder), 
						json.dumps(event_before, cls=pg_encoder), 
						event_time
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
		byte_data = "%b" % row_data
		b64_row=base64.b64encode(byte_data)
		schema = row_data["schema"]
		table  = row_data["table"]
		print(b64_row)
		sql_save="""
			INSERT INTO sch_chameleon.t_discarded_rows
				(
					i_id_batch, 
					v_schema_name,
					v_table_name,
					t_row_data
				)
			VALUES 
				(
					%s,
					%s,
					%s,
					%s
				);
		"""
		self.pgsql_cur.execute(sql_save,(batch_id, schema, table,b64_row))
	
	
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
	
	def update_schema_mappings(self):
		"""
			The method updates the schema mappings for the given source.
			Before executing the updates the method checks for the need to run an update and for any
			mapping already present in the replica catalogue. 
			If everything is fine the database connection is set autocommit=false.
			The method updates the schemas  in the table t_replica_tables and then updates the mappings in the 
			table t_sources. After the final update the commit is issued to make the updates permanent.
			
			:todo: The method should run only at replica stopped for the given source. The method should also  replay all the logged rows for the given source before updating the schema mappings to avoid  to get an inconsistent replica.
		"""
		self.connect_db()
		self.set_source_id()
		self.replay_replica()
		new_schema_mappings = self.sources[self.source]["schema_mappings"]
		old_schema_mappings = self.get_schema_mappings()
		
		
		if new_schema_mappings != old_schema_mappings:
			duplicate_mappings = self.check_schema_mappings(True)
			if not duplicate_mappings:
				self.logger.debug("Updating schema mappings for source %s" % self.source)
				self.set_autocommit_db(False)
				for schema in old_schema_mappings:
					old_mapping = old_schema_mappings[schema]
					try:
						new_mapping = new_schema_mappings[schema]
					except KeyError:
						new_mapping = None
					if not new_mapping:
						self.logger.debug("The mapping for schema %s has ben removed. Deleting the reference from the replica catalogue." % (schema))
						sql_delete = """
							DELETE FROM sch_chameleon.t_replica_tables 
							WHERE 	
									i_id_source=%s
								AND	v_schema_name=%s
							;
						"""
						self.pgsql_cur.execute(sql_delete, (self.i_id_source,old_mapping ))
					elif old_mapping != new_mapping:
						self.logger.debug("Updating mapping for schema %s. Old: %s. New: %s" % (schema, old_mapping, new_mapping))
						sql_tables = """
							UPDATE sch_chameleon.t_replica_tables 
								SET v_schema_name=%s
							WHERE 	
									i_id_source=%s
								AND	v_schema_name=%s
							;
						"""
						self.pgsql_cur.execute(sql_tables, (new_mapping, self.i_id_source,old_mapping ))
						sql_alter_schema = sql.SQL("ALTER SCHEMA {} RENAME TO {};").format(sql.Identifier(old_mapping), sql.Identifier(new_mapping))
						self.pgsql_cur.execute(sql_alter_schema)
				sql_source="""
					UPDATE sch_chameleon.t_sources
						SET 
							jsb_schema_mappings=%s
					WHERE
						i_id_source=%s
					;
							
				"""
				self.pgsql_cur.execute(sql_source, (json.dumps(new_schema_mappings), self.i_id_source))
				self.pgsql_conn.commit()
					
				self.set_autocommit_db(True)
			else:
				self.logger.error("Could update the schema mappings for source %s. There is a duplicate destination schema in other sources. The offending schema is %s." % (self.source, duplicate_mappings[1]))
		else:
			self.logger.debug("The configuration file and catalogue mappings for source %s are the same. Not updating." % self.source)
		#print (self.i_id_source)
		
		self.disconnect_db()
	
	def get_schema_mappings(self):
		"""
			The method gets the schema mappings for the given source.
			The list is the one stored in the table sch_chameleon.t_sources. 
			Any change in the configuration file is ignored
			The method assumes there is a database connection active.
			:return: the schema mappings extracted from the replica catalogue
			:rtype: dictionary
	
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
			The method updates the source status for the source_name and sets the class attribute i_id_source.
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
	
	def set_source_id(self):
		"""
			The method sets the class attribute i_id_source for the self.source.
			The method assumes there is a database connection active.
		"""
		sql_source = """
			SELECT i_id_source FROM 
				sch_chameleon.t_sources
			WHERE
				t_source=%s
			;
			"""
		self.pgsql_cur.execute(sql_source, ( self.source, ))
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
	
	def clean_not_processed_batches(self):
		"""
			The method cleans up the not processed batches rows from the table sch_chameleon.t_log_replica.
			The method should be executed only before starting a replica process.
			The method assumes there is a database connection active.
		"""
		self.set_source_id()
		sql_cleanup = """
			DELETE FROM sch_chameleon.t_log_replica 
			WHERE 
				i_id_batch IN (
					SELECT 
						i_id_batch 
					FROM 
						sch_chameleon.t_replica_batch 
					WHERE 
							i_id_source=%s 
						and	not b_processed
					)
			;
		"""
		self.pgsql_cur.execute(sql_cleanup, (self.i_id_source, ))
	
	def check_source_consistent(self):
		"""
			This method checks if the database is consistent using the source's high watermark and the 
			source's flab b_consistent.
			If the batch data is larger than the source's high watermark then the source is marked consistent and
			all the log data stored witth the source's tables are set to null in order to ensure all the tables are replicated.
		"""
		
		sql_check_consistent = """
			WITH hwm AS
				(
					SELECT 
						split_part(t_binlog_name,'.',2)::integer as i_binlog_sequence,
						i_binlog_position 
					FROM 
						sch_chameleon.t_sources
					WHERE
							i_id_source=%s
						AND	not b_consistent

				)
			SELECT 
				CASE
					WHEN	bat.binlog_data[1]>hwm.i_binlog_sequence
					THEN 
						True
					WHEN		bat.binlog_data[1]=hwm.i_binlog_sequence
						AND	bat.binlog_data[2]>=hwm.i_binlog_position
					THEN 
						True
					ELSE
						False
				END AS b_consistent 
			FROM
				(
					SELECT 
						max(
							array[
								split_part(t_binlog_name,'.',2)::integer, 
								i_binlog_position
							]
						) as binlog_data
					FROM 
						sch_chameleon.t_replica_batch
					WHERE
							i_id_source=%s
						AND	b_started
						AND	b_processed

				) bat,
				hwm
			;

		"""
		self.pgsql_cur.execute(sql_check_consistent, (self.i_id_source, self.i_id_source, ))
		self.logger.debug("Checking consistent status for source: %s" %(self.source, ) )
		source_consistent = self.pgsql_cur.fetchone()
		if source_consistent:
			if source_consistent[0]:
				self.logger.info("The source: %s reached the consistent status" %(self.source, ) )
				sql_set_source_consistent = """
					UPDATE sch_chameleon.t_sources
						SET
							b_consistent=True,
							t_binlog_name=NULL,
							i_binlog_position=NULL
					WHERE
						i_id_source=%s
				;
				"""
				sql_set_tables_consistent = """
					UPDATE sch_chameleon.t_replica_tables
						SET
							t_binlog_name=NULL,
							i_binlog_position=NULL
					WHERE
						i_id_source=%s
				;
				"""
				self.pgsql_cur.execute(sql_set_source_consistent, (self.i_id_source,  ))
				self.pgsql_cur.execute(sql_set_tables_consistent, (self.i_id_source,  ))
			else:
				self.logger.debug("The source: %s is not consistent " %(self.source, ) )
		else:
			self.logger.debug("The source: %s is consistent" %(self.source, ) )
	
	def set_source_highwatermark(self, master_status, consistent):
		"""
			This method saves the master data within the source.
			The values are used to determine whether the database has reached the consistent point.
			
			:param master_status: the master data with the binlogfile and the log position
		"""
		master_data = master_status[0]
		binlog_name = master_data["File"]
		binlog_position = master_data["Position"]
		sql_set  = """
			UPDATE sch_chameleon.t_sources
				SET 
					b_consistent=%s,
					t_binlog_name=%s,
					i_binlog_position=%s
			WHERE
				i_id_source=%s
			;
					
		"""
		self.pgsql_cur.execute(sql_set, (consistent, binlog_name, binlog_position, self.i_id_source, ))
		self.logger.info("Set high watermark for source: %s" %(self.source, ) )
		
		
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
		
		sql_log_table="""
			UPDATE sch_chameleon.t_sources 
			SET 
				v_log_table=ARRAY[v_log_table[2],v_log_table[1]]
				
			WHERE 
				i_id_source=%s
			RETURNING 
				v_log_table[1]
			; 
		"""

		sql_last_update = """
			UPDATE 
				sch_chameleon.t_last_received  
			SET 
				ts_last_received=to_timestamp(%s)
			WHERE 
				i_id_source=%s
			RETURNING ts_last_received
		;
		"""
		
		try:
			self.pgsql_cur.execute(sql_master, (self.i_id_source, binlog_name, binlog_position))
			results =self.pgsql_cur.fetchone()
			next_batch_id=results[0]
			self.pgsql_cur.execute(sql_log_table, (self.i_id_source, ))
			results = self.pgsql_cur.fetchone()
			log_table_name = results[0]
			self.pgsql_cur.execute(sql_last_update, (event_time, self.i_id_source, ))
			results = self.pgsql_cur.fetchone()
			db_event_time = results[0]
			self.logger.info("Saved master data for source: %s" %(self.source, ) )
			self.logger.debug("Binlog file: %s" % (binlog_name, ))
			self.logger.debug("Binlog position:%s" % (binlog_position, ))
			self.logger.debug("Last event: %s" % (db_event_time, ))
			self.logger.debug("Next log table name: %s" % ( log_table_name, ))
			
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
							i_binlog_position = EXCLUDED.i_binlog_position,
							b_replica_enabled = True
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
			self.logger.warning("Missing primary key. The table %s.%s will not be replicated." % (schema, table,))
			self.unregister_table(schema,  table)

	
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
			except ValueError:
				self.logger.error("error when inserting the row, value not allowed")
			except:
				self.logger.error("unexpected error when processing the row")
				self.logger.error(" - > Table: %s.%s" % (schema, table))
	
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
				idx_col = [column.strip() for column in index["index_columns"].split(',')]
				index_columns = ['"%s"' % column.strip() for column in idx_col]
				non_unique = index["non_unique"]
				if indx =='PRIMARY':
					pkey_name = "pk_%s_%s_%s " % (table[0:10],table_timestamp,  self.idx_sequence)
					pkey_def = 'ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" PRIMARY KEY (%s) ;' % (schema, table, pkey_name, ','.join(index_columns))
					idx_ddl[pkey_name] = pkey_def
					table_primary = idx_col
				else:
					if non_unique == 0:
						unique_key = 'UNIQUE'
						if table_primary == []:
							table_primary = idx_col
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
			self.set_autocommit_db(False)
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
			self.logger.debug("Commit the swap transaction" )
			self.pgsql_conn.commit()
			self.set_autocommit_db(True)
	
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
		self.pgsql_cur.execute(sql_update, (id_batch, ))
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
		self.pgsql_cur.execute(sql_collect_events, (id_batch, ))
	
	def swap_tables(self):
		"""
			The method loops over the tables stored in the class 
		"""
		self.set_autocommit_db(False)
		for schema in self.schema_tables:
			schema_loading = self.schema_loading[schema]["loading"]
			schema_destination = self.schema_loading[schema]["destination"]
			for table in self.schema_tables[schema]:
				self.logger.info("Swapping table %s.%s with %s.%s" % (schema_destination, table, schema_loading, table))
				sql_drop_origin = sql.SQL("DROP TABLE {}.{} ;").format(sql.Identifier(schema_destination),sql.Identifier(table))
				sql_set_schema_new = sql.SQL("ALTER TABLE {}.{} SET SCHEMA {};").format(sql.Identifier(schema_loading),sql.Identifier(table), sql.Identifier(schema_destination))
				self.logger.debug("Dropping the original table %s.%s " % (schema_destination, table))
				self.pgsql_cur.execute(sql_drop_origin)
				self.logger.debug("Changing the schema for table %s.%s to %s" % (schema_loading, table, schema_destination))
				self.pgsql_cur.execute(sql_set_schema_new)
				self.pgsql_conn.commit()
				
		self.set_autocommit_db(True)
	
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
		self.set_lock_timeout()
		try:
			self.pgsql_cur.execute(sql_drop)
		except:
			self.logger.error("could not drop the schema %s. You will need to drop it manually." % schema_name)
