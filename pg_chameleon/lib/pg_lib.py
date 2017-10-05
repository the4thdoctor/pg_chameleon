import psycopg2
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
			The method disconnects the postgres connection if there is any active. Otherwise issues a warning.
		"""
		if self.pgsql_conn:
			self.logger.debug("Trying to disconnect from the destination database.")
			self.pgsql_conn.close()
		else:
			self.logger.debug("There is no database connection to disconnect.")

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
			
			
			
			
			
			
