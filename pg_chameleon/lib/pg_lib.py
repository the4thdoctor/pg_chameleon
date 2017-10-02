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
		if 	isinstance(obj, datetime.time) or \
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
			print("Undefined connection parameters. Please fix it.")
			sys.exit()

	def disconnect_db(self):
		"""
			The method disconnects the postgres connection if there is any active. Otherwise issues a warning.
			Todo: Need to change the print in logging.
		"""
		if self.pgsql_conn:
			self.pgsql_conn.close()
		else:
			print("Warning, there is no database connection to disconnect.")
