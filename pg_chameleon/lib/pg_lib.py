import psycopg2

class pg_connection:
	def __init__(self, global_config):
		self.global_conf=global_config()
		self.pg_conn=self.global_conf.pg_conn
		self.pg_database=self.global_conf.pg_database
		self.dest_schema=self.global_conf.my_database
		self.pg_connection=None
		self.pg_cursor=None
		
	
	def connect_db(self):
		pg_pars=dict(self.pg_conn.items()+ {'dbname':self.pg_database}.items())
		strconn="dbname=%(dbname)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % pg_pars
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		self.pgsql_cur=self.pgsql_conn .cursor()
		
	
	def disconnect_db(self):
		self.pgsql_conn.close()
		

class pg_engine:
	def __init__(self, global_config):
		self.pg_conn=pg_connection(global_config)
		self.pg_conn.connect_db()
		self.table_ddl=[]
		
	def create_schema(self):
		sql_schema=" CREATE SCHEMA IF NOT EXISTS "+self.pg_conn.dest_schema+";"
		sql_path=" SET search_path="+self.pg_conn.dest_schema+";"
		self.pg_conn.pgsql_cur.execute(sql_schema)
		self.pg_conn.pgsql_cur.execute(sql_path)
	
		
	
