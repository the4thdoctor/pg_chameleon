import psycopg2
import os
import sys
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
	def __init__(self, global_config, table_metadata, table_file):
		self.pg_conn=pg_connection(global_config)
		self.pg_conn.connect_db()
		self.table_metadata=table_metadata
		self.table_file=table_file
		self.type_dictionary={
												'integer':'integer',
												'mediumint':'bigint',
												'tinyint':'integer',
												'smallint':'integer',
												'int':'bigint',
												'varchar':'character varying',
												'bigint':'bigint',
												'text':'text',
												'char':'character',
												'datetime':'timestamp without time zone',
												'timestamp':'timestamp without time zone',
												'longtext':'text',
												'tinytext':'text',
												'tinyblob':'bytea',
												'mediumblob':'bytea',
												'longblob':'bytea',
												'blob':'bytea', 
												'decimal':'numeric', 
												'double':'float', 
												'bit':'bit', 
												'year':'integer', 
												'enum':'enum', 
												'set':'text'
										}
		self.table_ddl={}
		self.idx_ddl={}
		self.type_ddl={}
		
	def create_schema(self):
		sql_schema=" CREATE SCHEMA IF NOT EXISTS "+self.pg_conn.dest_schema+";"
		sql_path=" SET search_path="+self.pg_conn.dest_schema+";"
		self.pg_conn.pgsql_cur.execute(sql_schema)
		self.pg_conn.pgsql_cur.execute(sql_path)
	
	def create_tables(self, drop_tables=False):
		
			for table in self.table_ddl:
				if drop_tables:
					sql_drop='DROP TABLE IF EXISTS "'+table+'" CASCADE ;'
					self.pg_conn.pgsql_cur.execute(sql_drop)
				try:
					sql_type=self.type_ddl[table]
					self.pg_conn.pgsql_cur.execute(sql_type)
				except:
					pass
				sql_create=self.table_ddl[table]
				try:
					self.pg_conn.pgsql_cur.execute(sql_create)
				except psycopg2.Error as e:
					print  "SQLCODE: " + e.pgcode+ " - " +e.pgerror
					print sql_create
	
	def create_indices(self):
		print "creating indices"
		for index in self.idx_ddl:
			idx_ddl= self.idx_ddl[index]
			for sql_idx in idx_ddl:
				print sql_idx
				self.pg_conn.pgsql_cur.execute(sql_idx)
	
	def push_data(self, table_file=[], my_tables={}):
		#print my_tables
		#sys.exit()
		
		if len(table_file)==0:
			print "table to file list is empty"
		else:
			for table in table_file:
				tmp_table="tmp_"+table
				sql_create_temp="CREATE TEMPORARY TABLE "+tmp_table+" (LIKE "+table+");"
				sql_drop_temp="DROP TABLE "+tmp_table+" ;"
				self.pg_conn.pgsql_cur.execute(sql_create_temp)
				column_copy=[]
				column_import=[]
				for column in my_tables[table]["columns"]:
					column_copy.append(column["column_name"])
					column_import.append(column["column_import"])
				sql_copy="COPY "+'"'+tmp_table+'"'+" ("+','.join(column_copy)+") FROM STDIN WITH NULL 'NULL' CSV QUOTE '\"' DELIMITER',' ESCAPE '\"'  "
				sql_insert="INSERT INTO "+table+" ("+','.join(column_copy)+") SELECT "+','.join(column_copy)+" FROM "+'"'+tmp_table+'"'+";"
				
				tab_file=open(table_file[table],'rb')
				self.pg_conn.pgsql_cur.copy_expert(sql_copy,tab_file)
				tab_file.close()
				try:
					self.pg_conn.pgsql_cur.execute(sql_insert)
					print "import successful, removing the file "+table_file[table]
					os.remove(table_file[table])
				except psycopg2.Error as e:
					print  "SQLCODE: " + e.pgcode+ " - " +e.pgerror
					print sql_insert
					
				
				self.pg_conn.pgsql_cur.execute(sql_drop_temp)
				
	def build_tab_ddl(self):
		""" the function iterates over the list l_tables and builds a new list with the statements for tables"""
		
		for table_name in self.table_metadata:
			table=self.table_metadata[table_name]
			columns=table["columns"]
			
			ddl_head="CREATE TABLE "+'"'+table["name"]+'" ('
			ddl_tail=");"
			ddl_columns=[]
			for column in columns:
				if column["is_nullable"]=="NO":
					col_is_null="NOT NULL"
				else:
					col_is_null="NULL"
				column_type=self.type_dictionary[column["data_type"]]
				if column_type=="enum":
					enum_type="enum_"+table["name"]+"_"+column["column_name"]
					sql_enum="CREATE TYPE "+enum_type+" AS ENUM "+column["enum_list"]+";"
					self.type_ddl[table["name"]]=sql_enum
					column_type=enum_type
				if column_type=="character varying" or column_type=="character":
					column_type=column_type+"("+str(column["character_maximum_length"])+")"
				if column_type=='numeric':
					column_type=column_type+"("+str(column["numeric_precision"])+","+str(column["numeric_scale"])+")"
				if column_type=='bit' or column_type=='float':
					column_type=column_type+"("+str(column["numeric_precision"])+")"
				if column["extra"]=="auto_increment":
					column_type="bigserial"
				ddl_columns.append(column["column_name"]+" "+column_type+" "+col_is_null )
			def_columns=str(',').join(ddl_columns)
			self.table_ddl[table["name"]]=ddl_head+def_columns+ddl_tail
	
	def build_idx_ddl(self):
		
		""" the function iterates over the list l_pkeys and builds a new list with the statements for pkeys """
		for table_name in self.table_metadata:
			table=self.table_metadata[table_name]
			
			table_name=table["name"]
			indices=table["indices"]
			table_idx=[]
			for index in indices:
				index_name=index["index_name"]
				index_columns=index["index_columns"]
				non_unique=index["non_unique"]
				if index_name=='PRIMARY':
					pkey_name="pk_"+table_name
					pkey_def='ALTER TABLE "'+table_name+'" ADD CONSTRAINT "'+pkey_name+'" PRIMARY KEY ('+index_columns+') ;'
					table_idx.append(pkey_def)
				else:
					if non_unique==0:
						unique_key='UNIQUE'
					else:
						unique_key=''
					idx_def='CREATE '+unique_key+' INDEX "idx_'+ index_name+'_'+table_name+'" ON '+table_name+' ('+index_columns+');'
					table_idx.append(idx_def)
					
			self.idx_ddl[table_name]=table_idx
	
