import psycopg2
import os
import sys
import json
import datetime
class pg_encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.datetime):
			return str(obj)
		return json.JSONEncoder.default(self, obj)

class pg_connection:
	def __init__(self, global_config):
		self.global_conf=global_config()
		self.pg_conn=self.global_conf.pg_conn
		self.pg_database=self.global_conf.pg_database
		self.dest_schema=self.global_conf.my_database
		self.pg_connection=None
		self.pg_cursor=None
		self.pg_charset=self.global_conf.pg_charset
		
	
	def connect_db(self):
		pg_pars=dict(self.pg_conn.items()+ {'dbname':self.pg_database}.items())
		strconn="dbname=%(dbname)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s"  % pg_pars
		self.pgsql_conn = psycopg2.connect(strconn)
		self.pgsql_conn .set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		self.pgsql_conn .set_client_encoding(self.pg_charset)
		self.pgsql_cur=self.pgsql_conn .cursor()
		
	
	def disconnect_db(self):
		self.pgsql_conn.close()
		

class pg_engine:
	def __init__(self, global_config, table_metadata, table_file, sql_dir='sql/'):
		self.sql_dir=sql_dir
		self.idx_sequence=0
		self.pg_conn=pg_connection(global_config)
		self.pg_conn.connect_db()
		self.table_metadata=table_metadata
		self.table_file=table_file
		self.type_dictionary={
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
												'double':'float', 
												'float':'float', 
												'bit':'bool', 
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
	
	def store_table(self, table_name):
		table_data=self.table_metadata[table_name]
		for index in table_data["indices"]:
			if index["index_name"]=="PRIMARY":
				sql_insert=""" INSERT INTO sch_chameleon.t_replica_tables 
										(
											v_table_name,
											v_schema_name,
											v_table_pkey
										)
										VALUES (
														%s,
														%s,
														ARRAY[%s]
													)
										ON CONFLICT (v_table_name,v_schema_name)
											DO UPDATE 
												SET v_table_pkey=EXCLUDED.v_table_pkey
										;
								"""
				self.pg_conn.pgsql_cur.execute(sql_insert, (table_name, self.pg_conn.dest_schema, index["index_columns"]))	
	
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
				self.store_table(table)
	
	def create_indices(self):
		print "creating indices"
		for index in self.idx_ddl:
			idx_ddl= self.idx_ddl[index]
			for sql_idx in idx_ddl:
				
				self.pg_conn.pgsql_cur.execute(sql_idx)
	
	def copy_data(self, table,  csv_file,  my_tables={}):
		column_copy=[]
		for column in my_tables[table]["columns"]:
			column_copy.append('"'+column["column_name"]+'"')
		sql_copy="COPY "+'"'+table+'"'+" ("+','.join(column_copy)+") FROM STDIN WITH NULL 'NULL' CSV QUOTE '\"' DELIMITER',' ESCAPE '\"' ; "
		self.pg_conn.pgsql_cur.copy_expert(sql_copy,csv_file)
		

	
	def push_data(self, table_file=[], my_tables={}):
		
		if len(table_file)==0:
			print "table to file list is empty"
		else:
			for table in table_file:
				column_copy=[]
				for column in my_tables[table]["columns"]:
					column_copy.append('"'+column["column_name"]+'"')
				sql_copy="COPY "+'"'+table+'"'+" ("+','.join(column_copy)+") FROM STDIN WITH NULL 'NULL' CSV QUOTE '\"' DELIMITER',' ESCAPE '\"'  "
				tab_file=open(table_file[table],'rb')
				self.pg_conn.pgsql_cur.copy_expert(sql_copy,tab_file)
				tab_file.close()
				
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
				ddl_columns.append('"'+column["column_name"]+'" '+column_type+" "+col_is_null )
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
	
	def create_service_schema(self, cleanup=False):
		if cleanup:
			self.drop_service_schema()
			
		file_schema=open(self.sql_dir+"create_schema.sql", 'rb')
		sql_schema=file_schema.read()
		file_schema.close()
		self.pg_conn.pgsql_cur.execute(sql_schema)

		
	def drop_service_schema(self):
		file_schema=open(self.sql_dir+"drop_schema.sql", 'rb')
		sql_schema=file_schema.read()
		file_schema.close()
		self.pg_conn.pgsql_cur.execute(sql_schema)
	
	def save_master_status(self, master_status):
		master_data=master_status[0]
		binlog_name=master_data["File"]
		binlog_position=master_data["Position"]
		sql_master="""
							INSERT INTO sch_chameleon.t_replica_batch
															(
																t_binlog_name, 
																i_binlog_position
															)
												VALUES (
																%s,
																%s
															)
						"""
		print "saving master data"
		self.pg_conn.pgsql_cur.execute(sql_master, (binlog_name, binlog_position))
		print "done"
		
	def get_batch_data(self):
		sql_batch="""WITH t_created AS
						(
							SELECT 
								max(ts_created) AS ts_created
							FROM 
								sch_chameleon.t_replica_batch  
							WHERE 
									NOT b_processed
						)
					UPDATE sch_chameleon.t_replica_batch
						SET b_started=True
						FROM 
							t_created
						WHERE
							t_replica_batch.ts_created=t_created.ts_created
					RETURNING
						i_id_batch,
						t_binlog_name,
						i_binlog_position 
					;
					"""
		self.pg_conn.pgsql_cur.execute(sql_batch)
		return self.pg_conn.pgsql_cur.fetchall()
	
	def write_batch(self, group_insert):
		insert_list=[]
		for row_data in group_insert:
			global_data=row_data["global_data"]
			event_data=row_data["event_data"]
			insert_list.append(self.pg_conn.pgsql_cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", (
																									global_data["batch_id"], 
																									global_data["table"],  
																									global_data["schema"], 
																									global_data["action"], 
																									global_data["binlog"], 
																									global_data["logpos"], 
																									json.dumps(event_data, cls=pg_encoder)
																								)
																		)
											)
			
		sql_insert="""
								INSERT INTO sch_chameleon.t_log_replica
								(
									i_id_batch, 
									v_table_name, 
									v_schema_name, 
									v_binlog_event, 
									t_binlog_name, 
									i_binlog_position, 
									jsb_event_data
								)
								VALUES
									"""+ ','.join(insert_list )+"""
						"""
		self.pg_conn.pgsql_cur.execute(sql_insert)
	
	def set_batch_processed(self, id_batch):
		sql_update=""" UPDATE sch_chameleon.t_replica_batch
										SET
												b_processed=True
								WHERE
										i_id_batch=%s
								;
							"""
		self.pg_conn.pgsql_cur.execute(sql_update, (id_batch, ))
		
	def process_batch(self):
		sql_process="""SELECT sch_chameleon.fn_process_batch();"""
		self.pg_conn.pgsql_cur.execute(sql_process)
		
