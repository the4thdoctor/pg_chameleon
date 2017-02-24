import io
import pymysql
import sys
import codecs
import binascii
import datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import RotateEvent
from pg_chameleon import sql_token

class mysql_connection(object):
	def __init__(self, global_config):
		self.global_conf=global_config
		self.my_server_id=self.global_conf.my_server_id
		self.mysql_conn=self.global_conf.mysql_conn
		self.my_database=self.global_conf.my_database
		self.my_charset=self.global_conf.my_charset
		self.tables_limit=self.global_conf.tables_limit
		self.replica_batch_size=self.global_conf.replica_batch_size
		self.copy_mode=self.global_conf.copy_mode
		self.my_connection=None
		self.my_cursor=None
		self.my_cursor_fallback=None
		
	def connect_db_ubf(self):
		"""  Establish connection with the database """
		self.my_connection_ubf=pymysql.connect(host=self.mysql_conn["host"],
							user=self.mysql_conn["user"],
							password=self.mysql_conn["passwd"],
							db=self.my_database,
							charset=self.my_charset,
							cursorclass=pymysql.cursors.SSCursor)
		self.my_cursor_ubf=self.my_connection_ubf.cursor()

		
	
	def connect_db(self):
		"""  Establish connection with the database """
		self.my_connection=pymysql.connect(host=self.mysql_conn["host"],
							user=self.mysql_conn["user"],
							password=self.mysql_conn["passwd"],
							db=self.my_database,
							charset=self.my_charset,
							cursorclass=pymysql.cursors.DictCursor)
		self.my_cursor=self.my_connection.cursor()
		self.my_cursor_fallback=self.my_connection.cursor()
	
	def disconnect_db(self):
		try:
			self.my_connection.close()
		except:
			pass
	def disconnect_db_ubf(self):
		try:
			self.my_connection_ubf.close()
		except:
			pass
		
class mysql_engine(object):
	def __init__(self, global_config, logger, out_dir="/tmp/"):
		self.hexify=global_config.hexify
		self.logger=logger
		self.out_dir=out_dir
		self.my_tables={}
		self.table_file={}
		self.mysql_con=mysql_connection(global_config)
		self.mysql_con.connect_db()
		self.get_table_metadata()
		self.my_streamer=None
		self.replica_batch_size=self.mysql_con.replica_batch_size
		self.master_status=[]
		self.id_batch=None
		self.sql_token=sql_token()
		self.pause_on_reindex=global_config.pause_on_reindex
	
	def normalise_query(self, parsed_query):
		"""
			Normalise a query the parsed query in in order to have a standard way to replicate the DDL on PostgreSQL
			The relation's medatada is extracted from mysql's information schema
			:param query: the query string to normalise
		"""
		
	
				
	def read_replica(self, batch_data, pg_engine):
		"""
		Stream the replica using the batch data.
		:param batch_data: The list with the master's batch data.
		"""
		table_type_map=self.get_table_type_map()	
		schema_name=pg_engine.dest_schema
		close_batch=False
		total_events=0
		master_data={}
		group_insert=[]
		id_batch=batch_data[0][0]
		log_file=batch_data[0][1]
		log_position=batch_data[0][2]
		log_table=batch_data[0][3]
		my_stream = BinLogStreamReader(
									connection_settings = self.mysql_con.mysql_conn, 
									server_id=self.mysql_con.my_server_id, 
									only_events=[RotateEvent, DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent], 
									log_file=log_file, 
									log_pos=log_position, 
									resume_stream=True, 
									only_schemas=[self.mysql_con.my_database]
									)
		self.logger.debug("log_file %s, log_position %s. id_batch: %s " % (log_file, log_position, id_batch))
		for binlogevent in my_stream:
			
			total_events+=1
			#self.logger.debug("log_file %s, log_position %s. id_batch: %s replica_batch_size:%s total_events:%s " % (log_file, log_position, id_batch, self.replica_batch_size, total_events))
			if isinstance(binlogevent, RotateEvent):
				event_time=binlogevent.timestamp
				binlogfile=binlogevent.next_binlog
				position=binlogevent.position
				self.logger.debug("rotate event. binlogfile %s, position %s. " % (binlogfile, position))
				if close_batch:
					if log_file!=binlogfile:
						master_data["File"]=binlogfile
						master_data["Position"]=position
						master_data["Time"]=event_time
					if len(group_insert)>0:
						pg_engine.write_batch(group_insert)
						group_insert=[]
					my_stream.close()
					return [master_data, close_batch]
			elif isinstance(binlogevent, QueryEvent):
				event_time=binlogevent.timestamp
				if len(group_insert)>0:
					pg_engine.write_batch(group_insert)
					group_insert=[]
				master_data["File"]=binlogfile
				master_data["Position"]=binlogevent.packet.log_pos
				master_data["Time"]=event_time
				self.sql_token.parse_sql(binlogevent.query)
				
				for token in self.sql_token.tokenised:
					if len(token)>0:
						#schema_name=binlogevent.schema.decode()
						
						query_data={
									"binlog":log_file, 
									"logpos":log_position, 
									"schema": schema_name, 
									"batch_id":id_batch, 
									"log_table":log_table
						}
						pg_engine.write_ddl(token, query_data)
						close_batch=True
					
				self.sql_token.reset_lists()
				if close_batch:
					my_stream.close()
					return [master_data, close_batch]

				
			else:
				for row in binlogevent.rows:
					total_events+=1
					log_file=binlogfile
					log_position=binlogevent.packet.log_pos
					table_name=binlogevent.table
					event_time=binlogevent.timestamp
					#self.logger.debug("row event. binlogfile %s, position %s. Date %s " % (binlogfile, log_position, datetime.datetime.fromtimestamp(event_time).isoformat()))
					#schema_name=binlogevent.schema

					column_map=table_type_map[table_name]
					global_data={
										"binlog":log_file, 
										"logpos":log_position, 
										"schema": schema_name, 
										"table": table_name, 
										"batch_id":id_batch, 
										"log_table":log_table
									}
					event_data={}
					event_update={}
					if isinstance(binlogevent, DeleteRowsEvent):
						global_data["action"] = "delete"
						event_data=row["values"]
					elif isinstance(binlogevent, UpdateRowsEvent):
						global_data["action"] = "update"
						event_data=row["after_values"]
						event_update=row["before_values"]
					elif isinstance(binlogevent, WriteRowsEvent):
						global_data["action"] = "insert"
						event_data=row["values"]
					for column_name in event_data:
						column_type=column_map[column_name]
						if column_type in self.hexify and event_data[column_name]:
							event_data[column_name]=binascii.hexlify(event_data[column_name])
					event_insert={"global_data":global_data,"event_data":event_data,  "event_update":event_update}
					group_insert.append(event_insert)
					#self.logger.debug("Action: %s Total events: %s " % (global_data["action"],  total_events))
					master_data["File"]=log_file
					master_data["Position"]=log_position
					master_data["Time"]=event_time
					if total_events>=self.replica_batch_size:
						self.logger.debug("total events exceeded. Writing batch.: %s  " % (master_data,  ))
						total_events=0
						pg_engine.write_batch(group_insert)
						group_insert=[]
						close_batch=True
						
		my_stream.close()
		if len(group_insert)>0:
			pg_engine.write_batch(group_insert)
			close_batch=True
		return [master_data, close_batch]

	def run_replica(self, pg_engine):
		"""
		Reads the MySQL replica and stores the data in postgres. 
		
		:param pg_engine: The postgresql engine object required for storing the master coordinates and replaying the batches
		"""
		if self.pause_on_reindex:
			pg_engine.check_reindex()
		batch_data=pg_engine.get_batch_data()
		self.logger.debug('batch data: %s' % (batch_data, ))
		if len(batch_data)>0:
			id_batch=batch_data[0][0]
			replica_data=self.read_replica(batch_data, pg_engine)
			master_data=replica_data[0]
			close_batch=replica_data[1]
			if close_batch:
				self.master_status=[]
				self.master_status.append(master_data)
				self.logger.debug("trying to save the master data...")
				next_id_batch=pg_engine.save_master_status(self.master_status)
				if next_id_batch:
					self.logger.debug("new batch created, saving id_batch %s in class variable" % (id_batch))
					self.id_batch=id_batch
				else:
					self.logger.debug("batch not saved. using old id_batch %s" % (self.id_batch))
				if self.id_batch:
					self.logger.debug("updating processed flag for id_batch %s", (id_batch))
					pg_engine.set_batch_processed(id_batch)
					self.id_batch=None
		self.logger.debug("replaying batch.")
		pg_engine.process_batch(self.replica_batch_size)

	def get_table_type_map(self):
		table_type_map={}
		self.logger.debug("collecting table type map")
		sql_tables="""	SELECT 
						table_schema,
						table_name
					FROM 
						information_schema.TABLES 
					WHERE 
							table_type='BASE TABLE' 
						AND	table_schema=%s
					;
							"""
		self.mysql_con.my_cursor.execute(sql_tables, (self.mysql_con.my_database))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			column_type={}
			sql_columns="""SELECT 
												column_name,
												data_type
												
									FROM 
												information_schema.COLUMNS 
									WHERE 
															table_schema=%s
												AND 	table_name=%s
									ORDER BY 
													ordinal_position
									;
								"""
			self.mysql_con.my_cursor.execute(sql_columns, (self.mysql_con.my_database, table["table_name"]))
			column_data=self.mysql_con.my_cursor.fetchall()
			for column in column_data:
				column_type[column["column_name"]]=column["data_type"]
			table_type_map[table["table_name"]]=column_type
		return table_type_map
		
			
		
	def get_column_metadata(self, table):
		sql_columns="""SELECT 
											column_name,
											column_default,
											ordinal_position,
											data_type,
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
											END AS enum_list,
											CASE
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned)')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_csv,
											CASE
												WHEN 
													data_type IN ('"""+"','".join(self.hexify)+"""')
												THEN
													concat('hex(',column_name,')')
												WHEN 
													data_type IN ('bit')
												THEN
													concat('cast(`',column_name,'` AS unsigned) AS','`',column_name,'`')
											ELSE
												concat('`',column_name,'`')
											END
											AS column_select
								FROM 
											information_schema.COLUMNS 
								WHERE 
														table_schema=%s
											AND 	table_name=%s
								ORDER BY 
												ordinal_position
								;
							"""
		self.mysql_con.my_cursor.execute(sql_columns, (self.mysql_con.my_database, table))
		column_data=self.mysql_con.my_cursor.fetchall()
		return column_data

	def get_index_metadata(self, table):
		sql_index="""SELECT 
										index_name,
										non_unique,
										GROUP_CONCAT(concat('"',column_name,'"') ORDER BY seq_in_index) as index_columns
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
		self.mysql_con.my_cursor.execute(sql_index, (self.mysql_con.my_database, table))
		index_data=self.mysql_con.my_cursor.fetchall()
		return index_data
	
	def get_table_metadata(self):
		self.logger.debug("getting table metadata")
		table_include=""
		if self.mysql_con.tables_limit:
			self.logger.info("table copy limited to tables: %s" % ','.join(self.mysql_con.tables_limit))
			table_include="AND table_name IN ('"+"','".join(self.mysql_con.tables_limit)+"')"
		sql_tables="""SELECT 
											table_schema,
											table_name
								FROM 
											information_schema.TABLES 
								WHERE 
														table_type='BASE TABLE' 
											AND 	table_schema=%s
											"""+table_include+"""
								;
							"""
		
		self.mysql_con.my_cursor.execute(sql_tables, (self.mysql_con.my_database))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			column_data=self.get_column_metadata(table["table_name"])
			index_data=self.get_index_metadata(table["table_name"])
			dic_table={'name':table["table_name"], 'columns':column_data,  'indices': index_data}
			self.my_tables[table["table_name"]]=dic_table
	
	def print_progress (self, iteration, total, table_name):
		if total>1:
			self.logger.info("Table %s copied %d %%" % (table_name, 100 * float(iteration)/float(total)))
		else:
			self.logger.debug("Table %s copied %d %%" % (table_name, 100 * float(iteration)/float(total)))
		
	def generate_select(self, table_columns, mode="csv"):
		column_list=[]
		columns=""
		if mode=="csv":
			for column in table_columns:
					column_list.append("COALESCE(REPLACE("+column["column_csv"]+", '\"', '\"\"'),'NULL') ")
			columns="REPLACE(CONCAT('\"',CONCAT_WS('\",\"',"+','.join(column_list)+"),'\"'),'\"NULL\"','NULL')"
		if mode=="insert":
			for column in table_columns:
				column_list.append(column["column_select"])
			columns=','.join(column_list)
		return columns
	

	def insert_table_data(self, pg_engine, ins_arg):
		"""fallback to inserts for table and slices """
		slice_insert=ins_arg[0]
		table_name=ins_arg[1]
		columns_ins=ins_arg[2]
		copy_limit=ins_arg[3]
		for slice in slice_insert:
			sql_out="SELECT "+columns_ins+"  FROM "+table_name+" LIMIT "+str(slice*copy_limit)+", "+str(copy_limit)+";"
			self.mysql_con.my_cursor_fallback.execute(sql_out)
			insert_data =  self.mysql_con.my_cursor_fallback.fetchall()
			pg_engine.insert_data(table_name, insert_data , self.my_tables)

	def copy_table_data(self, pg_engine,  copy_max_memory):
		out_file='/tmp/output_copy.csv'
		self.logger.info("locking the tables")
		self.lock_tables()
		table_list = []
		if pg_engine.table_limit[0] == '*':
			for table_name in self.my_tables:
				table_list.append(table_name)
		else:
			table_list = pg_engine.table_limit
		for table_name in table_list:
			slice_insert=[]
			self.logger.info("copying table "+table_name)
			table=self.my_tables[table_name]
			
			table_name=table["name"]
			table_columns=table["columns"]
			self.logger.debug("estimating rows in "+table_name)
			sql_count=""" 
								SELECT 
										table_rows,
										CASE
											WHEN avg_row_length>0
											then
												round(("""+copy_max_memory+"""/avg_row_length))
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
			self.mysql_con.my_cursor.execute(sql_count, (self.mysql_con.my_database, table_name))
			count_rows=self.mysql_con.my_cursor.fetchone()
			total_rows=count_rows["table_rows"]
			copy_limit=int(count_rows["copy_limit"])
			if copy_limit == 0:
				copy_limit=1000000
			num_slices=int(total_rows//copy_limit)
			range_slices=list(range(num_slices+1))
			total_slices=len(range_slices)
			slice=range_slices[0]
			self.logger.debug("%s will be copied in %s slices of %s rows"  % (table_name, total_slices, copy_limit))
			columns_csv=self.generate_select(table_columns, mode="csv")
			columns_ins=self.generate_select(table_columns, mode="insert")
			csv_data=""
			sql_out="SELECT "+columns_csv+" as data FROM "+table_name+";"
			self.mysql_con.connect_db_ubf()
			try:
				self.logger.debug("Executing query for table %s"  % (table_name, ))
				self.mysql_con.my_cursor_ubf.execute(sql_out)
			except:
				self.logger.error("error when pulling data from %s. sql executed: " % (table_name, sql_out))
			
			self.logger.debug("Starting extraction loop for table %s"  % (table_name, ))
			while True:
				csv_results = self.mysql_con.my_cursor_ubf.fetchmany(copy_limit)
				if len(csv_results) == 0:
					break
				csv_data="\n".join(d[0] for d in csv_results )
				
				if self.mysql_con.copy_mode=='direct':
					csv_file=io.StringIO()
					csv_file.write(csv_data)
					csv_file.seek(0)

				if self.mysql_con.copy_mode=='file':
					csv_file=codecs.open(out_file, 'wb', self.mysql_con.my_charset)
					csv_file.write(csv_data)
					csv_file.close()
					csv_file=open(out_file, 'rb')
					
				try:
					pg_engine.copy_data(table_name, csv_file, self.my_tables)
				except:
					self.logger.info("table %s error in PostgreSQL copy, saving slice number for the fallback to insert statements " % (table_name, ))
					slice_insert.append(slice)
					
				self.print_progress(slice+1,total_slices, table_name)
				slice+=1
				csv_file.close()
			self.mysql_con.disconnect_db_ubf()
			if len(slice_insert)>0:
				ins_arg=[]
				ins_arg.append(slice_insert)
				ins_arg.append(table_name)
				ins_arg.append(columns_ins)
				ins_arg.append(copy_limit)
				self.insert_table_data(pg_engine, ins_arg)
		self.logger.info("releasing the lock")
		self.unlock_tables()
		
	def get_master_status(self):
		t_sql_master="SHOW MASTER STATUS;"
		self.mysql_con.my_cursor.execute(t_sql_master)
		self.master_status=self.mysql_con.my_cursor.fetchall()		
		
		
	def lock_tables(self):
		""" lock tables and get the log coords """
		self.locked_tables=[]
		for table_name in self.my_tables:
			table=self.my_tables[table_name]
			self.locked_tables.append(table["name"])
		t_sql_lock="FLUSH TABLES "+", ".join(self.locked_tables)+" WITH READ LOCK;"
		self.mysql_con.my_cursor.execute(t_sql_lock)
		self.get_master_status()
	
	def unlock_tables(self):
		""" unlock tables previously locked """
		t_sql_unlock="UNLOCK TABLES;"
		self.mysql_con.my_cursor.execute(t_sql_unlock)
			
			
	def __del__(self):
		self.mysql_con.disconnect_db()
