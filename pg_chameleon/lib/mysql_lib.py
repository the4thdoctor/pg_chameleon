import StringIO
import pymysql
import sys
import codecs

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import RotateEvent

class mysql_connection:
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
		self.my_connection.close()
		
		
class mysql_engine:
	def __init__(self, global_config, logger, out_dir="/tmp/"):
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
		

	def do_stream_data(self, pg_engine):
		group_insert=[]
		master_data={}
		num_insert=0
			
		batch_data=pg_engine.get_batch_data()
		if len(batch_data)>0:
			self.logger.debug("start replica stream: %s", (batch_data, ))
			id_batch=batch_data[0][0]
			log_file=batch_data[0][1]
			log_position=batch_data[0][2]
			log_table=batch_data[0][3]
			self.my_stream = BinLogStreamReader(
																	connection_settings = self.mysql_con.mysql_conn, 
																	server_id=self.mysql_con.my_server_id, 
																	only_events=[RotateEvent,DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], 
																	log_file=log_file, 
																	log_pos=log_position, 
																	resume_stream=True
															)
															
			for binlogevent in self.my_stream:
				if isinstance(binlogevent, RotateEvent):
					binlogfile=binlogevent.next_binlog
				else:
					for row in binlogevent.rows:
						log_file=binlogfile
						log_position=binlogevent.packet.log_pos
						global_data={
											"binlog":log_file, 
											"logpos":log_position, 
											"schema": binlogevent.schema, 
											"table": binlogevent.table, 
											"batch_id":id_batch, 
											"log_table":log_table
										}
						event_data={}
						if isinstance(binlogevent, DeleteRowsEvent):
							global_data["action"] = "delete"
							event_data = dict(event_data.items() + row["values"].items())
						elif isinstance(binlogevent, UpdateRowsEvent):
							global_data["action"] = "update"
							event_data = dict(event_data.items() + row["after_values"].items())
						elif isinstance(binlogevent, WriteRowsEvent):
							global_data["action"] = "insert"
							event_data = dict(event_data.items() + row["values"].items())
						event_insert={"global_data":global_data,"event_data":event_data}
						group_insert.append(event_insert)
						num_insert+=1
						if num_insert>=self.replica_batch_size:
							pg_engine.write_batch(group_insert)
							num_insert=0
							group_insert=[]
			if len(group_insert)>0:
				pg_engine.write_batch(group_insert)

		
			
			master_data["File"]=log_file
			master_data["Position"]=log_position
			self.logger.debug("master data: logfile %s log position ", (log_file, log_position))
			try:
				self.master_status=[]
				self.master_status.append(master_data)
				self.logger.debug("trying to save the master data...")
				pg_engine.save_master_status(self.master_status)
				self.logger.debug("success, saving id_batch %s in class variable", (id_batch))
				self.id_batch=id_batch
				
			except:
				self.logger.debug("failure, means empty batch. using old id_batch %s", (self.id_batch))
				
			if self.id_batch:
				self.logger.debug("updating processed flag for id_batch %s", (id_batch))
				pg_engine.set_batch_processed(id_batch)
				self.id_batch=None
			self.logger.debug("closing replication stream")
			self.my_stream.close()
		
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
													data_type IN ('blob','tinyblob','longblob','binary')
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
													data_type IN ('blob','tinyblob','longblob','binary')
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
		self.logger.info("getting table metadata")
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
		
	def copy_table_data(self, pg_engine,  limit=10000):
		out_file='/tmp/output_copy.csv'
		self.logger.info("locking the tables")
		self.lock_tables()
		for table_name in self.my_tables:
			self.logger.info("copying table "+table_name)
			table=self.my_tables[table_name]
			
			table_name=table["name"]
			table_columns=table["columns"]
			self.logger.debug("counting rows in "+table_name)
			sql_count="SELECT count(*) as i_cnt FROM `"+table_name+"` ;"
			self.mysql_con.my_cursor.execute(sql_count)
			count_rows=self.mysql_con.my_cursor.fetchone()
			num_slices=count_rows["i_cnt"]/limit
			range_slices=range(num_slices+1)
			total_slices=len(range_slices)
			self.logger.debug(table_name +" will be copied in "+str(total_slices)+" slices" )
			columns_csv=self.generate_select(table_columns, mode="csv")
			columns_ins=self.generate_select(table_columns, mode="insert")
			
			
			for slice in range_slices:
				csv_data=""
				sql_out="SELECT "+columns_csv+" as data FROM "+table_name+" LIMIT "+str(slice*limit)+", "+str(limit)+";"
				try:
					self.mysql_con.my_cursor.execute(sql_out)
				except:
					print sql_out
				csv_results = self.mysql_con.my_cursor.fetchall()
				
				csv_data="\n".join(d['data'] for d in csv_results )
				
				if self.mysql_con.copy_mode=='direct':
					csv_file=StringIO.StringIO()
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
					print "error in PostgreSQL copy, fallback to insert statements "
					
					sql_out="SELECT "+columns_ins+"  FROM "+table_name+" LIMIT "+str(slice*limit)+", "+str(limit)+";"
					self.mysql_con.my_cursor_fallback.execute(sql_out)
					insert_data =  self.mysql_con.my_cursor_fallback.fetchall()
					pg_engine.insert_data(table_name, insert_data , self.my_tables)
				self.print_progress(slice+1,total_slices, table_name)
				csv_file.close()
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
