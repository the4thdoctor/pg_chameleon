import io
import pymysql
import codecs
import binascii
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent
from pymysqlreplication.event import RotateEvent
from pg_chameleon import sql_token
from os import remove

class mysql_connection(object):
	def __init__(self, global_config):
		self.global_conf = global_config
		self.my_server_id = self.global_conf.my_server_id
		self.mysql_conn = self.global_conf.mysql_conn
		self.my_database = self.global_conf.my_database
		self.my_charset = self.global_conf.my_charset
		self.tables_limit = self.global_conf.tables_limit
		self.replica_batch_size = self.global_conf.replica_batch_size
		self.copy_mode = self.global_conf.copy_mode
		self.my_connection = None
		self.my_cursor = None
		self.my_cursor_fallback = None
		
	def connect_db_ubf(self):
		""" Connects to the database and creates an unbuffered cursor """
		self.my_connection_ubf=pymysql.connect(
			host=self.mysql_conn["host"],
			user=self.mysql_conn["user"],
			password=self.mysql_conn["passwd"],
			db=self.my_database,
			charset=self.my_charset,
			cursorclass=pymysql.cursors.SSCursor
		)
		self.my_cursor_ubf=self.my_connection_ubf.cursor()

		
	
	def connect_db(self):
		""" Connects to the database and creates a dictionary cursor """
		self.my_connection=pymysql.connect(
			host=self.mysql_conn["host"],
			user=self.mysql_conn["user"],
			password=self.mysql_conn["passwd"],
			db=self.my_database,
			charset=self.my_charset,
			cursorclass=pymysql.cursors.DictCursor
		)
		self.my_cursor=self.my_connection.cursor()
		self.my_cursor_fallback=self.my_connection.cursor()
	
	def disconnect_db(self):
		"""Disconnects the dictionary connection"""
		try:
			self.my_connection.close()
		except:
			pass
	def disconnect_db_ubf(self):
		"""Disconnects the unbuffered connection"""
		try:
			self.my_connection_ubf.close()
		except:
			pass
		
class mysql_engine(object):
	def __init__(self, global_config, logger):
		"""
			Class constructor
		"""
		self.hexify = global_config.hexify
		self.logger = logger
		self.out_dir = global_config.out_dir
		self.my_tables = {}
		self.table_file = {}
		self.mysql_con = mysql_connection(global_config)
		self.mysql_con.connect_db()
		self.get_table_metadata()
		self.my_streamer = None
		self.replica_batch_size = self.mysql_con.replica_batch_size
		self.master_status = []
		self.id_batch = None
		self.sql_token = sql_token()
		self.pause_on_reindex = global_config.pause_on_reindex
		self.stat_skip = ['BEGIN', 'COMMIT']
		self.tables_limit = global_config.tables_limit
		self.my_schema = global_config.my_database
	
	def read_replica(self, batch_data, pg_engine):
		"""
		Stream the replica using the batch data. This method evaluates the different events streamed from MySQL 
		and manages them accordingly. The BinLogStreamReader function is called with the only_event parameter which
		restricts the event type received by the streamer.
		The events managed are the following.
		RotateEvent which happens whether mysql restarts or the binary log file changes.
		QueryEvent which happens when a new row image comes in (BEGIN statement) or a DDL is executed.
		The BEGIN is always skipped. The DDL is parsed using the sql_token class. 
		[Write,Update,Delete]RowEvents are the row images pulled from the mysql replica.
		
		The RotateEvent and the QueryEvent cause the batch to be closed.
		
		The for loop reads the row events, builds the dictionary carrying informations like the destination schema,
		the 	binlog coordinates and store them into the group_insert list.
		When the number of events exceeds the replica_batch_size the group_insert is written into PostgreSQL.
		The batch is not closed in that case and the method exits only if there are no more rows available in the stream.
		Therefore the replica_batch_size is just the maximum size of the single insert and the size of replayed batch on PostgreSQL.
		The binlog switch or a captured DDL determines whether a batch is closed and processed.
		
		The update row event stores in a separate key event_update the row image before the update. This is required
		to allow updates where the primary key is updated as well.
		
		Each row event is scanned for data types requiring conversion to hex string.
		
		:param batch_data: The list with the master's batch data.
		:param pg_engine: The postgresql engine object required for writing the rows in the log tables
		"""
		table_type_map = self.get_table_type_map()	
		schema_name = pg_engine.dest_schema
		close_batch = False
		total_events = 0
		master_data = {}
		group_insert = []
		id_batch = batch_data[0][0]
		log_file = batch_data[0][1]
		log_position = batch_data[0][2]
		log_table = batch_data[0][3]
		my_stream = BinLogStreamReader(
			connection_settings = self.mysql_con.mysql_conn, 
			server_id = self.mysql_con.my_server_id, 
			only_events = [RotateEvent, DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent], 
			log_file = log_file, 
			log_pos = log_position, 
			resume_stream = True, 
			only_schemas = [self.mysql_con.my_database], 
			only_tables = self.tables_limit
		)
		self.logger.debug("log_file %s, log_position %s. id_batch: %s " % (log_file, log_position, id_batch))
		for binlogevent in my_stream:
			total_events+=1
			if isinstance(binlogevent, RotateEvent):
				
				event_time = binlogevent.timestamp
				binlogfile = binlogevent.next_binlog
				position = binlogevent.position
				self.logger.debug("rotate event. binlogfile %s, position %s. " % (binlogfile, position))
				if log_file != binlogfile:
					close_batch = True
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
				event_time = binlogevent.timestamp
				try:
					query_schema = binlogevent.schema.decode()
				except:
					query_schema = binlogevent.schema
				if binlogevent.query.strip().upper() not in self.stat_skip and query_schema == self.my_schema: 
					log_position = binlogevent.packet.log_pos
					master_data["File"] = binlogfile
					master_data["Position"] = log_position
					master_data["Time"] = event_time
					close_batch=True
					if len(group_insert)>0:
						pg_engine.write_batch(group_insert)
						group_insert=[]
					self.logger.debug("query event. binlogfile %s, position %s.\n--------\n%s\n-------- " % (binlogfile, log_position, binlogevent.query))
					self.sql_token.parse_sql(binlogevent.query)
					for token in self.sql_token.tokenised:
						event_time = binlogevent.timestamp
						if len(token)>0:
							query_data={
								"binlog":log_file, 
								"logpos":log_position, 
								"schema": schema_name, 
								"batch_id":id_batch, 
								"log_table":log_table
							}
							pg_engine.write_ddl(token, query_data)
							
						
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
							event_data[column_name]=binascii.hexlify(event_data[column_name]).decode()
						elif column_type in self.hexify and isinstance(event_data[column_name], bytes):
							event_data[column_name] = ''
					for column_name in event_update:
						column_type=column_map[column_name]
						if column_type in self.hexify and event_update[column_name]:
							event_update[column_name]=binascii.hexlify(event_update[column_name]).decode()
						elif column_type in self.hexify and isinstance(event_update[column_name], bytes):
							event_update[column_name] = ''
					event_insert={"global_data":global_data,"event_data":event_data,  "event_update":event_update}
					group_insert.append(event_insert)
					master_data["File"]=log_file
					master_data["Position"]=log_position
					master_data["Time"]=event_time
					if total_events>=self.replica_batch_size:
						self.logger.debug("total events exceeded %s. Writing batch.: %s  " % (total_events, master_data,  ))
						total_events=0
						pg_engine.write_batch(group_insert)
						group_insert=[]
						close_batch=True
						
						
		my_stream.close()
		if len(group_insert)>0:
			self.logger.debug("writing the last %s events" % (len(group_insert), ))
			pg_engine.write_batch(group_insert)
			close_batch=True
		
		return [master_data, close_batch]

	def run_replica(self, pg_engine):
		"""
		Run a MySQL replica read attempt and stores the data in postgres if found. 
		The method first checks if there is a reindex in progress using pg_engine.check_reindex.
		The gets the batch data from PostgreSQL. If the batch data is not empty the method read_replica is executed.
		When the method exits the replica_data list is decomposed in the master_data (log name, position and last event's timestamp).
		If the flag close_batch is set the master status is saved in PostgreSQL, and the new batch id is saved in the mysql_engine class variable id_batch.
		This variable is used to determine whether the old batch should be closed or not. If the variable is not empty then the previous batch
		is closed updating the processed flag on the replica catalogue.
		Before the exit the method calls the pg_engine.process_batch to replay the changes in PostgreSQL
		
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
		"""
			The method builds a dictionary composed by the table name and the column/type mappings.
			The dictionary is used in read_replica to determine whether a field requires hex encoding.
		"""
		table_type_map={}
		self.logger.debug("collecting table type map")
		sql_tables="""	
			SELECT 
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
			sql_columns="""
				SELECT 
					column_name,
					data_type
				FROM 
					information_schema.COLUMNS 
				WHERE 
						table_schema=%s
					AND table_name=%s
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
		"""
			The method extracts the columns metadata for a specific table.
			The select builds also the field list formatted for the CSV copy or a single insert copy.
			The data types included in hexlify are hex encoded on the fly.
			
			:param table: The table name.
		"""
		sql_columns="""
			SELECT 
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
		"""
			The method extracts the index metadata for a specific table.
			The select searches only for the BTREE indices using the information_schema.statistics table.
	
			:param table: The table name.
		"""
		sql_index="""
			SELECT 
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
	
	def get_fk_metadata(self):
		"""
			The method collects the foreign key metadata for the detach replica process.
			Currently doesn't get the ON UPDATE/ON DELETE triggers
		"""
		self.logger.debug("getting foreign keys metadata")
		sql_fkeys = """ 
			SELECT 
				table_name,
				constraint_name,
				referenced_table_name,
				GROUP_CONCAT(concat('"',column_name,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as fk_cols,
				GROUP_CONCAT(concat('"',REFERENCED_COLUMN_NAME,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as ref_columns
			FROM 
				information_schema.key_column_usage 
			WHERE 
					table_schema=%s 
				AND referenced_table_name IS NOT NULL
				AND referenced_table_schema=%s
				
			GROUP BY 
				table_name,
				constraint_name,
				referenced_table_name
				
			ORDER BY 
				table_name
			;
		"""
		self.mysql_con.my_cursor.execute(sql_fkeys, (self.mysql_con.my_database, self.mysql_con.my_database))
		fkey_list=self.mysql_con.my_cursor.fetchall()
		return fkey_list
		
	def get_table_metadata(self):
		"""
			the metod collects the metadata for all the tables in the mysql schema specified
			in self.my_database using get_column_metadata and get_index_metadata.
			If there are tables in tables_limit the variable table_include is set in order to limit the results to the tables_limit only.
			The informations are stored in a dictionary.
			The key column stores the column metadata and indices stores the index metadata.
			The key name stores the table name.
			The dictionary is then saved in the class dictionary self.my_tables with the key set to table name.
			
		"""
		self.logger.debug("getting table metadata")
		table_include=""
		if self.mysql_con.tables_limit:
			self.logger.info("table copy limited to tables: %s" % ','.join(self.mysql_con.tables_limit))
			table_include="AND table_name IN ('"+"','".join(self.mysql_con.tables_limit)+"')"
		sql_tables="""
			SELECT 
				table_schema,
				table_name
			FROM 
				information_schema.TABLES 
			WHERE 
					table_type='BASE TABLE' 
				AND table_schema=%s
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
		"""
			Print the copy progress. 
			As the variable total on innodb is estimated the percentage progress exceed the 100%.
			In order to reduce noise when the log level is info only the tables copied in multiple slices
			get the print progress.
			
			:param iteration: The slice number currently processed
			:param total: The estimated total slices
			:param table_name: The table name
		"""
		if total>1:
			self.logger.info("Table %s copied %d %%" % (table_name, 100 * float(iteration)/float(total)))
		else:
			self.logger.debug("Table %s copied %d %%" % (table_name, 100 * float(iteration)/float(total)))
		
	def generate_select(self, table_columns, mode="csv"):
		"""
			The method builds the select list using the dictionary table_columns which is the columns key in the table's metadata.
			The method can build a select list for a CSV output or an INSERT output. The default mode is csv.
			
			:param table_columns: The table's column dictionary with the column metadata
			:param mode: the select mode, csv or insert
		"""
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
		"""
			This method is a fallback procedure whether copy_table_data fails.
			The ins_args is a list with the informations required to run the select for building the insert
			statements and the slices's start and stop.
			The process is performed in memory and can take a very long time to complete.
			
			:param pg_engine: the postgresql engine
			:param ins_arg: the list with the insert arguments (slice_insert, table_name, columns_insert, slice_size)
		"""
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
		"""
			copy the table data from mysql to postgres
			param pg_engine: The postgresql engine required to write into the postgres database.
			The process determines the estimated optimal slice size using copy_max_memory and avg_row_length 
			from MySQL's information_schema.TABLES. If the table contains no rows then the slice size is set to a
			reasonable high value (100,000) in order to get the table copied in one slice. The estimated numer of slices is determined using the 
			slice size.
			Then generate_select is used to build the csv and insert columns for the table.
			An unbuffered cursor is used to pull the data from MySQL using the CSV format. The fetchmany with copy_limit (slice size) is called
			to pull out the rows into a file object. 
			The copy_mode determines wheter to use a file (out_file) or an in memory file object (io.StringIO()).
			If there no more rows the loop exits, otherwise continue to the next slice. When the slice is saved the method pg_engine.copy_data is
			executed to load the data into the PostgreSQL table.
			If some error occurs the slice number is saved into the list slice_insert and after all the slices are copied the fallback procedure insert_table_data
			process the remaining slices using the inserts.
			
			:param pg_engine: the postgresql engine
			:param copy_max_memory: The estimated maximum amount of memory to use in a single slice copy
			
		"""
		out_file='%s/output_copy.csv' % self.out_dir
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
				self.logger.error("error when pulling data from %s. sql executed: %s" % (table_name, sql_out))
			
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
		try:
			remove(out_file)
		except:
			pass
			
	def get_master_status(self):
		"""
			The method gets the master's coordinates using the command SHOW MASTER STATUS.
			The dictionary cursor is stored in the class variable self.master_status
		"""
		t_sql_master="SHOW MASTER STATUS;"
		self.mysql_con.my_cursor.execute(t_sql_master)
		self.master_status=self.mysql_con.my_cursor.fetchall()		
		
		
	def lock_tables(self):
		""" 
			The method locks the tables using FLUSH TABLES WITH READ LOCK. The 
			tables locked are limited to the tables found by get_table_metadata.
			After locking the tables the metod gets the master's coordinates with get_master_status.
		"""
		self.locked_tables=[]
		for table_name in self.my_tables:
			table=self.my_tables[table_name]
			self.locked_tables.append(table["name"])
		t_sql_lock="FLUSH TABLES "+", ".join(self.locked_tables)+" WITH READ LOCK;"
		self.mysql_con.my_cursor.execute(t_sql_lock)
		self.get_master_status()
	
	def unlock_tables(self):
		"""The method unlocks the tables previously locked by lock_tables"""
		t_sql_unlock="UNLOCK TABLES;"
		self.mysql_con.my_cursor.execute(t_sql_unlock)
			
			
	def __del__(self):
		"""
			Class destructor, disconnects the mysql connections.
		"""
		self.mysql_con.disconnect_db()
