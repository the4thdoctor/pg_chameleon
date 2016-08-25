import os
import sys
from pg_chameleon import global_config
import pymysql
class mysql_connection:
	def __init__(self):
		self.global_conf=global_config()
		self.mysql_conn=self.global_conf.mysql_conn
		self.my_database=self.global_conf.my_database
		self.my_connection=None
		self.my_cursor=None
		
	
	def connect_db(self):
		"""  Establish connection with the database """
		self.my_connection=pymysql.connect(host=self.mysql_conn["host"],
									user=self.mysql_conn["user"],
									password=self.mysql_conn["passwd"],
									db=self.my_database,
									charset='utf8mb4',
									cursorclass=pymysql.cursors.DictCursor)
		self.my_cursor=self.my_connection.cursor()

	def disconnect_db(self):
		self.my_connection.close()
		
		
class mysql_engine:
	def __init__(self, out_dir="/tmp/"):
		self.out_dir=out_dir
		self.my_tables=[]
		self.type_dictionary={
												'integer':'integer',
												'mediumint':'bigint',
												'tinyint':'integer',
												'smallint':'integer',
												'int':'bigint',
												'varchar':'varchar',
												'bigint':'bigint',
												'text':'text',
												'char':'char',
												'datetime':'date',
												'longtext':'text',
												'tinytext':'text',
												'tinyblob':'bytea',
												'mediumblob':'bytea',
												'longblob':'bytea',
												'blob':'bytea'
										}
		self.mysql_con=mysql_connection()
		self.mysql_con.connect_db()
		self.get_table_metadata()

	def get_column_metadata(self, table):
		sql_columns="""SELECT 
											column_name,
											column_default,
											ordinal_position,
											data_type,
											character_maximum_length,
											extra,
											column_key
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
										GROUP_CONCAT(column_name ORDER BY seq_in_index) as index_columns
									FROM
										information_schema.statistics
									WHERE
														table_schema=%s
											AND 	table_name=%s
											AND	index_type = 'BTREE'
									GROUP BY 
										table_name,
										index_name
									;
							"""
		self.mysql_con.my_cursor.execute(sql_index, (self.mysql_con.my_database, table))
		index_data=self.mysql_con.my_cursor.fetchall()
		return index_data
	
	def get_table_metadata(self):
		print "getting table metadata"
		sql_tables="""SELECT 
											table_schema,
											table_name
								FROM 
											information_schema.TABLES 
								WHERE 
														table_type='BASE TABLE' 
											AND 	table_schema=%s;
							"""
		self.mysql_con.my_cursor.execute(sql_tables, (self.mysql_con.my_database))
		table_list=self.mysql_con.my_cursor.fetchall()
		for table in table_list:
			column_data=self.get_column_metadata(table["table_name"])
			index_data=self.get_index_metadata(table["table_name"])
			dic_table={'name':table["table_name"], 'columns':column_data,  'indices': index_data}
			self.my_tables.append(dic_table)
	
	def pull_table_data(self, table=None, limit=10000):
		
		for table in self.my_tables:
			column_list=[]
			table_name=table["name"]
			table_columns=table["columns"]
			sql_count="SELECT count(*) as i_cnt FROM `"+table_name+"` ;"
			self.mysql_con.my_cursor.execute(sql_count)
			count_rows=self.mysql_con.my_cursor.fetchone()
			num_slices=count_rows["i_cnt"]/limit
			range_slices=range(num_slices+1)
			for column in table_columns:
				column_list.append("COALESCE(REPLACE("+column["column_name"]+", '\"', '\"\"'),'NULL') ")
			columns="REPLACE(CONCAT('\"',CONCAT_WS('\",\"',"+','.join(column_list)+"),'\"'),'\"NULL\"','NULL')"
			out_file=self.out_dir+'/out_data'+table_name+'.csv'
			csv_file=open(out_file, 'wb')
			for slice in range_slices:
				sql_out="SELECT "+columns+" as data FROM "+table_name+" LIMIT "+str(slice*limit)+", "+str(limit)+";"
				print "pulling out "+str(slice) + " of "+str(num_slices)
				self.mysql_con.my_cursor.execute(sql_out)
				csv_results = self.mysql_con.my_cursor.fetchall()
				for csv_row in csv_results:
					csv_file.write(csv_row["data"]+"\n")
				
			csv_file.close()
			
			
	def __del__(self):
		self.mysql_con.disconnect_db()
