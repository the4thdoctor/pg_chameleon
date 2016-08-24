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
		self.tables=[]
	
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
		
		
class mysql_engine():
	def __init__(self):
		self.mysql_con=mysql_connection()
		self.mysql_con.connect_db()
		self.get_table_metadata()
		self.my_tables={}
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

	def get_column_metadata(self, table):
		sql_columns="""SELECT 
											table_schema,
											table_name,
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
											AND 	table_name=%s;
								;
							"""
		self.mysql_con.my_cursor.execute(sql_columns, (self.mysql_con.my_database, table))
		column_list=self.mysql_con.my_cursor.fetchall()
		return column_list
		
	def get_table_metadata(self):
		print "getting tables"
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
			dic_table={table["table_name"]:{'columns':column_data}}
			print dic_table
	
	def __del__(self):
		self.mysql_con.disconnect_db()
