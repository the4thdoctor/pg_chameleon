import os
import sys
from pg_chameleon import global_config
import pymysql
class mysql_engine:
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


	def disconnect_db(self):
		self.my_connection.close()
		
		
