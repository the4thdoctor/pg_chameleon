import re
import json
import sqlparse
from sqlparse.sql import Identifier
from sqlparse.tokens import Keyword, DDL

class sql_utility:
	"""
	Class sql_utility. Tokenise the sql statements captured by the mysql replication.
	Each statement is converted in dictionary being used by pg_engine.
	"""
	def __init__(self):
		self.statements=[]
		self.token_list=[]
		self.match_pkeys=re.compile(r'\w*(primary)\s*key', re.IGNORECASE)
		self.match_ukeys=re.compile(r'\w*(unique)\s*key', re.IGNORECASE)
		self.match_idx=re.compile(r'(key)|(unique)?\s*(index)', re.IGNORECASE)
		self.match_fkeys=re.compile(r'(constraint)?\n*\s*\w*\s*foreign key', re.IGNORECASE)
		self.match_nullcons=re.compile(r'(not)?\s*(null)', re.IGNORECASE|re.MULTILINE)
		self.match_autoinc=re.compile(r'(auto_increment)', re.IGNORECASE|re.MULTILINE)
		self.match_autoinc=re.compile(r'(auto_increment)', re.IGNORECASE|re.MULTILINE)
		self.match_cr_table=re.compile(r'\s*(create)\s*(table)\s*', re.IGNORECASE)
		
		
	
	def parse_column(self, col_def):
		col_dic={}
		col_list=col_def.split()
		if len(col_list)>1:
			col_dic["name"]=col_list[0]
			col_dic["type"]=col_list[1]
			nullcons=self.match_nullcons.search(col_def)
			autoinc=self.match_autoinc.search(col_def)
			if nullcons:
				col_dic["null"]=nullcons.group(0)
			if autoinc:
				col_dic["autoinc"]=nullcons.group(0)
		return col_dic
		
	def parse_group(self, token_dic):
		column_group=token_dic["group"]
		column_parsed=[]
		for column in column_group:
			column=re.sub(r'[\n]', '', column)
			column_list=column.split(',')
			for col_def in column_list:
				col_def=col_def.strip('(').strip()
				pkey=self.match_pkeys.match(col_def)
				ukey=self.match_ukeys.match(col_def)
				fkey=self.match_fkeys.match(col_def)
				idx=self.match_idx.match(col_def)
				if pkey:
					print "matched primary key: "+col_def
				elif ukey:
					print "matched unique key: "+col_def
				elif fkey:
					print "matched foreign key: "+col_def
				elif idx:
					print "matched index key: "+col_def
				else:
					#print "column definition: "+col_def
					col_dic=self.parse_column(col_def)
					if len(col_dic)>0:
						column_parsed.append(col_dic)
		return column_parsed
				

	def collect_tokens(self, tokens):
		token_dic={}
		group_list=[]
		for token in tokens:
			if token.is_whitespace():
				pass
			elif token.ttype is DDL:
				token_dic["command"]=token.value.upper()
			elif token.ttype is Keyword:
				pass
			elif token.ttype==None:
				if isinstance(token, Identifier):
					token_dic["identifier"]=token.value
				elif token.is_group():
					group_list.append(token.value)
		token_dic["group"]=group_list
		token_dic["group"]=self.parse_group(token_dic)
		print token_dic
		
	
	def parse_sql(self, sql_string):
		"""
			Splits the sql string in statements using the conventional end of statement marker ;
			A regular expression greps the words and parentesis and a split converts them in
			a list. Each list of words is then stored in the list token_list.
			
			:param sql_string: The sql string with the sql statements.
		"""
		token_list=[]
		self.statements=sqlparse.split(sql_string)
		for statement in self.statements:
			stat_cleanup=re.sub(r'/\*.*?\*/', '', statement, re.DOTALL)
			stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)
			stat_cleanup=re.sub(r'[\b)\b]', ' ) ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b(\b]', ' ( ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b,\b]', ', ', stat_cleanup)
			sql_clean=re.sub("[^\w][(][)]", " ",  stat_cleanup)
			parsed = sqlparse.parse(sql_clean)
			if len(parsed)>0:
				createtab=self.match_cr_table.match(sql_clean)
				print createtab
				self.collect_tokens(parsed[0])
