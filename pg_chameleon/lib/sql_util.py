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
		#self.match_keys=re.compile(r'PRIMARY KEY', re.IGNORECASE)
		
	def parse_columns(self, token_dic):
		column_group=token_dic["group"]
		for column in column_group:
			column=re.sub(r'[\n]', '', column)
			column_list=column.split(',')
			for col_def in column_list:
				col_def =re.sub(r'[(]', '', col_def )
				col_def =re.sub(r'[)]', '', col_def )
				col_def=col_def.strip()
				pkey=self.match_pkeys.match(col_def)
				ukey=self.match_ukeys.match(col_def)
				fkey=self.match_fkeys.match(col_def)
				idx=self.match_idx.match(col_def)
				if pkey:
					print "matched primary key "+col_def
				elif ukey:
					print "matched unique key "+col_def
				elif fkey:
					print "matched foreign key "+col_def
				elif idx:
					print "matched index key "+col_def

	def collect_tokens(self, tokens):
		token_dic={}
		group_list=[]
		keyword_list=[]
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
		self.parse_columns(token_dic)
		
	
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
				self.collect_tokens(parsed[0])
