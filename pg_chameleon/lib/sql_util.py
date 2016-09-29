import re
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DDL

class sql_json:
	"""
	Class sql_utility. Tokenise the sql statements captured by the mysql replication.
	Each statement is converted in dictionary being used by pg_engine.
	"""
	def __init__(self):
		self.statements=[]
		self.token_list=[]
		self.stopwords=['TEMPORARY']
	
	def build_tokens(self, tokens):
		token_dic={}
		for token in tokens:
			if token.is_whitespace():
				pass
			elif token.ttype is DDL:
				token_dic["command"]=token.value.upper()
			else:
				print " Type: "+str(token.ttype)+" Value: "+token.value
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
			stat_cleanup=re.sub(r'[\b,\b]', ' ', stat_cleanup)
			sql_clean=re.sub("[^\w][(][)]", " ",  stat_cleanup)
			parsed = sqlparse.parse(sql_clean)
			if len(parsed)>0:
				self.build_tokens(parsed[0])
