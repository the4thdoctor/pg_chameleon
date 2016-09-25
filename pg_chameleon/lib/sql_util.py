import re
class sql_utility:
	"""
	Class sql_utility. Tokenise the sql statements captured by the mysql replication.
	Each statement is converted in dictionary being used by pg_engine.
	"""
	def __init__(self):
		self.statements=[]
		self.tokens=[]
		
	def tokenise(self, sql_string):
		"""
			Splits the sql string in statements using the conventional end of statement marker ;
			A regular expression greps the words and parentesis and a split converts them in
			a list. Each list of words is then stored in the list token_list.
			
			:param sql_string: The sql string with the sql statements.
		"""
		token_list=[]
		
		self.statements=sql_string.split(';')
		for statement in self.statements:
			token_list=re.sub("[^\w][(][)]", " ",  statement).split()
			self.tokens.append(token_list)
		print self.tokens
	
