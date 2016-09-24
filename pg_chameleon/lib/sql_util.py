import re
class sql_utility:
	def __init__(self):
		self.stop_chars=['\n', ' ', '\t']
		self.statements=[]
		self.tokens=[]
		
	def tokenise(self, sql_string):
		token_list=[]
		
		self.statements=sql_string.split(';')
		for statement in self.statements:
			token_list=re.sub("[^\w][(][)]", " ",  statement).split()
			self.tokens.append(token_list)
		print self.tokens
