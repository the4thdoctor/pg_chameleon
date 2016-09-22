class sql_utility:
	def __init__(self):
		self.stop_chars=['\n', ' ', '\t']
		self.statements=[]
		self.normalised_words=[]
		
	def serialise_sql(self, sql_string):
		stat_elements=[]
		stat_word=[]

		self.statements=sql_string.split(';')
		for statement in self.statements:
			statement=statement.strip()
			for chr in statement:
				if chr in self.stop_chars and len(stat_word)>0:
					stat_elements.append(''.join(stat_word))
					stat_word=[]
				else:
					stat_word.append(chr)
				
			if len(stat_word)>0:
					stat_elements.append(''.join(stat_word))
					stat_word=[]
			self.normalised_words.append(stat_elements)
			stat_elements=[]
		print self.normalised_words
