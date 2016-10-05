import re

class sql_token:
	"""
	Class sql_token. Tokenise the sql statements captured by the mysql replication.
	Each statement is converted in dictionary being used by pg_engine.
	"""
	def __init__(self):
		self.statements=[]
		self.query_list=[]
		#re for query elements
		self.m_pkeys=re.compile(r',\s*PRIMARY\s*KEY\s*\((.*?)\)\s*', re.IGNORECASE)
		self.m_ukeys=re.compile(r',\s*UNIQUE\s*KEY\s*\w*\s*\((.*?)\)\s*', re.IGNORECASE)
		self.m_idx=re.compile(r',\s*(KEY|INDEX)\s*\w*\s*\((.*?)\)\s*', re.IGNORECASE)
		self.m_fkeys=re.compile(r',\s*CONSTRAINT\s*\w*\s*FOREIGN\s*KEY.*,', re.IGNORECASE)
		self.m_nulls=re.compile(r'(not)?\s*(null)', re.IGNORECASE)
		self.m_autoinc=re.compile(r'(auto_increment)', re.IGNORECASE)
		
		#re for query type
		self.m_create=re.compile(r'\s*(create\s*(table|index))\s*', re.IGNORECASE)
		
		
	
	def parse_column(self, col_def):
		col_dic={}
		col_list=col_def.split()
		if len(col_list)>1:
			col_dic["name"]=col_list[0]
			col_dic["type"]=col_list[1]
			nullcons=self.m_nulls.search(col_def)
			autoinc=self.m_autoinc.search(col_def)
			if nullcons:
				col_dic["null"]=nullcons.group(0)
			if autoinc:
				col_dic["autoinc"]="true"
		return col_dic
		
	
	def build_column_dic(self, inner_stat):
		cols_parse=[]
		column_list=inner_stat.split(',')
		for col_def in column_list:
			col_def=col_def.strip()
			pkey=self.m_pkeys.match(col_def)
			ukey=self.m_ukeys.match(col_def)
			fkey=self.m_fkeys.match(col_def)
			idx=self.m_idx.match(col_def)
			if pkey or ukey or idx or fkey:
				pass 
			else:
				print "column definition: "+col_def
				col_dic=self.parse_column(col_def)
				if col_dic:
					cols_parse.append(col_dic)
		return cols_parse
	
	def parse_create_table(self, sql_create):
		cnt_open=0
		cnt_close=0
		stat_buffer=[]
		for char in sql_create:
			if char=='(':
				cnt_open+=1
			if char==')':
				cnt_close+=1
			if cnt_open-cnt_close>0:
				stat_buffer.append(char)
		stat_buffer.append(',')
		inner_stat=''.join(stat_buffer[1:]).strip()
		column_list=self.m_pkeys.sub( '', inner_stat)
		column_list=self.m_ukeys.sub( '', column_list)
		column_list=self.m_idx.sub( '', column_list)
		column_list=self.m_fkeys.sub( '', column_list)
		
		print column_list
		return self.build_column_dic(column_list)
			
		
	def parse_sql(self, sql_string):
		"""
			Splits the sql string in statements using the conventional end of statement marker ;
			A regular expression greps the words and parentesis and a split converts them in
			a list. Each list of words is then stored in the list token_list.
			
			:param sql_string: The sql string with the sql statements.
		"""
		self.statements=sql_string.split(';')
		for statement in self.statements:
			token_dic={}
			stat_cleanup=re.sub(r'/\*.*?\*/', '', statement, re.DOTALL)
			stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)
			stat_cleanup=re.sub(r'[\b)\b]', ' ) ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b(\b]', ' ( ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b,\b]', ', ', stat_cleanup)
			stat_cleanup=re.sub(r'\n*', '', stat_cleanup)
			stat_cleanup=re.sub("\([\w*\s*]\)", " ",  stat_cleanup)
			print stat_cleanup
			mcreate=self.m_create.match(stat_cleanup)
			if mcreate:
				command=' '.join(mcreate.group(0).split()).upper().strip()
				token_dic["command"]=command
				print command
				if command=='CREATE TABLE':
					token_dic["columns"]=self.parse_create_table(stat_cleanup)
					print token_dic
