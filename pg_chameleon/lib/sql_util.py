import re

class sql_token(object):
	"""
	The class tokenises the sql statements captured by mysql_engine.
	Several regular expressions analyse and build the elements of the token.
	The DDL support is purposely limited to the following.
	
	DROP PRIMARY KEY
	CREATE (UNIQUE) INDEX/KEY
	CREATE TABLE
	ALTER TABLE
	
	The regular expression m_fkeys is used to remove any foreign key definition from the sql statement
	as we don't enforce any foreign key on the PostgreSQL replication.
	"""
	def __init__(self):
		"""
			Class constructor the regular expressions are compiled and the token lists are initialised.
		"""
		self.tokenised=[]
		self.query_list=[]
		self.pkey_cols=""
		
		#re for column definitions
		self.m_columns=re.compile(r'\((.*)\)', re.IGNORECASE)
		self.m_inner=re.compile(r'\((.*)\)', re.IGNORECASE)
		
		#re for keys and indices
		self.m_pkeys=re.compile(r',\s*(?:CONSTRAINT)?\s*`?\w*`?\s*PRIMARY\s*KEY\s*\((.*?)\)\s?', re.IGNORECASE)
		self.m_ukeys=re.compile(r',\s*UNIQUE\s*KEY\s*`?\w*`?\s*\((.*?)\)\s*', re.IGNORECASE)
		self.m_keys=re.compile(r',\s*(?:UNIQUE)?\s*(?:KEY|INDEX)\s*`?\w*`?\s*\((?:.*?)\)\s*', re.IGNORECASE)
		self.m_idx=re.compile(r',\s*(?:KEY|INDEX)\s*`?\w*`?\s*\((.*?)\)\s*', re.IGNORECASE)
		self.m_fkeys=re.compile(r',\s*(?:CONSTRAINT)?\s*`?\w*`?\s*FOREIGN\s*KEY(?:\(?.*\(??)(?:\s*REFERENCES\s*`?\w*`)?(?:ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT|CASCADE)\s*)?', re.IGNORECASE)
		self.m_inline_pkeys=re.compile(r'(.*?)\bPRIMARY\b\s*\bKEY\b', re.IGNORECASE)
		
		#re for fields
		self.m_field=re.compile(r'(?:`)?(\w*)(?:`)?\s*(?:`)?(\w*\s*(?:precision|varying)?)(?:`)?\s*((\(\s*\d*\s*\)|\(\s*\d*\s*,\s*\d*\s*\))?)', re.IGNORECASE)
		self.m_dbl_dgt=re.compile(r'((\(\s?\d+\s?),(\s?\d+\s?\)))',re.IGNORECASE)
		self.m_pars=re.compile(r'(\((:?.*?)\))', re.IGNORECASE)
		self.m_dimension=re.compile(r'(\(.*?\))', re.IGNORECASE)
		self.m_fields=re.compile(r'(.*?),', re.IGNORECASE)
		
		#re for column constraint and auto incremental
		self.m_nulls=re.compile(r'(NOT)?\s*(NULL)', re.IGNORECASE)
		self.m_autoinc=re.compile(r'(AUTO_INCREMENT)', re.IGNORECASE)
		
		#re for query type
		self.m_create_table = re.compile(r'(CREATE\s*TABLE)\s*(?:IF\s*NOT\s*EXISTS)?\s*(?:(?:`)?(?:\w*)(?:`)?\.)?(?:`)?(\w*)(?:`)?', re.IGNORECASE)
		self.m_drop_table = re.compile(r'(DROP\s*TABLE)\s*(?:IF\s*EXISTS)?\s*(?:`)?(\w*)(?:`)?', re.IGNORECASE)
		self.m_truncate_table = re.compile(r'(TRUNCATE)\s*(?:TABLE)?\s*(?:`)?(\w*)(?:`)?', re.IGNORECASE)
		self.m_alter_index = re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+((?:ADD|DROP)\s+(?:UNIQUE)?\s*?(?:INDEX).*,?)', re.IGNORECASE)
		self.m_alter_table = re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+((?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?.*,?)', re.IGNORECASE)
		self.m_alter_list = re.compile(r'((?:(?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?))(.*?,)', re.IGNORECASE)
		self.m_alter_column = re.compile(r'\s*`?(\w*)`?\s*(\w*)\s*(?:\((.*?)\))?', re.IGNORECASE)
		self.m_default_value = re.compile(r"(\bDEFAULT\b)\s*('?\w*'?)\s*", re.IGNORECASE)
		self.m_alter_change = re.compile(r'\s*`?(\w*)`?\s*`?(\w*)`?\s*(\w*)\s*(?:\((.*?)\))?', re.IGNORECASE)
		self.m_drop_primary = re.compile(r'(?:(?:ALTER\s+?TABLE)\s+(`?\b.*?\b`?)\s+(DROP\s+PRIMARY\s+KEY))', re.IGNORECASE)
		self.m_modify = re.compile(r'((?:(?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?))(.*?,)', re.IGNORECASE)
		self.m_ignore_keywords = re.compile(r'(CONSTRAINT)|(PRIMARY)|(INDEX)|(UNIQUE)|(FOREIGN\s*KEY)', re.IGNORECASE)
		#'CONSTRAINT', 'PRIMARY', 'INDEX', 'UNIQUE', 'FOREIGN KEY' 
	def reset_lists(self):
		"""
			The method resets the lists to empty lists after a successful tokenisation.
		"""
		self.tokenised=[]
		self.query_list=[]
		
	def parse_column(self, col_def):
		"""
			This method parses the column definition searching for the name, the data type and the
			dimensions.
			If there's a match the dictionary is built with the keys
			column_name, the column name
			data_type, the column's data type
			is nullable, the value is set always to yes except if the column is primary key ( column name present in key_cols)
			enum_list,character_maximum_length,numeric_precision are the dimensions associated with the data type.
			The auto increment is set if there's a match for the auto increment specification.s
			
			:param col_def: The column definition
			:return: col_dic the column dictionary 
			:rtype: dictionary
		"""
		colmatch = self.m_field.search(col_def)
		dimmatch = self.m_dimension.search(col_def)
		col_dic={}
		if colmatch:
			col_dic["column_name"]=colmatch.group(1).strip("`").strip()
			col_dic["data_type"]=colmatch.group(2).lower().strip()
			col_dic["is_nullable"]="YES"
			if dimmatch:
				dimensions = dimmatch.group(1).replace('|', ',').replace('(', '').replace(')', '').strip()
				enum_list = dimmatch.group(1).replace('|', ',').strip()
				numeric_dims = dimensions.split(',')
				numeric_precision = numeric_dims[0].strip()
				try:
					numeric_scale = numeric_dims[1].strip()
				except:
					numeric_scale = 0
				
				col_dic["enum_list"] = enum_list
				col_dic["character_maximum_length"] = dimensions
				col_dic["numeric_precision"]=numeric_precision
				col_dic["numeric_scale"]=numeric_scale
			nullcons=self.m_nulls.search(col_def)
			autoinc=self.m_autoinc.search(col_def)
			pkey_list=self.pkey_cols.split(',')
			col_dic["is_nullable"]="YES"
			if col_dic["column_name"] in pkey_list:
				col_dic["is_nullable"]="NO"
			elif nullcons:
				pkey_list=[cln.strip() for cln in pkey_list]
				if nullcons.group(0)=="NOT NULL":
					col_dic["is_nullable"]="NO"
				
			if autoinc:
				col_dic["extra"]="auto_increment"
			else :
				col_dic["extra"]=""
		return col_dic
		
	def quote_cols(self, cols):
		"""
			The method adds the " quotes to the column names.
			The string is converted to a list using the split method with the comma separator.
			The columns are then stripped and quoted with the "".
			Finally the list elements are rejoined in a string which is returned.
			The method is used in build_key_dic to sanitise the column names.
			
			:param cols: The columns string
			:return: The columns quoted between ".
			:rtype: text
		"""
		idx_cols = cols.split(',')
		idx_cols = ['"%s"' % col.strip() for col in idx_cols]
		quoted_cols = ",".join(idx_cols)
		return quoted_cols
	
	
	def build_key_dic(self, inner_stat, table_name):
		"""
			The method matches and tokenise the primary key and index/key definitions in the create table's inner statement.
			
			As the primary key can be defined as column or table constraint there is an initial match attempt with the regexp m_inline_pkeys.
			If the match is successful then the primary key dictionary is built from the match data.
			Otherwise the primary key dictionary is built using the eventual table key definition.
			
			The method search for primary keys keys and indices defined in the inner_stat.
			The index name PRIMARY is used to tell pg_engine we are building a primary key.
			Otherwise the index name is built using the format (uk)idx_tablename[0:20] + counter.
			If there's a match for a primary key the composing columns are save into pkey_cols.
			
			The tablename limitation is required as PostgreSQL enforces a strict limit for the identifier name's lenght.
			
			Each key dictionary have three keys. 
			index_name, the index name or PRIMARY 
			index_columns, a list with the column names
			non_unique, follows the MySQL's information schema convention and marks an index if is unique or not.
			
			When the dictionary is built is appended to idx_list and finally returned to the calling method parse_create_table.s
			

			:param inner_stat: The statement within the round brackets in CREATE TABLE
			:param table_name: The table name
			:return: idx_list the list of dictionary with the index definitions
			:rtype: list
		"""
		key_dic={}
		idx_list=[]
		idx_counter=0
		inner_stat=inner_stat.strip()

		pk_match =  self.m_inline_pkeys.match(inner_stat)
		pkey=self.m_pkeys.findall(inner_stat)

		ukey=self.m_ukeys.findall(inner_stat)
		idx=self.m_idx.findall(inner_stat)

		if pk_match:
			key_dic["index_name"]='PRIMARY'
			idx_cols = (pk_match.group(1).strip().split()[0]).replace('`', '')
			key_dic["index_columns"] = self.quote_cols(idx_cols)
			key_dic["non_unique"]=0
			self.pkey_cols = idx_cols
			idx_list.append(dict(list(key_dic.items())))
			key_dic={}
		elif pkey:
			key_dic["index_name"]='PRIMARY'
			idx_cols = pkey[0].replace('`', '')
			key_dic["index_columns"] = self.quote_cols(idx_cols)
			key_dic["non_unique"]=0
			self.pkey_cols = idx_cols
			idx_list.append(dict(list(key_dic.items())))
			key_dic = {}
		if ukey:
			for cols in ukey:
				key_dic["index_name"] = 'ukidx_'+table_name[0:20]+'_'+str(idx_counter)
				idx_cols = cols.replace('`', '')
				key_dic["index_columns"] = self.quote_cols(idx_cols)
				key_dic["non_unique"]=0
				idx_list.append(dict(list(key_dic.items())))
				key_dic={}
				idx_counter+=1
		if idx:
			for cols in idx:
				key_dic["index_name"]='idx_'+table_name[0:20]+'_'+str(idx_counter)
				idx_cols = cols.replace('`', '')
				key_dic["index_columns"] = self.quote_cols(idx_cols)
				key_dic["non_unique"]=1
				idx_list.append(dict(list(key_dic.items())))
				key_dic={}
				idx_counter+=1
		return idx_list
		
	def build_column_dic(self, inner_stat):
		"""
			The method builds a list of dictionaries with the column definitions.
			
			The regular expression m_fields is used to find all the column occurrences and, for each occurrence,
			the method parse_column is called.
			If parse_column returns a dictionary, this is appended to the list col_parse.
			
			:param inner_stat: The statement within the round brackets in CREATE TABLE
			:return: cols_parse the list of dictionary with the column definitions
			:rtype: list
		"""
		column_list=self.m_fields.findall(inner_stat)
		cols_parse=[]
		for col_def in column_list:
			col_def=col_def.strip()
			col_dic=self.parse_column(col_def)
			if col_dic:
				cols_parse.append(col_dic)
		return cols_parse
		
	
	def parse_create_table(self, sql_create, table_name):
		"""
			The method parse and generates a dictionary from the CREATE TABLE statement.
			The regular expression m_inner is used to match the statement within the round brackets.
			
			This inner_stat is then cleaned from the primary keys, keys indices and foreign keys in order to get
			the column list.
			The indices are stored in the dictionary key "indices" using the method build_key_dic.
			The regular expression m_pars is used for finding and replacing all the commas with the | symbol within the round brackets
			present in the columns list.
			At the column list is also appended a comma as required by the regepx used in build_column_dic.
			The build_column_dic method is then executed and the return value is stored in the dictionary key "columns"
			
			:param sql_create: The sql string with the CREATE TABLE statement
			:param table_name: The table name
			:return: table_dic the table dictionary tokenised from the CREATE TABLE 
			:rtype: dictionary
		"""
		
		m_inner=self.m_inner.search(sql_create)
		inner_stat=m_inner.group(1).strip()
		table_dic={}
		
		column_list=self.m_pkeys.sub( '', inner_stat)
		column_list=self.m_keys.sub( '', column_list)
		column_list=self.m_idx.sub( '', column_list)
		column_list=self.m_fkeys.sub( '', column_list)
		table_dic["indices"]=self.build_key_dic(inner_stat, table_name)
		mpars=self.m_pars.findall(column_list)
		for match in mpars:
			new_group=str(match[0]).replace(',', '|')
			column_list=column_list.replace(match[0], new_group)
		column_list=column_list+","
		table_dic["columns"]=self.build_column_dic(column_list)
		return table_dic	
	
	def parse_alter_table(self, malter_table):
		"""
			The method parses the alter table match.
			As alter table can be composed of multiple commands the original statement (group 0 of the match object)
			is searched with the regexp m_alter_list.
			For each element in returned by findall the first word is evaluated as command. The parse alter table 
			manages the following commands.
			DROP,ADD,CHANGE,MODIFY.
			
			Each command build a dictionary alter_dic with at leaset the keys command and name defined.
			Those keys are respectively the commant itself and the attribute name affected by the command.
			
			ADD defines the keys type and dimension. If type is enum then the dimension key stores the enumeration list.
			
			CHANGE defines the key command and then runs a match with m_alter_change. If the match is successful 
			the following keys are defined.
			
			old is the old previous field name
			new is the new field name
			type is the new data type
			dimension the field's dimensions or the enum list if type is enum
			
			MODIFY works similarly to CHANGE except that the field is not renamed.
			In that case we have only the keys type and dimension defined along with name and command.s
			
			The class's regular expression self.m_ignore_keywords is used to skip the CONSTRAINT,INDEX and PRIMARY and FOREIGN KEY KEYWORDS in the
			alter command.
			
			:param malter_table: The match object returned by the match method against tha alter table statement.
			:return: stat_dic the alter table dictionary tokenised from the match object.
			:rtype: dictionary
		"""
		stat_dic={}
		alter_cmd=[]
		alter_stat=malter_table.group(0) + ','
		stat_dic["command"]=malter_table.group(1).upper().strip()
		stat_dic["name"]=malter_table.group(2).strip().strip('`')
		dim_groups=self.m_dimension.findall(alter_stat)
		
		for dim_group in dim_groups:
			alter_stat=alter_stat.replace(dim_group, dim_group.replace(',','|'))
		
		alter_list=self.m_alter_list.findall(alter_stat)
		for alter_item in alter_list:
			alter_dic={}
			m_ignore_item = self.m_ignore_keywords.search(alter_item[1])
			
			if not m_ignore_item:
				command = (alter_item[0].split())[0].upper().strip()
				if command == 'DROP':
					alter_dic["command"] = command
					alter_dic["name"] = alter_item[1].strip().strip(',').replace('`', '').strip()
				elif command == 'ADD':
					alter_string = alter_item[1].strip()
					alter_column=self.m_alter_column.search(alter_string)
					default_value = self.m_default_value.search(alter_string)
					if alter_column:
						alter_dic["command"] = command
						alter_dic["name"] = alter_column.group(1).strip().strip('`')
						alter_dic["type"] = alter_column.group(2).lower()
						try:
							alter_dic["dimension"]=alter_column.group(3).replace('|', ',').strip()
						except:
							alter_dic["dimension"]=0
						if default_value:
							alter_dic["default"] = default_value.group(2)
						else:
							alter_dic["default"] = None
						
				elif command == 'CHANGE':
					alter_dic["command"] = command
					alter_column = self.m_alter_change.search(alter_item[1].strip())
					
					if alter_column:
						alter_dic["command"] = command
						alter_dic["old"] = alter_column.group(1).strip().strip('`')
						alter_dic["new"] = alter_column.group(2).strip().strip('`')
						alter_dic["type"] = alter_column.group(3).strip().strip('`').lower()
						alter_dic["name"] = alter_column.group(1).strip().strip('`')
						try:
							alter_dic["dimension"]=alter_column.group(4).replace('|', ',').strip()
						except:
							alter_dic["dimension"]=0
					
				elif command == 'MODIFY':
					alter_column=self.m_alter_column.search(alter_item[1].strip())
					if alter_column:
						alter_dic["command"] = command
						alter_dic["name"] = alter_column.group(1).strip().strip('`')
						alter_dic["type"] = alter_column.group(2).lower()
						try:
							alter_dic["dimension"]=alter_column.group(3).replace('|', ',').strip()
						except:
							alter_dic["dimension"]=0
				alter_cmd.append(alter_dic)
			stat_dic["alter_cmd"]=alter_cmd
		return stat_dic
		
	def parse_sql(self, sql_string):
		"""
			The method cleans and parses the sql string
			A regular expression replaces all the default value definitions with a space.
			Then the statements are split in a list using the statement separator;
		
			For each statement a set of regular expressions remove the comments, single and multi line.
			Parenthesis are surrounded by spaces and commas are rewritten in order to get at least one space after the comma.
			The statement is then put on a single line and stripped.
			
			Six different match are performed on the statement.
			CREATE TABLE
			DROP TABLE
			ALTER TABLE
			ALTER INDEX
			DROP PRIMARY KEY
			TRUNCATE TABLE
			
			The match which is successful determines the parsing of the rest of the statement.
			Each parse builds a dictionary with at least two keys.
			Name and Command. 
			Each statement comes with specific keys.
			
			When the token dictionary is complete is added to the class list tokenised
			
			:param sql_string: The sql string with the sql statements.
		"""
		#sql_string=re.sub(r'\s+default(.*?),', ' ', sql_string, re.IGNORECASE)
		statements=sql_string.split(';')
		for statement in statements:
			
			stat_dic={}
			stat_cleanup=re.sub(r'/\*.*?\*/', '', statement, re.DOTALL)
			stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)
			stat_cleanup=re.sub(r'[\b)\b]', ' ) ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b(\b]', ' ( ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b,\b]', ', ', stat_cleanup)
			stat_cleanup=stat_cleanup.replace('\n', ' ')
			stat_cleanup = re.sub("\([\w*\s*]\)", " ",  stat_cleanup)
			stat_cleanup = stat_cleanup.strip()
			mcreate_table = self.m_create_table.match(stat_cleanup)
			mdrop_table = self.m_drop_table.match(stat_cleanup)
			malter_table = self.m_alter_table.match(stat_cleanup)
			malter_index = self.m_alter_index.match(stat_cleanup)
			mdrop_primary = self.m_drop_primary.match(stat_cleanup)
			mtruncate_table = self.m_truncate_table.match(stat_cleanup)
			
			if mcreate_table:
				command=' '.join(mcreate_table.group(1).split()).upper().strip()
				stat_dic["command"]=command
				stat_dic["name"]=mcreate_table.group(2)
				create_parsed=self.parse_create_table(stat_cleanup, stat_dic["name"])
				stat_dic["columns"]=create_parsed["columns"]
				stat_dic["indices"]=create_parsed["indices"]				
			elif mdrop_table:
				command=' '.join(mdrop_table.group(1).split()).upper().strip()
				stat_dic["command"]=command
				stat_dic["name"]=mdrop_table.group(2)
			elif mtruncate_table:
				command=' '.join(mtruncate_table.group(1).split()).upper().strip()
				stat_dic["command"]=command
				stat_dic["name"]=mtruncate_table.group(2)
			elif mdrop_primary:
				stat_dic["command"]="DROP PRIMARY KEY"
				stat_dic["name"]=mdrop_primary.group(1).strip().strip(',').replace('`', '').strip()
			elif malter_index:
				pass
			elif malter_table:
				stat_dic=self.parse_alter_table(malter_table)
				if len(stat_dic["alter_cmd"]) == 0:
					stat_dic = {}
				
			if stat_dic!={}:
				self.tokenised.append(stat_dic)
		
