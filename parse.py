#!/usr/bin/env python
from pg_chameleon import sql_token

statement=""" 



alter table `test` 
					drop primary key ; 

				"""

token_sql=sql_token()
token_sql.parse_sql(statement)
#print token_sql.tokenised
for token in token_sql.tokenised:
	if   token["command"]=="ALTER TABLE":
		print token["alter_cmd"]
	elif token["command"]=="CREATE TABLE":	
		print token
	print token
