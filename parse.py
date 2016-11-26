#!/usr/bin/env python
from pg_chameleon import sql_token

statement=""" 



ALTER TABLE test
ADD COLUMN `count` SMALLINT(6) NOT NULL AFTER `test_enum`,
ADD COLUMN `log` VARCHAR(12) NOT NULL AFTER `count`,
ADD COLUMN new_enum ENUM('asd','r') NOT NULL AFTER `log`,
ADD COLUMN status INT(10) UNSIGNED NOT NULL AFTER `new_enum`;


ALTER TABLE `test`
DROP COLUMN `count` ,
ADD COLUMN newstatus INT(10) UNSIGNED NOT NULL AFTER `log`;

ALTER TABLE `test` DROP PRIMARY KEY;

				"""

token_sql=sql_token()
token_sql.parse_sql(statement)
#print token_sql.tokenised
for token in token_sql.tokenised:
	if   token["command"]=="ALTER TABLE":
		print token["alter_cmd"]
