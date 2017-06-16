#!/usr/bin/env python
from pg_chameleon import sql_token

statement=""" 

-- drop table
DROP TABLE `test`;
-- create table
CREATE   TABLE `test` (
  store_id TINYINT UNSIGNED NULL AUTO_INCREMENT,
  manager_staff_id TINYINT UNSIGNED NOT NULL,
  address_id SMALLINT UNSIGNED NOT NULL,
  `address_txt` varchar (30) NOT NULL default 'default_t;ext',
  `address_dp` double precision (30,2) NOT NULL,
  `test_enum` enum ('a','b'),
  size ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (store_id,address_id),
  UNIQUE KEY idx_unique_manager (manager_staff_id),
  KEY idx_fk_address_id2 (address_id),
  index
  idx_fk_address_id (address_id,store_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


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

#statement="""alter table test_pk drop primary key;"""
#statement="""ALTER TABLE test ADD COLUMN `dkdkd` timestamp NULL;"""
#statement="""create table test_pk (id int ,PRIMARY KEY  (id) ); """
#statement="""alter table test change   date_create_new date_create_new timestamp;"""
#statement="""ALTER TABLE `test_table` MODIFY `test_column` bigint(20) DEFAULT NULL; 
#ALTER TABLE table2 CHANGE column1 column2 bigint(20);
#ALTER TABLE `test_table` MODIFY `test_column` enum('blah','dd') DEFAULT NULL; """
#statement="""ALTER TABLE `test_table` ADD UNIQUE INDEX `idx_unique` (`log`, `status`);"""
#statement = """CREATE TABLE test (`id` integer null auto_increment primary key, `test_col` bigint(20)) ;"""
#statement = """CREATE TABLE TEST(ID integer auto_increment primary key);"""
#statement = """CREATE TABLE test (id integer auto_increment, primary key(`id`)  )"""
#statement = """TRUNCATE table  `test`;"""
statement = """"""
token_sql=sql_token()
token_sql.parse_sql(statement)
print (token_sql.tokenised)
for token in token_sql.tokenised:
	print(token)
#	if   token["command"]=="ALTER TABLE":
#		alter_cmd = token["alter_cmd"][0]
#		if alter_cmd["command"] == "MODIFY" and alter_cmd["type"] == 'enum':
#			print(alter_cmd["dimension"].split(','))
	#else:	
	
