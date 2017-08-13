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
ADD COLUMN `count` SMALLINT(6) NOT NULL default 3 AFTER `test_enum`,
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
#statement = """alter table test add column `test_default` varchar(30) not null default 20  """
#statement = """ALTER TABLE test
#ADD COLUMN `count` SMALLINT(6) NULL ,
#ADD COLUMN `log` VARCHAR(12) NULL default 'blah' AFTER `count`,
#ADD COLUMN new_enum ENUM('asd','r') NULL AFTER `log`,
#ADD COLUMN status INT(10) UNSIGNED NULL AFTER `new_enum`
#"""

statement = """ALTER TABLE foo DROP FOREIGN KEY fk_trigger_bar,ADD COLUMN `count` SMALLINT(6) NULL;"""
statement = """ALTER TABLE test
ADD `count` SMALLINT(6) NULL ,
ADD COLUMN `log` VARCHAR(12) default 'blah' NULL AFTER `count`,
ADD COLUMN new_enum ENUM('asd','r') NULL AFTER `log`,
ADD COLUMN status INT(10) UNSIGNED NULL AFTER `new_enum`,
DROP FOREIGN            KEY fk_trigger_bar,
add primary key,
drop unique index asdf
"""
statement="""alter table kp_web_log add key idx_idx_mobile(idx_mobile)"""
print(statement)
token_sql=sql_token()
token_sql.parse_sql(statement)
for token in token_sql.tokenised:
	print (token)
#	for column in token["columns"]:
#		print(column)
	#else:	

