#!/usr/bin/env python
from pg_chameleon import sql_token


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
ADD COLUMN mydate datetime NULL AFTER `status`,
ADD COLUMN mytstamp timestamp NULL AFTER `status`,
DROP FOREIGN            KEY fk_trigger_bar,
add primary key,
drop unique index asdf
"""
statement="""
CREATE TABLE film_text (
  film_id SMALLINT NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  PRIMARY KEY  (film_id),
   KEY idx_title_description (title,description)
)ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

statement="""
CREATE TABLE film_text (
  film_id SMALLINT NOT NULL PRIMARY KEY
)ENGINE=InnoDB DEFAULT CHARSET=utf8
"""
#statement="""RENAME TABLE `sakila`.`test_partition` TO `sakila`.`_test_partition_old`, `_test_partition_new` TO `test_partition`;"""
#statement="""RENAME TABLE test_partition TO _test_partition_old, _test_partition_new TO test_partition; """
#statement="""RENAME TABLE sakila.test_partition TO sakila._test_partition_old, sakila._test_partition_new TO sakila.test_partition ;"""
#statement="""RENAME TABLE `sakila`.`test_partition` TO `sakila`.`_test_partition_old`, `sakila`.`_test_partition_new` TO `sakila`.`test_partition`;"""
#statement = """create table blah(id integer(30) not null auto_increment, datevalue datetime,primary key (id,datevalue))"""
#statement = """alter table dd add column(foo varchar(30)); alter table dd add column foo varchar(30);"""
#statement = """create table test_tiny(id int(4) auto_increment, value tinyint(1), unique key(id),unique key (id,value),unique key (value,id)); """


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
statement="""ALTER TABLE t_user_info ADD (
group_id INT(11) UNSIGNED DEFAULT NULL,
contact_phone VARCHAR(20) DEFAULT NULL
);"""
statement = """ALTER TABLE foo RENAME TO bar;"""
statement = """RENAME TABLE `sakila`.`test_partition` TO `sakila`.`_test_partition_old`, `_test_partition_new` TO `test_partition`;"""
#statement="""ALTER TABLE foo MODIFY bar INT UNSIGNED DEFAULT NULL;"""
#statement="""ALTER TABLE foo change bar bar INT UNSIGNED;"""
statement="""ALTER TABLE `some_sch`.`my_great_table` CHANGE COLUMN `IMEI` `IMEI` VARCHAR(255) NULL DEFAULT NULL COMMENT 'IMEI datatype changed'"""
token_sql=sql_token()
token_sql.parse_sql(statement)
print (token_sql.tokenised)
#for token in token_sql.tokenised:
	#print (token)
#	for column in token["columns"]:
#		print(column)
	#else:	

