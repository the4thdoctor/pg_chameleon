#!/usr/bin/env python
from pg_chameleon import sql_token

statement=""" 


--drop table
DROP TABLE `test;
--create table
CREATE   TABLE `test` (
  store_id TINYINT UNSIGNED NULL AUTO_INCREMENT,
  manager_staff_id TINYINT UNSIGNED NOT NULL,
  address_id SMALLINT UNSIGNED NOT NULL,
  `address_txt` `varchar` (30) NOT NULL,
  `address_dp` double precision (30,2) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (store_id,address_id),
  UNIQUE KEY idx_unique_manager (manager_staff_id),
  KEY idx_fk_address_id (address_id),
  index
  idx_fk_address_id (address_id,store_id),
  CONSTRAINT fk_store_staff FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_store_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

				"""

token_sql=sql_token()
token_sql.parse_sql(statement)
for token in token_sql.tokenised:
	print token
#print parsesql.query_list
