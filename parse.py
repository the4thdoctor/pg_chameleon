#!/usr/bin/env python
from pg_chameleon import sql_json

statement=""" 



--
-- Table structure for table `store`
--

CREATE temporary TABLE if exists `store` (
  store_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,  manager_staff_id TINYINT UNSIGNED NOT NULL,
  
  CONSTRAINT fk_store_staff FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_store_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


				"""

parsesql=sql_json()
parsesql.parse_sql(statement)
