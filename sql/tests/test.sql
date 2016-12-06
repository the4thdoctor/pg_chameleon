CREATE TABLE test (
  id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  value1 VARCHAR(45) NOT NULL,
  value2 VARCHAR(45) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (id),
  KEY idx_actor_last_name (value2)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO test (value1,value2)
VALUES
('hello','dave'),
('knock knock','neo'),
('the answer','is 42');
ALTER TABLE test
ADD COLUMN `count` SMALLINT(6) NULL ,
ADD COLUMN `log` VARCHAR(12) NULL AFTER `count`,
ADD COLUMN new_enum ENUM('asd','r') NULL AFTER `log`,
ADD COLUMN status INT(10) UNSIGNED NULL AFTER `new_enum`;
ALTER TABLE test
DROP COLUMN `count` ,
ADD COLUMN status_2 INT(10) UNSIGNED NULL AFTER `new_enum`;
DELETE FROM test WHERE value1='the answer';
UPDATE test SET value2 = 'world' WHERE value1 = 'hello';
