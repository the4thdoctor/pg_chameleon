TRUNCATE TABLE test;
CREATE TEMPORARY TABLE tmp_test(
  id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  value1 VARCHAR(45) NOT NULL,
  PRIMARY KEY  (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into tmp_test (value1) values('blah'),('blah');
insert into test (value1) values('blah');
DROP TEMPORARY TABLE if exists tmp_test ;
