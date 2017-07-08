DROP TABLE IF EXISTS test_pkeyless;
CREATE TABLE test_pkeyless (
  id SMALLINT UNSIGNED NOT NULL,
  value1 VARCHAR(45) NOT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO test_pkeyless (id,value1)
VALUES
(1,'dave'),
(2,'neo'),
(3,'phoenix'),
(4,'is 42');

/*
stop the replica then run  call prepare_data then run this additional inserts 

call prepare_data;
INSERT INTO test_pkeyless (id,value1)
VALUES
(5,'rainbow'),
(6,'twilight'),
(7,'pinkie'),
(8,'apple'),
(9,'rarity')
;



*/

ALTER TABLE `test_pkeyless` ADD COLUMN `id_pkey` INT AUTO_INCREMENT PRIMARY KEY FIRST; 
