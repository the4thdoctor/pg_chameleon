DROP TABLE IF EXISTS test_partition;
CREATE TABLE test_partition (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  id_partition int(10) NULL,
  PRIMARY KEY  (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


DELIMITER $$
DROP PROCEDURE IF EXISTS prepare_data$$
DELIMITER ;
DELIMITER $$
CREATE PROCEDURE prepare_data()
BEGIN
  DECLARE v_part INT DEFAULT 1;
  DECLARE rnd_val INT DEFAULT 1;
  WHILE v_part < 50000 DO
    SET rnd_val = FLOOR(RAND() * (30000 - 1 + 1)) + 1 ;
    INSERT INTO test_partition (id_partition) VALUES (rnd_val);
    SET v_part = v_part + 1;
  END WHILE;
END$$
DELIMITER ;


DROP TABLE IF EXISTS test_partition2;
CREATE TABLE test_partition2 (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  id_partition int(10) NOT NULL,
  PRIMARY KEY  (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


DELIMITER $$
DROP PROCEDURE IF EXISTS prepare_data2$$
DELIMITER ;
DELIMITER $$
CREATE PROCEDURE prepare_data2()
BEGIN
  DECLARE v_part INT DEFAULT 1;
  DECLARE rnd_val INT DEFAULT 1;
  WHILE v_part < 50000 DO
    SET rnd_val = FLOOR(RAND() * (30000 - 1 + 1)) + 1 ;
    INSERT INTO test_partition2 (id_partition) VALUES (rnd_val);
    SET v_part = v_part + 1;
  END WHILE;
END$$
DELIMITER ;
