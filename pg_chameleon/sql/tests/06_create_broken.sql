CREATE TABLE test_broken (
  id SMALLINT UNSIGNED NULL AUTO_INCREMENT,
  value1 VARCHAR(45) NOT NULL,
  value2 VARCHAR(45) NOT NULL,
  val_enum enum('postgresql','rocks') null,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (id),
  KEY idx_actor_last_name (value2)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
