DROP TABLE IF EXISTS TARGET;

CREATE TABLE TARGET  (
	ID BIGINT  NOT NULL PRIMARY KEY ,
	IP VARCHAR(20) NOT NULL,
	PORT INT NOT NULL,
	CONNECTED BOOLEAN NULL,
	BANNER VARCHAR(255)
) ENGINE=InnoDB;
