CREATE TABLE IF NOT EXISTS cars(
   carId CHAR(10) unique,
   manufacturer VARCHAR(20),
   model VARCHAR(20),
   b_price DECIMAL,
   ph_price DECIMAL,
   deposit INTEGER,
   PRIMARY KEY( carId )
);

CREATE TABLE IF NOT EXISTS users(
   uid INTEGER NOT NULL,
   cid CHAR(10),
   b_timestamp TIMESTAMP without time zone NOT NULL,
   delta_sec INTEGER,
   CONSTRAINT fk_customer
      FOREIGN KEY(cid) 
	  REFERENCES cars(carId)
);