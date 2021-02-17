-- projection migration
DROP TABLE IF EXISTS customer_proj1 CASCADE;
CREATE TABLE customer_proj1 (
  -- id SERIAL,
  c_w_id int NOT NULL,
  c_d_id int NOT NULL,
  c_id int NOT NULL,
  c_discount decimal(4,4) NOT NULL,
  c_credit char(2) NOT NULL,
  c_last varchar(16) NOT NULL,
  c_first varchar(16) NOT NULL,
  c_balance decimal(12,2) NOT NULL,
  c_ytd_payment float NOT NULL,
  c_payment_cnt int NOT NULL,
  c_delivery_cnt int NOT NULL,
  c_data varchar(500) NOT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id)
);


-- projection migration
DROP TABLE IF EXISTS customer_proj2 CASCADE;
CREATE TABLE customer_proj2 (
  -- id SERIAL,
  c_w_id int NOT NULL,
  c_d_id int NOT NULL,
  c_id int NOT NULL,
  c_last varchar(16) NOT NULL,
  c_first varchar(16) NOT NULL,
  c_street_1 varchar(20) NOT NULL,
  c_city varchar(20) NOT NULL,
  c_state char(2) NOT NULL,
  c_zip char(9) NOT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id)
);


CREATE INDEX idx_customer_name1 ON customer_proj1 (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_customer_name2 ON customer_proj2 (c_w_id,c_d_id,c_last,c_first);
