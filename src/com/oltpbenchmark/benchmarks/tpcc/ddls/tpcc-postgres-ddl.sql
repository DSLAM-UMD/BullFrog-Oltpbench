-- TODO: c_since ON UPDATE CURRENT_TIMESTAMP,

DROP TABLE IF EXISTS order_line CASCADE;
CREATE TABLE order_line (
  -- id SERIAL,
  ol_w_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_o_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_delivery_d timestamp NULL DEFAULT NULL,
  ol_amount decimal(6,2) NOT NULL,
  ol_supply_w_id int NOT NULL,
  ol_quantity decimal(2,0) NOT NULL,
  ol_dist_info char(24) NOT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

DROP TABLE IF EXISTS new_order CASCADE;
CREATE TABLE new_order (
  no_w_id int NOT NULL,
  no_d_id int NOT NULL,
  no_o_id int NOT NULL,
  PRIMARY KEY (no_w_id,no_d_id,no_o_id)
);

DROP TABLE IF EXISTS stock CASCADE;
CREATE TABLE stock (
  -- id SERIAL,
  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_quantity decimal(4,0) NOT NULL,
  s_ytd decimal(8,2) NOT NULL,
  s_order_cnt int NOT NULL,
  s_remote_cnt int NOT NULL,
  s_data varchar(50) NOT NULL,
  s_dist_01 char(24) NOT NULL,
  s_dist_02 char(24) NOT NULL,
  s_dist_03 char(24) NOT NULL,
  s_dist_04 char(24) NOT NULL,
  s_dist_05 char(24) NOT NULL,
  s_dist_06 char(24) NOT NULL,
  s_dist_07 char(24) NOT NULL,
  s_dist_08 char(24) NOT NULL,
  s_dist_09 char(24) NOT NULL,
  s_dist_10 char(24) NOT NULL,
  PRIMARY KEY (s_w_id,s_i_id)
);

-- TODO: o_entry_d  ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS oorder CASCADE;
CREATE TABLE oorder (
  o_w_id int NOT NULL,
  o_d_id int NOT NULL,
  o_id int NOT NULL,
  o_c_id int NOT NULL,
  o_carrier_id int DEFAULT NULL,
  o_ol_cnt decimal(2,0) NOT NULL,
  o_all_local decimal(1,0) NOT NULL,
  o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (o_w_id,o_d_id,o_id),
  UNIQUE (o_w_id,o_d_id,o_c_id,o_id)
);

-- TODO: h_date ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS history CASCADE;
CREATE TABLE history (
  h_c_id int NOT NULL,
  h_c_d_id int NOT NULL,
  h_c_w_id int NOT NULL,
  h_d_id int NOT NULL,
  h_w_id int NOT NULL,
  h_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  h_amount decimal(6,2) NOT NULL,
  h_data varchar(24) NOT NULL
);

DROP TABLE IF EXISTS customer CASCADE;
CREATE TABLE customer (
  c_w_id int NOT NULL,
  c_d_id int NOT NULL,
  c_id int NOT NULL,
  c_discount decimal(4,4) NOT NULL,
  c_credit char(2) NOT NULL,
  c_last varchar(16) NOT NULL,
  c_first varchar(16) NOT NULL,
  c_credit_lim decimal(12,2) NOT NULL,
  c_balance decimal(12,2) NOT NULL,
  c_ytd_payment float NOT NULL,
  c_payment_cnt int NOT NULL,
  c_delivery_cnt int NOT NULL,
  c_street_1 varchar(20) NOT NULL,
  c_street_2 varchar(20) NOT NULL,
  c_city varchar(20) NOT NULL,
  c_state char(2) NOT NULL,
  c_zip char(9) NOT NULL,
  c_phone char(16) NOT NULL,
  c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  c_middle char(2) NOT NULL,
  c_data varchar(500) NOT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id)
);

DROP TABLE IF EXISTS district CASCADE;
CREATE TABLE district (
  d_w_id int NOT NULL,
  d_id int NOT NULL,
  d_ytd decimal(12,2) NOT NULL,
  d_tax decimal(4,4) NOT NULL,
  d_next_o_id int NOT NULL,
  d_name varchar(10) NOT NULL,
  d_street_1 varchar(20) NOT NULL,
  d_street_2 varchar(20) NOT NULL,
  d_city varchar(20) NOT NULL,
  d_state char(2) NOT NULL,
  d_zip char(9) NOT NULL,
  PRIMARY KEY (d_w_id,d_id)
);


DROP TABLE IF EXISTS item CASCADE;
CREATE TABLE item (
  i_id int NOT NULL,
  i_name varchar(24) NOT NULL,
  i_price decimal(5,2) NOT NULL,
  i_data varchar(50) NOT NULL,
  i_im_id int NOT NULL,
  PRIMARY KEY (i_id)
);

DROP TABLE IF EXISTS warehouse CASCADE;
CREATE TABLE warehouse (
  w_id int NOT NULL,
  w_ytd decimal(12,2) NOT NULL,
  w_tax decimal(4,4) NOT NULL,
  w_name varchar(10) NOT NULL,
  w_street_1 varchar(20) NOT NULL,
  w_street_2 varchar(20) NOT NULL,
  w_city varchar(20) NOT NULL,
  w_state char(2) NOT NULL,
  w_zip char(9) NOT NULL,
  PRIMARY KEY (w_id)
);


--add constraints and indexes
CREATE INDEX idx_customer_name ON customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_order ON oorder (o_w_id,o_d_id,o_c_id,o_id);
-- tpcc-mysql create two indexes for the foreign key constraints, Is it really necessary?

--add 'ON DELETE CASCADE'  to clear table work correctly

ALTER TABLE district  ADD CONSTRAINT fkey_district_1 FOREIGN KEY(d_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
ALTER TABLE customer ADD CONSTRAINT fkey_customer_1 FOREIGN KEY(c_w_id,c_d_id) REFERENCES district(d_w_id,d_id)  ON DELETE CASCADE ;
ALTER TABLE history  ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
ALTER TABLE history  ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id) ON DELETE CASCADE;
ALTER TABLE new_order ADD CONSTRAINT fkey_new_order_1 FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES oorder(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
ALTER TABLE oorder ADD CONSTRAINT fkey_order_1 FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_1 FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES oorder(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2 FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(s_w_id,s_i_id) ON DELETE CASCADE;
ALTER TABLE stock ADD CONSTRAINT fkey_stock_1 FOREIGN KEY(s_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
ALTER TABLE stock ADD CONSTRAINT fkey_stock_2 FOREIGN KEY(s_i_id) REFERENCES item(i_id) ON DELETE CASCADE;

-- join migration
DROP TABLE IF EXISTS orderline_stock CASCADE;
CREATE TABLE orderline_stock (
  -- id SERIAL,
  ol_w_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_o_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_delivery_d timestamp NULL DEFAULT NULL,
  ol_amount decimal(6,2) NOT NULL,
  ol_supply_w_id int NOT NULL,
  ol_quantity decimal(2,0) NOT NULL,
  ol_dist_info char(24) NOT NULL,

  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_quantity decimal(4,0) NOT NULL,
  s_ytd decimal(8,2) NOT NULL,
  s_order_cnt int NOT NULL,
  s_remote_cnt int NOT NULL,
  s_data varchar(50) NOT NULL,
  s_dist_01 char(24) NOT NULL,
  s_dist_02 char(24) NOT NULL,
  s_dist_03 char(24) NOT NULL,
  s_dist_04 char(24) NOT NULL,
  s_dist_05 char(24) NOT NULL,
  s_dist_06 char(24) NOT NULL,
  s_dist_07 char(24) NOT NULL,
  s_dist_08 char(24) NOT NULL,
  s_dist_09 char(24) NOT NULL,
  s_dist_10 char(24) NOT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number,s_w_id,s_i_id)
);

CREATE INDEX FKEY_STOCK_2 ON STOCK (S_I_ID);
CREATE INDEX s_order_1 ON stock (s_w_id, s_quantity);
-- CREATE INDEX s_order_2 ON stock (s_w_id, s_i_id);

CREATE INDEX FKEY_ORDER_LINE_2 ON ORDER_LINE (OL_I_ID);
CREATE INDEX ol_order_1 ON order_line (ol_o_id, ol_d_id, ol_w_id);
-- CREATE INDEX ol_order_2 ON order_line (ol_w_id,ol_d_id,ol_o_id,ol_number);

CREATE INDEX os_order_1 ON orderline_stock (ol_o_id, ol_d_id, ol_w_id);
CREATE INDEX os_order_2 ON orderline_stock (ol_o_id, ol_d_id, ol_w_id, ol_number); 
CREATE INDEX os_order_3 ON orderline_stock (s_i_id);
CREATE INDEX os_order_4 ON orderline_stock (s_w_id, s_quantity);
CREATE INDEX os_order_5 ON orderline_stock (s_w_id, s_i_id);
CREATE INDEX os_order_6 ON orderline_stock (ol_i_id);

CREATE OR REPLACE VIEW orderline_stock_v AS
(
  SELECT *
  FROM order_line, stock
  WHERE ol_i_id = s_i_id
);

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
-- CREATE INDEX idx_customer_name2 ON customer_proj (c_w_id,c_d_id,c_id);

CREATE OR REPLACE VIEW customer_proj_v AS
(
  SELECT c_w_id, c_d_id, c_id, c_credit, c_last,
  c_first, c_balance, c_ytd_payment, c_payment_cnt,
  c_delivery_cnt, c_street_1, c_city, c_state, c_zip, c_data
  FROM customer
);

-- aggregation migration
DROP TABLE IF EXISTS orderline_agg CASCADE;
CREATE TABLE orderline_agg (
  ol_amount_sum decimal(12,2) NOT NULL,
  ol_quantity_avg decimal(4,0) NOT NULL,
  ol_o_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_w_id int NOT NULL,
  PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id)
);

CREATE OR REPLACE VIEW orderline_agg_v AS
(
  SELECT sum(ol_amount), avg(ol_quantity), ol_o_id, ol_d_id, ol_w_id
  FROM order_line
  GROUP BY ol_o_id, ol_d_id, ol_w_id
);

-- two splits migration

CREATE OR REPLACE VIEW os_orderline_split_v AS
(
  SELECT  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
          ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info,
          s_w_id, s_i_id, s_quantity 
  FROM orderline_stock
);

CREATE OR REPLACE VIEW os_stock_split_v AS
(
  SELECT  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
          s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
          s_dist_07, s_dist_08, s_dist_09, s_dist_10,
          ol_w_id, ol_d_id, ol_o_id
  FROM orderline_stock
);
