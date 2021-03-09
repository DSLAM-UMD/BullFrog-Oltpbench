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

