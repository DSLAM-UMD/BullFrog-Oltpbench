cd ~/lazyschema/postgres/
source env.sh
pg_ctl -D $PGDATA stop

cd ~/lazyschema/so-postgres/postgresql-11.0/
source env.sh
pg_ctl -D $PGDATA restart

cd ~/lazyschema/oltpbench/

./oltpbenchmark -b tpcc -c config/pgtpcc_lazy_split.xml --create=true --load=true --port=5433 --path=/Users/liaogang/Downloads/postgresql-11.0/dev/bin
# ./oltpbenchmark -b tpcc -c config/pgtpcc_lazy_split.xml --create=true --load=true --port=5433 --path=/home/gangliao/lazyschema/so-postgres/postgresql-11.0/dev/bin 

sql = "insert into orderline_stock(
    ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
    ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id,
    s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
    s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
    s_dist_07, s_dist_08, s_dist_09, s_dist_10)
    (select
     ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
     ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id,
     s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
     s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
     s_dist_07, s_dist_08, s_dist_09, s_dist_10 from order_line, stock 
     where ol_i_id = s_i_id);"

echo \"$sql\" | psql -qS -1 -p 5433 tpcc 

./oltpbenchmark -b tpcc -c config/pgtpcc_lazy_split.xml --execute=true -s 5 -o lazy_proj --port=5433 --path=/Users/liaogang/Downloads/postgresql-11.0/dev/bin
# ./oltpbenchmark -b tpcc -c config/pgtpcc_lazy_split.xml --execute=true -s 5 -o lazy_proj --port=5433 --path=/home/gangliao/lazyschema/so-postgres/postgresql-11.0/dev/bin
