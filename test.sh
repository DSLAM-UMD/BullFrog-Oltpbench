cd ~/lazyschema/so-postgres/postgresql-11.0/
source env.sh
pg_ctl -D $PGDATA stop

cd ~/lazyschema/postgres/
source env.sh
pg_ctl -D $PGDATA restart

cd ~/lazyschema/oltpbench/

./oltpbenchmark -b tpcc -c config/pgtpcc_test0_config.xml --create=true --load=true --port=5435 --path=/home/gangliao/lazyschema/postgres/dev/bin

./oltpbenchmark -b tpcc -c config/pgtpcc_test0_config.xml  --execute=true -s 5 -o test0  --port=5435 --path=/home/gangliao/lazyschema/postgres/dev/bin --migration=1

