cd ~/lazyschema/so-postgres/postgresql-11.0/
source env.sh
pg_ctl -D $PGDATA stop

cd ~/lazyschema/postgres/
source env.sh
pg_ctl -D $PGDATA restart

cd ~/lazyschema/oltpbench/

./oltpbenchmark -b tpcc -c config/pgtpcc_base_join.xml --create=true --load=true --port=5435 --path=/home/gangliao/lazyschema/postgres/dev/bin

./oltpbenchmark -b tpcc -c config/pgtpcc_base_join.xml  --execute=true -s 5 -o base_join  --port=5435 --path=/home/gangliao/lazyschema/postgres/dev/bin --migration=6

