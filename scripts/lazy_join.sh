cd ~/lazyschema/postgres/
source env.sh
pg_ctl -D $PGDATA stop

cd ~/lazyschema/so-postgres/postgresql-11.0/
source env.sh
pg_ctl -D $PGDATA restart

cd ~/lazyschema/oltpbench/

./oltpbenchmark -b tpcc -c config/pgtpcc_lazy_join.xml --create=true --load=true --port=5436 --path=/home/gangliao/lazyschema/so-postgres/postgresql-11.0/dev/bin

./oltpbenchmark -b tpcc -c config/pgtpcc_lazy_join.xml --execute=true -s 5 -o lazy_join --port=5436 --path=/home/gangliao/lazyschema/so-postgres/postgresql-11.0/dev/bin

