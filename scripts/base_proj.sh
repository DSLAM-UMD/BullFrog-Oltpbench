cd ~/lazyschema/so-postgres/postgresql-11.0/
source env.sh
pg_ctl -D $PGDATA stop

cd ~/lazyschema/postgres/
source env.sh
pg_ctl -D $PGDATA restart

cd ~/lazyschema/oltpbench/

./oltpbenchmark -b tpcc -c config/pgtpcc_base_proj.xml --create=true --load=true --port=5434 --path=/home/gangliao/lazyschema/postgres/dev/bin

./oltpbenchmark -b tpcc -c config/pgtpcc_base_proj.xml  --execute=true -s 5 -o base_proj  --port=5434 --path=/home/gangliao/lazyschema/postgres/dev/bin --migration=6

