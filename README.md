# BullFrog-OLTPBench

## BullFrog

### Compile and Install BullFrog

```shell
git clone https://github.com/DSLAM-UMD/BullFrog

# choose one branch -- one type of schema migration 
# git checkout migrate-projection-on-bitmap


export POSTGRES_INSTALLDIR=$PWD/dev
export LD_LIBRARY_PATH=$POSTGRES_INSTALLDIR/lib:$LD_LIBRARY_PATH
export PATH=$POSTGRES_INSTALLDIR/bin:$PATH
export PGDATA=$POSTGRES_INSTALLDIR/data
mkdir -p $POSTGRES_INSTALLDIR

cd postgresql-11.0
./configure --prefix=$POSTGRES_INSTALLDIR --enable-cassert --enable-debug CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer"
make -j8
make install
```

### Deploy BullFrog

```shell
# create a new PostgreSQL database cluster
rm -rf $PGDATA
initdb -D $PGDATA

# you can now start the database server
# $ pg_ctl -D $PGDATA -l $POSTGRES_INSTALLDIR/logfile start
pg_ctl -D $PGDATA -o "-F -p 5433" start
pg_ctl -D $PGDATA status

# create database: (we assume here that you used post number as 5433 above)
createdb -h localhost -p 5433 tpcc

# create role
psql -h localhost -p 5433 tpcc -c "CREATE USER postgres WITH SUPERUSER PASSWORD 'postgres';"
```

### Stop BullFrog

```shell
$ pg_ctl -D $PGDATA stop
waiting for server to shut down.... done
server stopped
$ pg_ctl -D $PGDATA status
pg_ctl: no server running
```

### Oltp-bench

