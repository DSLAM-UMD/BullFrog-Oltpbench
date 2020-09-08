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

## OLTP-BENCH

### Build

```shell
git clone https://github.com/DSLAM-UMD/BullFrog-Oltpbench
ant resolve
ant build
```

### TPC-C configuration file

There are many conf files under [`config`](https://github.com/DSLAM-UMD/BullFrog-Oltpbench/tree/master/config) folder. You can pick any of them if it relates to TPC-C. 

```xml
<?xml version="1.0"?>
<parameters>
    <dbtype>postgres</dbtype>
    <driver>org.postgresql.Driver</driver>
    <DBUrl>jdbc:postgresql://localhost:5433/tpcc</DBUrl>
    <DBName>tpcc</DBName>
    <username>postgres</username>
    <password>postgres</password>
    <terminals>10</terminals>
    
    <scalefactor>10</scalefactor>
    <uploadCode></uploadCode>
    <uploadUrl></uploadUrl>
    
   
        <transactiontypes>
        <transactiontype>
                <name>NewOrder</name>
                <id>1</id>
        </transactiontype>
        <transactiontype>
                <name>Payment</name>
                <id>2</id>
        </transactiontype>
        <transactiontype>
                <name>OrderStatus</name>
                <id>3</id>
        </transactiontype>
        <transactiontype>
                <name>Delivery</name>
                <id>4</id>
        </transactiontype>
        <transactiontype>
                <name>StockLevel</name>
                <id>5</id>
        </transactiontype>
        </transactiontypes>
   
    <works>
        <work>
          <time>20</time>
          <rate>10</rate>
          <weights>20,20,20,20,20</weights>
        </work>
        </works>

</parameters
```

### Load TPC-C Dataset

Since data loading can be a lengthy process, one would first create a and populate a database which can be reused for multiple experiments:

```shell
./oltpbenchmark -b tpcc -c config/pgtpcc_config.xml --create=true --load=true
```

### Run TPC-C

```shell
./oltpbenchmark -b tpcc -c config/pg_tpcc_config.xml --execute=true -s 5 -o outputfile
```
