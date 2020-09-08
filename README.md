# BullFrog-OLTPBench


## Compile and Install BullFrog

```
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


