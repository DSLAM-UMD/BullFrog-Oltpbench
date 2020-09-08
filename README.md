# BullFrog-OLTPBench


## Compile and Install BullFrog

```
git clone https://github.com/DSLAM-UMD/BullFrog
cd postgresql-11.0
./configure --prefix=$POSTGRES_INSTALLDIR --enable-cassert --enable-debug CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer"
make -j8
make install
```


