#!/bin/sh
gnuplot << EOF
set datafile separator ','
set terminal png
set output "$1.png"
set xlabel "Time [Seconds]"
set ylabel "Throughput [TPs]"
set title "TPC-C Benchmark - rate: 300, terminal: 5"
plot [] [0:350] "$1" using 1:2 t "Migration Join - Lazy" w l
EOF
