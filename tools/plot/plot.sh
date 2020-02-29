#!/bin/sh
gnuplot << EOF
set datafile separator ','
set terminal png
set output "$1.png"
set xlabel "Time [Seconds]"
set ylabel "Throughput [TPs]"
set title "TPC-C Benchmark - rate: 100"
plot "$1" using 1:2 t "Migration Join - Baseline" w l
EOF
