#!/bin/bash

sb_bench_bin="./build/main"
log_file="./result.txt"

for bench in "select" "where" "alterdur" "aggregate" "join" "yahoo"
do
    for cores in 1 2 4 8 16 32 64
    do
        for size in 100000000 1000000000 10000000000
        do
            echo ${sb_bench_bin} ${bench} ${cores} ${size}
            ${sb_bench_bin} ${bench} ${cores} ${size} 2>&1 | tee ${log_file}
        done
    done
done