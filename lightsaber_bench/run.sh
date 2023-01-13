
#!/bin/bash

sb_bench_bin="./build/lightsaber"
log_file="./result.txt"

for bench in "select" "where" "aggregate" "yahoo"
do
    for cores in 1 3 7 15 31
    do
        echo ${sb_bench_bin} ${bench} 100000 10000 ${cores} ${size}
        ${sb_bench_bin} ${bench} 100000 10000 ${cores} ${size}
    done
done