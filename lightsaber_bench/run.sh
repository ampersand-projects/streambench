
#!/bin/bash

sb_bench_bin="./build/lightsaber"
log_file="./result.txt"

for bench in "select" "where" "aggregate" "yahoo"
do
    for cores in 1 3 7 15 31
    do
        echo ${sb_bench_bin} ${bench} 10000 1000000 ${cores} ${size}
        ${sb_bench_bin} ${bench} 10000 1000000 ${cores} ${size}
    done
done


std::string testcase = (argc > 1) ? argv[1] : "select";
int64_t size = (argc > 2) ? atoi(argv[2]) : 10000; // In # of events
int64_t runs = (argc > 3) ? atoi(argv[3]) : 1000000;
int64_t batch_size = (argc > 4) ? atoi(argv[4]) : 1;
int64_t slots = (argc > 5) ? atoi(argv[5]) : 1;
int64_t partial_windows = (argc > 6) ? atoi(argv[6]) : 1;
int64_t hash_table_size = (argc > 7) ? atoi(argv[7]) : 1;
int64_t cores = (argc > 8) ? atoi(argv[8]) : 1;


./lightsaber yahoo 100000 10000 1048576 512 512 1024 15