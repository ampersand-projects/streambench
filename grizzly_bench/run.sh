#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/grizzly_yahoo.XXXXXX)
OUTFILE=grizzly_yahoo.csv

function run_grizzly {
	docker run --rm -it  grizzly_image ./grizzly_bench $1 $2 $3 ../data-generator/yahoo_data.bin | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' '
}

# YSB benchmark
echo "Running YSB on Grizzly ..."
run_grizzly yahoo 20000000 1  | tee -a $TMPFILE
run_grizzly yahoo 20000000 2  | tee -a $TMPFILE
run_grizzly yahoo 20000000 4  | tee -a $TMPFILE
run_grizzly yahoo 20000000 8  | tee -a $TMPFILE
run_grizzly yahoo 20000000 16 | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
