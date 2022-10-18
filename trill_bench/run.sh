#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/trill_yahoo.XXXXXX)
OUTFILE=trill_yahoo.csv

function run_trill {
	docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench $1 $2 $3 | grep "$STUB" | awk -F, '{ print $3 "," $4}' | tr -d ' '
}

# YSB benchmark
run_trill yahoo 320000000 1 | tee -a $TMPFILE
run_trill yahoo 160000000 2 | tee -a $TMPFILE
run_trill yahoo 80000000 4  | tee -a $TMPFILE
run_trill yahoo 40000000 8  | tee -a $TMPFILE
run_trill yahoo 20000000 16 | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
