#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/streambox_yahoo.XXXXXX)
OUTFILE=streambox_yahoo.csv

function run_streambox {
	docker run --rm -it  streambox_image ./main $1 $2 $3 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' '
}

# YSB benchmark
echo "Running YSB on StreamBox ..."
run_streambox yahoo 1 320000000 | tee -a $TMPFILE
run_streambox yahoo 2 160000000 | tee -a $TMPFILE
run_streambox yahoo 4 80000000 | tee -a $TMPFILE
run_streambox yahoo 8 40000000 | tee -a $TMPFILE
run_streambox yahoo 16 20000000 | tee -a $TMPFILE

cp $TMPFILE $OUTFILE

