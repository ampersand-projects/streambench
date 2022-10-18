#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/tilt_yahoo.XXXXXX)
OUTFILE=tilt_yahoo.csv

function run_tilt {
	docker run --rm -it  tilt_image ./main $1 $2 $3 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
}

# YSB benchmark
run_tilt yahoo 320000000 1 | tee -a $TMPFILE
run_tilt yahoo 160000000 2 | tee -a $TMPFILE
run_tilt yahoo 80000000 4  | tee -a $TMPFILE
run_tilt yahoo 40000000 8  | tee -a $TMPFILE
run_tilt yahoo 20000000 16 | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
