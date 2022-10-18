#!/bin/bash -e

STUB="Throughput(M/s) 50"

TMPFILE=$(mktemp /tmp/lightsaber_yahoo.XXXXXX)
OUTFILE=lightsaber_yahoo.csv

function run_lightsaber {
	docker run --rm -it  lightsaber_image ./test/benchmarks/applications/yahoo_benchmark --circular-size 67108864 --slots 128 --batch-size 1048576 --bundle-size 2097152 --threads $1 | grep "$STUB" | awk -v threads=$1 '{ print threads "," $10/1000000 }' | tr -d ' '
}

# YSB benchmark
run_lightsaber 1 | tee -a $TMPFILE
run_lightsaber 3 | tee -a $TMPFILE
run_lightsaber 7 | tee -a $TMPFILE
run_lightsaber 15 | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
