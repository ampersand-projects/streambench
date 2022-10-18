#!/bin/bash -e

STUB="Throughput(M/s) 50"

TMPFILE=$(mktemp /tmp/lightsaber_yahoo.XXXXXX)
OUTFILE=lightsaber_yahoo.csv

# YSB benchmark
docker run --rm -it  lightsaber_image ./test/benchmarks/applications/yahoo_benchmark --circular-size 67108864 --slots 128 --batch-size 1048576 --bundle-size 2097152 --threads 1 | grep "$STUB" | awk '{ print 2 "," $10/1000000 }' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  lightsaber_image ./test/benchmarks/applications/yahoo_benchmark --circular-size 67108864 --slots 128 --batch-size 1048576 --bundle-size 2097152 --threads 3 | grep "$STUB" | awk '{ print 4 "," $10/1000000 }' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  lightsaber_image ./test/benchmarks/applications/yahoo_benchmark --circular-size 67108864 --slots 128 --batch-size 1048576 --bundle-size 2097152 --threads 7 | grep "$STUB" | awk '{ print 8 "," $10/1000000 }' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  lightsaber_image ./test/benchmarks/applications/yahoo_benchmark --circular-size 67108864 --slots 128 --batch-size 1048576 --bundle-size 2097152 --threads 15 | grep "$STUB" | awk '{ print 16 "," $10/1000000 }' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
