#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/grizzly_yahoo.XXXXXX)
OUTFILE=grizzly_yahoo.csv

# YSB benchmark
docker run --rm -it  grizzly_image ./grizzly_bench yahoo 20000000 1 ../data-generator/yahoo_data.bin | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  grizzly_image ./grizzly_bench yahoo 20000000 2 ../data-generator/yahoo_data.bin | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  grizzly_image ./grizzly_bench yahoo 20000000 4 ../data-generator/yahoo_data.bin | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  grizzly_image ./grizzly_bench yahoo 20000000 8 ../data-generator/yahoo_data.bin | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  grizzly_image ./grizzly_bench yahoo 20000000 16 ../data-generator/yahoo_data.bin | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
