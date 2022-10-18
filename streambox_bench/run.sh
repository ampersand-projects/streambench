#!/bin/bash -e

docker run --rm -it  streambox_image ./main yahoo 1 320000000

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/streambox_yahoo.XXXXXX)
OUTFILE=streambox_yahoo.csv

# YSB benchmark
docker run --rm -it  streambox_image ./main yahoo 1 320000000 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  streambox_image ./main yahoo 2 160000000 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  streambox_image ./main yahoo 4 80000000 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  streambox_image ./main yahoo 8 40000000 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  streambox_image ./main yahoo 16 20000000 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE

