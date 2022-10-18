#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/tilt_yahoo.XXXXXX)
OUTFILE=tilt_yahoo.csv

# YSB benchmark
docker run --rm -it  tilt_image ./main yahoo 320000000 1 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  tilt_image ./main yahoo 160000000 2 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  tilt_image ./main yahoo 80000000 4 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  tilt_image ./main yahoo 40000000 8 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  tilt_image ./main yahoo 20000000 16 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
