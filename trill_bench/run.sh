#!/bin/bash -e

STUB="Throughput(M/s)"

TMPFILE=$(mktemp /tmp/trill_yahoo.XXXXXX)
OUTFILE=trill_yahoo.csv

# YSB benchmark
docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench yahoo 320000000 1 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench yahoo 160000000 2 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench yahoo 80000000 4 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench yahoo 40000000 8 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench yahoo 20000000 16 | grep $STUB | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
