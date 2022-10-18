#!/bin/bash -e

STUB="Throughput(M/s)"

function run_trill {
	docker run --rm -it  trill_image /root/trill_bench/bench/bin/Release/netcoreapp3.1/bench $1 $2 $3 | grep "$STUB"
}

# YSB benchmark
TMPFILE=$(mktemp /tmp/trill_yahoo.XXXXXX)
OUTFILE=trill_yahoo.csv

echo "Running YSB on Trill ..."

run_trill yahoo 320000000 1 | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill yahoo 160000000 2 | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill yahoo 80000000 4  | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill yahoo 40000000 8  | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill yahoo 20000000 16 | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE


# Real-world benchmark
TMPFILE=$(mktemp /tmp/trill_real.XXXXXX)
OUTFILE=trill_real.csv

echo "Running real-world benchmark on Trill"
run_trill normalize 10000000 8   | awk -F, '{ print "Normalize," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill fillmean 10000000 8    | awk -F, '{ print "Impute," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill resample 10000000 8    | awk -F, '{ print "Resample," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill algotrading 10000000 8 | awk -F, '{ print "Trading," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill rsi 10000000 8         | awk -F, '{ print "RSI," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill pantom 10000000 8      | awk -F, '{ print "PanTom," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill kurtosis 10000000 8    | awk -F, '{ print "Vibration," $4}' | tr -d ' ' | tee -a $TMPFILE
run_trill largeqty 10000000 8    | awk -F, '{ print "FraudDet," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
