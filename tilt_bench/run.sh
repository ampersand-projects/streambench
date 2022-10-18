#!/bin/bash -e

STUB="Throughput(M/s)"

function run_tilt {
	docker run --rm -it  tilt_image ./main $1 $2 $3 | grep $STUB
}

# YSB benchmark
TMPFILE=$(mktemp /tmp/tilt_yahoo.XXXXXX)
OUTFILE=tilt_yahoo.csv

echo "Running YSB on TiLT ..."
run_tilt yahoo 320000000 1 | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt yahoo 160000000 2 | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt yahoo 80000000 4  | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt yahoo 40000000 8  | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt yahoo 20000000 16 | awk -F, '{ print $3 "," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE


# Real-world benchmark
TMPFILE=$(mktemp /tmp/tilt_real.XXXXXX)
OUTFILE=tilt_real.csv

THREADS=8

echo "Running real-world benchmark on TiLT ..."
run_tilt normalize 10000000 $THREADS   | awk -F, '{ print "Normalize," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt fillmean 10000000 $THREADS    | awk -F, '{ print "Impute," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt resample 10000000 $THREADS    | awk -F, '{ print "Resample," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt algotrading 10000000 $THREADS | awk -F, '{ print "Trading," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt rsi 10000000 $THREADS         | awk -F, '{ print "RSI," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt pantom 10000000 $THREADS      | awk -F, '{ print "PanTom," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt kurtosis 10000000 $THREADS    | awk -F, '{ print "Vibration ," $4}' | tr -d ' ' | tee -a $TMPFILE
run_tilt largeqty 10000000 $THREADS    | awk -F, '{ print "FraudDet," $4}' | tr -d ' ' | tee -a $TMPFILE

cp $TMPFILE $OUTFILE
