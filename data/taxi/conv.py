import time
import datetime
import csv

def conv(file, fn):
    with open(file) as f:
        reader = csv.reader(f)
        for line in reader:
            print(fn(line))

def trip(line):
    st = line[0]
    et = line[1]
    d = line[2]
    st_time = int(time.mktime(time.strptime(st, "%Y-%m-%d %H:%M:%S")))
    et_time = int(time.mktime(time.strptime(et, "%Y-%m-%d %H:%M:%S")))
    return ",".join([str(st_time), str(et_time), d])

def fare(line):
    st = line[0]
    d = line[1]
    st_time = int(time.mktime(time.strptime(st, "%Y-%m-%d %H:%M:%S")))
    return ",".join([str(st_time), d])

#conv('trip.csv', trip)
conv('fare.csv', fare)

