#!/usr/bin/python

import sys, getopt
import subprocess

path = sys.argv[1]
num_bolts = sys.argv[2]
latency = sys.argv[3]

base_value = 420
values = [420, 1260, 2100, 4200, 8400, 16800, 25200, 33600]
values = [420]
for value in values:
    if(value < base_value):
        num_spouts=1
    else:
        num_spouts=(value/base_value)

    for i in range(10):
        storage_path=path+"/%s/%s" % (str(value), str(i))
        process = subprocess.Popen(["./run_topology.sh 4 500 %s %s %s %s ~/Downloads/telles.pem" % (str(num_spouts), str(num_bolts), str(latency),  storage_path)], shell=True)
        process.wait()
