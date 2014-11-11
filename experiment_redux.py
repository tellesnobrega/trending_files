#!/usr/bin/python

import sys, getopt
import subprocess

path = sys.argv[1]
nimbus = sys.argv[2]
begin = sys.argv[3]
end = sys.argv[4]
jar_file=sys.argv[5]

base_value = 420
values = [16800, 33600, 67200]

for value in values:
    if(value < base_value):
        num_spouts=1
    else:
        num_spouts=(value/base_value)

    storage_path=path+"/%s" % str(value)
    process = subprocess.Popen(["./run_topology.sh 41 500 %s %s ~/Downloads/telles.pem %s %s %s %s" % (str(num_spouts), storage_path, nimbus, str(begin), str(end), jar_file)], shell=True)

    process.wait()
