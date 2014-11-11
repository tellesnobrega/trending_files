import re
import math
import sys

def parse_line(base_time, latency):
    hour = base_time[0]
    minutes = base_time[1]
    seconds = base_time[2]

    line = hour +";"+minutes+";"+seconds+";"+ str(latency)
    return line

def _write(f, line):
    f.write(line+"\n")

def _get_99_percentil(data):
    size = len(data)
    data.sort()

    percentil_99 = float(size * 99.0 / 100.0)
    top = int(math.ceil(percentil_99))
    bottom = int(math.floor(percentil_99))
    value = (data[top-1] + data[bottom-1]) / 2
    print(value)
    return value 

def main(args):
  latency = []
  file_name = str(args[0])
  out_file = str(args[1])
  f = open(file_name, 'r')
  out = open(out_file, 'w')
  _write(out, "hour;minute;second;latency")
  base_time="-1"
  for line in f:
      if 'AckSent' in line:
          line_split = line.strip().split(";")
          base_time = line_split[0].split(":")
          latency = line_split[2]
          to_write=parse_line(base_time, latency) 
          _write(out, to_write)

if __name__ == "__main__":
  main(sys.argv[1:])
