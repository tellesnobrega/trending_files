#!/bin/bash

STORAGE_FOLDER=$1
for i in {1..7} 
do
    for j in {0..3}
    do  
       if [ -f $STORAGE_FOLDER/worker-$j.$i.log ];
       then
           python latency_parser.py $STORAGE_FOLDER/worker-$j.$i.log $STORAGE_FOLDER/worker-trimmed-$j-$i.log
       else
           echo "hour;minute;second;latency" > $STORAGE_FOLDER/worker-trimmed-$j-$i.log
       fi  
    done
done

