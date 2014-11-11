#!/bin/bash

STORAGE_FOLDER=$1
for i in {1..7} 
do
   if [ -f $STORAGE_FOLDER/worker-3.$i.log ];
   then
       python simple_copy.py $STORAGE_FOLDER/worker-3.$i.log $STORAGE_FOLDER/worker-copy-3-$i.log
   else
       echo "hour;minute;second;latency" > $STORAGE_FOLDER/worker-trimmed-3-$i.log
   fi  
done

