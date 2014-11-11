#!/bin/bash

PEM_FILE=$1

for i in {1..7}
do
    ssh -i $PEM_FILE ubuntu@telles-storm-slave$i 'sudo rm -rf /usr/local/storm/logs/worker-6700.log'
    ssh -i $PEM_FILE ubuntu@telles-storm-slave$i 'sudo rm -rf /usr/local/storm/logs/worker-6701.log'
    ssh -i $PEM_FILE ubuntu@telles-storm-slave$i 'sudo rm -rf /usr/local/storm/logs/worker-6702.log'
    ssh -i $PEM_FILE ubuntu@telles-storm-slave$i 'sudo rm -rf /usr/local/storm/logs/worker-6703.log'
done
