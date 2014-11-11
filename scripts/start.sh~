#!/bin/sh

cd ..
ant -buildfile build.xml 
storm jar target/alarm-storm.jar alarm.Main
sleep 600
storm kill alarm-storm

exit 0
