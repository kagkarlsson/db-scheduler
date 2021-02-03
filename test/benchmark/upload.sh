#!/usr/bin/env bash


for vm in "34.77.123.54" "35.241.217.7" "34.78.158.78" "104.199.29.131"
do
  scp target/db-scheduler-benchmark-1.0-SNAPSHOT.jar "$vm:~/"
done



#scp target/db-scheduler-benchmark-1.0-SNAPSHOT.jar "$1:~/"



#ssh $1 -tt "PGHOST=$PGHOST PGPASSWORD=$PGPASSWORD java -jar db-scheduler-benchmark-1.0-SNAPSHOT.jar"
