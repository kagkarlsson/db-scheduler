#!/usr/bin/env bash


for vm in "34.76.128.224" "34.76.98.79" "34.77.115.108" "35.205.163.153"
do
  scp target/db-scheduler-benchmark-1.0-SNAPSHOT.jar "$vm:~/"
done



#scp target/db-scheduler-benchmark-1.0-SNAPSHOT.jar "$1:~/"



#ssh $1 -tt "PGHOST=$PGHOST PGPASSWORD=$PGPASSWORD java -jar db-scheduler-benchmark-1.0-SNAPSHOT.jar"
