#!/bin/zsh

for i in `seq 100`
do
    go test -run TestSnapshotRecoverManyClients3B &> log/log1
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    fi
    echo "$i success"
done