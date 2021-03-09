#!/bin/zsh

rm log -rf
mkdir log


for i in `seq 100`
do
    go test -run TestSnapshotInstallUnCrash2D &> log/log
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    fi
    echo "$i success"
done
