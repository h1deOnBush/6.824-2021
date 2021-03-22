#!/bin/zsh

for i in `seq 1000`
do
    go test -race &> log/log
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    else
        echo "$i success"
    fi
done