#!/bin/zsh
for i in `seq 100`
do
    go test -run TestConcurrent2 -race &> log/log
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    else
        echo "$i success"
    fi
done
for i in `seq 100`
do
    go test -run TestStaticShards -race &> log/log
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    else
        echo "$i success"
    fi
done

for i in `seq 100`
do
    go test -run TestJoinLeave -race &> log/log
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    else
        echo "$i success"
    fi
done

