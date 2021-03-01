#!/bin/zsh

rm res -rf
mkdir res


for i in `seq 100`
do
    go test -run 2C -race &> res/res.$i
    echo "$i result:$?"
done
