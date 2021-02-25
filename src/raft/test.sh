#!/bin/zsh

rm res -rf
mkdir res


for i in `seq 100`
do
    date +%F_%T
    go test -run TestFigure8Unreliable2C -race &> res/f8ur
    echo "$i result:$?"
    if [ $? -ne 0 ]
    then
       echo "run failed."
       exit 1
    fi
done
