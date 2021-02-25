#!/bin/zsh

rm res -rf
mkdir res


for i in `seq 1000`
do
    date +%F_%T
    go test &> res/all.$i
    echo "$i result:$?"
    if [ $? -ne 0 ]
    then
       echo "run failed."
       exit 1
    fi
done
