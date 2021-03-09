#!/bin/zsh
for i in `seq 100`
do
    echo "$i"
    ls
    if [ $? -ne 0 ]
    then
       echo "run failed."
       exit 1;
    fi
done