for i in `seq 100`
do
    go test -run 3B &> log/log
    if [ $? -ne 0 ]
    then
        echo "$i  fail"
        exit 1
    fi
    echo "$i success"
done