package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan struct{})
	go func() {
		time.Sleep(4*time.Second)
		ch <- struct{}{}
	}()
	select {
	case <- ch:
		fmt.Println(111)
	case <- time.After(3*time.Second):
		fmt.Println(222)
	}
}