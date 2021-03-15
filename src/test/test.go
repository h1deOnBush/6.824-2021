package main

import (
	"fmt"
)

func main() {
	var a []int
	for v := range a {
		fmt.Println(v)
	}
}