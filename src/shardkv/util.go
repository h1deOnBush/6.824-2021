package shardkv


import (
	"log"
)

// Debugging
const flag = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if flag {
		log.Printf(format, a...)
	}
	return
}