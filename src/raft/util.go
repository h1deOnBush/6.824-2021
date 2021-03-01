package raft

import (
	"log"
)

// Debugging
const flag = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if flag {
		log.Printf(format, a...)
	}
	return
}
