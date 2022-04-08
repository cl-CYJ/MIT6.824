package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


func max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}