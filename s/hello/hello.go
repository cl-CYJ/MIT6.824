package main

import (
	"fmt"
	// "log"
	"math/rand"
	// "os"
	"time"
)

func main() {
	fmt.Println(time.Now())
	a := time.Now()
	t1 := time.NewTimer(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
	fmt.Println(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
	fmt.Println(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
	fmt.Println(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
	fmt.Println(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
	fmt.Println(time.Duration(150 + rand.Intn(150)) * time.Millisecond)

	select{
	case <-t1.C:
		fmt.Println(time.Now().Sub(a))
	}
}
// [[0 0 0 0] [0 0 0 0] [0 0 0 0]]
// [[0 0 0 0] [0 0 0 0] [0 0 0 0]]


