package main

import (
	"fmt"
	"runtime"
)

func sing() {
	for i := 0; i < 1000; i ++ {
		fmt.Println("唱歌ing")
		runtime.Gosched()
	}
}

func dance() {
	for i := 0; i < 100; i ++ {
		fmt.Println("跳舞ing")
		runtime.Gosched()
	}
}

func main() {
	go sing()
	go dance()
	for {
		;
	}

}