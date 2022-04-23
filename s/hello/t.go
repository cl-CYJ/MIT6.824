package main

import (
	"fmt"
	// "fmt"
	// "io/ioutil"
	"os"
	// "os"
)

func main() {
	err1 := os.Rename("ttttt.txt", "tmp.txt")
	if err1 != nil {
		fmt.Println("第一步命名错误")
	}
	err2 := os.Rename("test.txt", "tmp.txt")
	if err2 != nil {
		fmt.Println("第二步命名错误")
		fmt.Print(err2)
	}
	// data, _ := ioutil.ReadFile("")
	// pos, _ = temp.Seek(0, os.SEEK_CUR)
	// fmt.Println(string(data))
}