package main

import (
	// "fmt",
	"fmt"
	"io/ioutil"
	"os"
)

func main() {
	dir, _ := os.Getwd();
	fmt.Println(dir)
	temp, _ := ioutil.TempFile(dir, "hello-temp-*");
	temp.Write([]byte("123456"));
	pos, _ := temp.Seek(0, os.SEEK_CUR)
	fmt.Println("写后seek位置: ", pos)
	data, _ := ioutil.ReadFile(temp.Name())
	pos, _ = temp.Seek(0, os.SEEK_CUR)
	fmt.Println("读后seek位置: ", pos, " ", string(data))
	temp.Write([]byte("ooooooo11111"));
	pos, _ =  temp.Seek(0, os.SEEK_CUR) 
	fmt.Println("写后seek位置: ", pos)
	data, _ = ioutil.ReadFile(temp.Name())
	pos, _ =  temp.Seek(0, os.SEEK_CUR) 
	fmt.Println("读后seek位置: ", pos, string(data))
	fmt.Println("结束")
}