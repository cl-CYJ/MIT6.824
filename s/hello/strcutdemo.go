package main

import "fmt"
import "os"

func test(f func(s string)) {
	f("hello world")
}

func main() {
	test(func(s string) {
		fmt.Println(s)
	})
	var a [3]int
	a = [3]int{1, 2, 3}
	var b *[3]int
	fmt.Printf("类型为%T\n", &a)
	fmt.Printf("类型为%T\n", &a[0])
	fmt.Printf("类型为%T\n", b)
	b = &a
	b[1] = 5
	fmt.Println(a)
	fmt.Println(os.Getuid())
	// fmt.Printf("类型为%T\n", b)

}