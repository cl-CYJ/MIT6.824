package main

import (
	"fmt"
	"strings"
)

func main() {
	fmt.Printf("Fields are: %q\n", strings.Fields("  foo bar  baz   "))
}