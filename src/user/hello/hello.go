package main

import (
	"fmt"

	//"user/newmath"
)

func main() {
	qwe := []string{"qa", "wa", "ra"}
//	fmt.Printf("Hello, world.  Sqrt(2) = %v\n", newmath.Sqrt(2))
	for column := range qwe {
		fmt.Println(qwe[column])
	}
}
