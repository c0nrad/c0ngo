package main

import (
	"fmt"

	"github.com/c0nrad/c0ngo"
)

func main() {
	fmt.Println("Driver started!")

	c, err := c0ngo.NewConn("localhost", false)
	if err != nil {
		panic(err)
	}

	result, err := c.IsMaster()

	fmt.Printf("[+] Results: %+v\n", result)

}
