package main

import (
	"fmt"

	"github.com/c0nrad/c0ngo"
	"github.com/c0nrad/c0ngo/bson"
)

func main() {
	c, err := c0ngo.NewConn("localhost", true)
	if err != nil {
		panic(err)
	}

	result, err := c.Find("lol", "demo", bson.Document{})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Find %+v\n", result)
}
