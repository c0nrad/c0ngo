package main

import (
	"bytes"
	"fmt"

	"github.com/c0nrad/c0ngo/bson"
)

func main() {
	fmt.Println("Driver started!")

	c, err := NewConn("localhost")
	if err != nil {
		panic(err)
	}
	result, err := c.Find("lol", "demo", bson.Document{})

	r := OpRely{}
	err = r.Deserialize(bytes.NewReader(result))
	if err != nil {
		panic(err)
	}
	fmt.Printf("[+] Results: %+v\n", r)
}
