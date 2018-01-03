package main

import (
	"fmt"

	"github.com/c0nrad/c0ngo"
	"github.com/c0nrad/c0ngo/bson"
)

func main() {
	fmt.Println("Driver started!")

	c, err := c0ngo.NewConn("localhost", false)
	if err != nil {
		panic(err)
	}

	doc := bson.Document{
		bson.NewStringElement("hello", "world"),
	}

	data, err := doc.Serialize()
	fmt.Println(data, err)

	result, err := c.Insert("lol", "demo", doc)
	fmt.Println(result, err)

}
