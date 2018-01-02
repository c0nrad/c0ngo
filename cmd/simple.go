package main

import (
	"fmt"

	"github.com/c0nrad/c0ngo"
	"github.com/c0nrad/c0ngo/bson"
)

func main() {
	fmt.Println("Driver started!")

	c, err := c0ngo.NewConn("localhost")
	if err != nil {
		panic(err)
	}

	data := bson.CString("world")
	id := bson.ObjectId("abc123abc123")
	doc := bson.Document{
		bson.Element{EName: "hello", Data: &data},
		bson.Element{EName: "_id", Data: &id},
	}

	result, err := c.Insert("lol", "demo", doc)
	fmt.Println(result, err)

	result, err = c.Find("lol", "demo", doc)
	fmt.Println(result, err)

}
