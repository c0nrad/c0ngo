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
		bson.NewDoubleElement("hello", 1),
	}

	result, err := c.Find("lol", "demo", doc)
	fmt.Println(result, err)

}

func PrintHexArray(in []byte) {
	for _, c := range in {
		fmt.Print("%x ", c)
	}
}
