package main

import (
	"fmt"
	"net"

	"github.com/c0nrad/c0ngo/bson"
)

type Conn struct {
	conn net.Conn
}

func NewConn(host string) (*Conn, error) {
	c, err := net.Dial("tcp", host+":27017")
	if err != nil {
		return nil, err
	}

	return &Conn{c}, nil
}

func (c Conn) Find(db string, collection string, query bson.Document) ([]byte, error) {
	q := OpQuery{}
	q.MsgHeader.MessageLength = 1111
	q.MsgHeader.RequestId = 1
	q.MsgHeader.ResponseTo = 0
	q.MsgHeader.OpCode = 2004

	q.Flags = 0
	q.FullCollectionName = bson.CString(db + "." + collection)
	q.NumberToSkip = 0
	q.NumberToReturn = 100

	data, err := q.Serialize()
	if err != nil {
		return nil, err
	}

	q.MsgHeader.MessageLength = bson.Int32(len(data))

	data, err = q.Serialize()
	if err != nil {
		return nil, err
	}

	fmt.Printf("[+] Query: %+v\n", q)

	_, err = c.conn.Write(data)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 4096)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer[0:n], nil
}
