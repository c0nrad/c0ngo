package c0ngo

import (
	"bytes"
	"fmt"
	"net"

	"github.com/c0nrad/c0ngo/bson"
)

type Connection struct {
	conn net.Conn
}

func NewConn(host string) (*Connection, error) {
	c, err := net.Dial("tcp", host+":27017")
	if err != nil {
		return nil, err
	}

	return &Connection{c}, nil
}

func (c Connection) IsMaster() (*OpCommandReply, error) {
	q := OpCommand{}
	q.MsgHeader.MessageLength = 0
	q.MsgHeader.RequestId = 1337
	q.MsgHeader.ResponseTo = 0
	q.MsgHeader.OpCode = 2010

	q.CommandName = "isMaster"
	q.Database = "admin"

	data, err := c.Execute(&q)

	if err != nil {
		return nil, err
	}

	r := &OpCommandReply{}
	err = r.Deserialize(bytes.NewReader(data))
	return r, err
}

func (c Connection) Execute(q bson.Serializable) ([]byte, error) {
	data, err := q.Serialize()
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

func (c Connection) Find(db string, collection string, query bson.Document) (*OpReply, error) {
	q := OpQuery{}
	q.MsgHeader.MessageLength = 0
	q.MsgHeader.RequestId = 1337
	q.MsgHeader.ResponseTo = 0
	q.MsgHeader.OpCode = 2004

	q.Flags = 0
	q.FullCollectionName = bson.CString(db + "." + collection)
	q.NumberToSkip = 0
	q.NumberToReturn = 100
	q.Query = query

	data, err := c.Execute(&q)

	if err != nil {
		return nil, err
	}

	r := &OpReply{}
	err = r.Deserialize(bytes.NewReader(data))
	return r, err
}

func (c Connection) Insert(db, collection string, document bson.Document) (*OpReply, error) {
	q := OpInsert{}
	q.MsgHeader.MessageLength = 0
	q.MsgHeader.RequestId = 1337
	q.MsgHeader.ResponseTo = 0
	q.MsgHeader.OpCode = 2004

	q.Flags = 0
	q.FullCollectionName = bson.CString(db + "." + collection)
	q.Documents = []bson.Document{document}

	data, err := c.Execute(&q)

	if err != nil {
		return nil, err
	}

	r := &OpReply{}
	err = r.Deserialize(bytes.NewReader(data))
	return r, err
}
