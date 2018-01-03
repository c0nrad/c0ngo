package c0ngo

import (
	"bytes"
	"fmt"
	"net"

	"github.com/c0nrad/c0ngo/bson"
)

type Connection struct {
	conn net.Conn

	UseCompression bool
}

func NewConn(host string, compress bool) (*Connection, error) {
	c, err := net.Dial("tcp", host+":27017")
	if err != nil {
		return nil, err
	}

	return &Connection{c, compress}, nil
}

func (c Connection) IsMaster() (*OpCommandReply, error) {
	q := OpCommand{}
	q.MsgHeader.MessageLength = 0
	q.MsgHeader.RequestId = 1337
	q.MsgHeader.ResponseTo = 0
	q.MsgHeader.OpCode = 2010

	q.CommandName = "isMaster"
	q.Database = "admin"

	metadata := bson.Document{
		bson.NewInt32Element("isMaster", 1),
	}

	q.Metadata = metadata

	data, err := c.Write(&q, true)

	if err != nil {
		return nil, err
	}

	r := &OpCommandReply{}
	err = r.Deserialize(bytes.NewReader(data))
	return r, err
}

func (c Connection) Write(q bson.Serializable, isResponse bool) ([]byte, error) {
	data, err := q.Serialize()
	if err != nil {
		return nil, err
	}

	fmt.Printf("[+] Query: %+v\n", q)
	PrintHexArray(data)

	_, err = c.conn.Write(data)
	if err != nil {
		return nil, err
	}

	if !isResponse {
		return nil, nil
	}

	buffer := make([]byte, 4096)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer[0:n], nil
}

func PrintHexArray(in []byte) {
	for _, c := range in {
		fmt.Printf("%x ", c)
	}
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

	if c.UseCompression {
		message, err := CompressMessage(&q)
		if err != nil {
			return nil, err
		}

		fmt.Printf("[+] Writing CompressedMessage %+v\n", message)

		data, err := c.Write(message, true)

		if err != nil {
			return nil, err
		}

		compressed := &OpCompressed{}
		err = compressed.Deserialize(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}

		fmt.Printf("[+] Reading CompressedMessage %+v\n", message)

		return compressed.ToOpReply()
	} else {
		data, err := c.Write(&q, true)

		if err != nil {
			return nil, err
		}

		r := &OpReply{}
		err = r.Deserialize(bytes.NewReader(data))
		return r, err
	}

}

func (c Connection) Insert(db, collection string, document bson.Document) (*OpReply, error) {
	q := OpInsert{}
	q.MsgHeader.MessageLength = 0
	q.MsgHeader.RequestId = 1337
	q.MsgHeader.ResponseTo = 0
	q.MsgHeader.OpCode = 2002

	q.Flags = 0
	q.FullCollectionName = bson.CString(db + "." + collection)
	q.Documents = []bson.Document{document}

	data, err := c.Write(&q, false)

	if err != nil {
		return nil, err
	}

	r := &OpReply{}
	err = r.Deserialize(bytes.NewReader(data))
	return r, err
}
