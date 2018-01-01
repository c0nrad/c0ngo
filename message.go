package main

import (
	"bytes"

	"github.com/c0nrad/c0ngo/bson"
)

var OpCodeToIntMap = map[string]int{
	"OP_RELY": 1,
}

type MsgHeader struct {
	MessageLength bson.Int32
	RequestId     bson.Int32
	ResponseTo    bson.Int32
	OpCode        bson.Int32
}

func (h MsgHeader) Serialize() ([]byte, error) {
	return bson.SerializeArray([]bson.Serializable{&h.MessageLength, &h.RequestId, &h.ResponseTo, &h.OpCode})
}

func (h *MsgHeader) Deserialize(in *bytes.Reader) error {
	return bson.DeserializeArray([]bson.Serializable{&h.MessageLength, &h.RequestId, &h.ResponseTo, &h.OpCode}, in)
}

type OpQuery struct {
	MsgHeader
	Flags              bson.Int32
	FullCollectionName bson.CString
	NumberToSkip       bson.Int32
	NumberToReturn     bson.Int32
	Query              bson.Document
}

func (q OpQuery) Serialize() ([]byte, error) {
	return bson.SerializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query})
}

func (q *OpQuery) Deserialize(in *bytes.Reader) error {
	return bson.DeserializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query}, in)
}

type OpRely struct {
	MsgHeader
	ResponseFlags  bson.Int32
	CursorId       bson.Int64
	StartingFrom   bson.Int32
	NumberReturned bson.Int32
	Documents      []bson.Document
}

func (r OpRely) Serialize() ([]byte, error) {
	elements := []bson.Serializable{&r.MsgHeader, &r.ResponseFlags, &r.CursorId, &r.StartingFrom, &r.NumberReturned}
	for _, d := range r.Documents {
		elements = append(elements, &d)

	}

	return bson.SerializeArray(elements)
}

func (r *OpRely) Deserialize(in *bytes.Reader) error {
	elements := []bson.Serializable{&r.MsgHeader, &r.ResponseFlags, &r.CursorId, &r.StartingFrom, &r.NumberReturned}
	bson.DeserializeArray(elements, in)

	r.Documents = make([]bson.Document, r.NumberReturned)
	for i := range r.Documents {
		err := r.Documents[i].Deserialize(in)
		if err != nil {
			return err
		}
	}

	return nil
}
