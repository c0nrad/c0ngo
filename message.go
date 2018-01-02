package c0ngo

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
	data, err := bson.SerializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query})
	if err != nil {
		return nil, err
	}

	q.MsgHeader.MessageLength = bson.Int32(len(data))

	return bson.SerializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query})
}

func (q *OpQuery) Deserialize(in *bytes.Reader) error {
	return bson.DeserializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query}, in)
}

type OpInsert struct {
	MsgHeader
	Flags              bson.Int32
	FullCollectionName bson.CString
	Documents          []bson.Document
}

func (op OpInsert) Serialize() ([]byte, error) {
	elements := []bson.Serializable{&op.MsgHeader, &op.Flags, &op.FullCollectionName}
	for _, d := range op.Documents {
		elements = append(elements, &d)
	}

	data, err := bson.SerializeArray(elements)
	if err != nil {
		return nil, err
	}

	op.MsgHeader.MessageLength = bson.Int32(len(data))

	return bson.SerializeArray(elements)
}

type OpReply struct {
	MsgHeader
	ResponseFlags  bson.Int32
	CursorId       bson.Int64
	StartingFrom   bson.Int32
	NumberReturned bson.Int32
	Documents      []bson.Document
}

func (r OpReply) Serialize() ([]byte, error) {
	elements := []bson.Serializable{&r.MsgHeader, &r.ResponseFlags, &r.CursorId, &r.StartingFrom, &r.NumberReturned}
	for _, d := range r.Documents {
		elements = append(elements, &d)

	}

	data, err := bson.SerializeArray(elements)
	if err != nil {
		return nil, err
	}

	r.MsgHeader.MessageLength = bson.Int32(len(data))
	return bson.SerializeArray(elements)

}

func (r *OpReply) Deserialize(in *bytes.Reader) error {
	elements := []bson.Serializable{&r.MsgHeader, &r.ResponseFlags, &r.CursorId, &r.StartingFrom, &r.NumberReturned}
	err := bson.DeserializeArray(elements, in)
	if err != nil {
		return err
	}

	r.Documents = make([]bson.Document, r.NumberReturned)
	for i := range r.Documents {
		err := r.Documents[i].Deserialize(in)
		if err != nil {
			return err
		}
	}

	return nil
}

type OpCommand struct {
	MsgHeader
	Database    bson.CString
	CommandName bson.CString
	Metadata    bson.Document
	CommandArgs bson.Document
	InputDocs   []bson.Document
}

func (op OpCommand) Serialize() ([]byte, error) {
	elements := []bson.Serializable{&op.MsgHeader, &op.Database, &op.CommandName, &op.Metadata, &op.CommandArgs}
	for _, d := range op.InputDocs {
		elements = append(elements, &d)

	}

	data, err := bson.SerializeArray(elements)
	if err != nil {
		return nil, err
	}

	op.MsgHeader.MessageLength = bson.Int32(len(data))
	return bson.SerializeArray(elements)

}

func (op *OpCommand) Deserialize(in *bytes.Reader) error {
	elements := []bson.Serializable{&op.MsgHeader, &op.Database, &op.CommandName, &op.Metadata, &op.CommandArgs}
	bson.DeserializeArray(elements, in)

	for in.Len() != 0 {
		d := new(bson.Document)
		err := d.Deserialize(in)
		if err != nil {
			return err
		}

		op.InputDocs = append(op.InputDocs, *d)
	}

	return nil
}

type OpCommandReply struct {
	MsgHeader
	Metadata     bson.Document
	CommandReply bson.Document
	OutputDocs   []bson.Document
}

func (op *OpCommandReply) Deserialize(in *bytes.Reader) error {
	elements := []bson.Serializable{&op.MsgHeader, &op.Metadata, &op.CommandReply}
	return bson.DeserializeArray(elements, in)
}

type OpCompressed struct {
	MsgHeader
	OriginalOpCode    bson.Int32
	UncompressedSize  bson.Int32
	CompressorId      bson.Byte
	CompressedMessage []byte
}

// func (c OpCompressed) Serialize() ([]byte, error) {
// 	data, err := bson.SerializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query})
// }

// func (c *OpCompressed) Deserialize(in *bytes.Reader) error {
// 	return bson.DeserializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query}, in)
// }
