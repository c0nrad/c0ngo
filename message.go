package c0ngo

import (
	"bytes"
	"errors"

	"github.com/c0nrad/c0ngo/bson"
	"github.com/golang/snappy"
)

type WireMessage interface {
	bson.Serializable

	Header() MsgHeader
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

func (q OpQuery) Header() MsgHeader {
	return q.MsgHeader
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

func (q OpQuery) OpCode() int {
	return 2004
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

func CompressMessage(m WireMessage) (*OpCompressed, error) {
	op := &OpCompressed{}

	op.OriginalOpCode = m.Header().OpCode
	op.MsgHeader.OpCode = 2012
	op.MsgHeader.RequestId = 0xdead
	op.MsgHeader.ResponseTo = 0

	originalMessage, err := m.Serialize()
	if err != nil {
		return nil, err
	}

	op.UncompressedSize = bson.Int32(len(originalMessage)) - 16     // don't include msgheader
	op.CompressorId = 1                                             // snappy
	op.CompressedMessage = snappy.Encode(nil, originalMessage[16:]) // don't include msgheader

	//Update message length
	elements := []bson.Serializable{&op.MsgHeader, &op.OriginalOpCode, &op.UncompressedSize, &op.CompressorId}
	data, err := bson.SerializeArray(elements)
	if err != nil {
		return nil, err
	}
	op.MsgHeader.MessageLength = bson.Int32(len(data) + len(op.CompressedMessage))

	return op, nil
}

func (op OpCompressed) Serialize() ([]byte, error) {
	elements := []bson.Serializable{&op.MsgHeader, &op.OriginalOpCode, &op.UncompressedSize, &op.CompressorId}

	data, err := bson.SerializeArray(elements)
	if err != nil {
		return nil, err
	}

	op.MsgHeader.MessageLength = bson.Int32(len(data) + len(op.CompressedMessage))

	out, err := bson.SerializeArray(elements)
	if err != nil {
		return nil, err
	}

	out = append(out, op.CompressedMessage...)
	return out, nil
}

func (op *OpCompressed) Deserialize(in *bytes.Reader) error {
	elements := []bson.Serializable{&op.MsgHeader, &op.OriginalOpCode, &op.UncompressedSize, &op.CompressorId}
	err := bson.DeserializeArray(elements, in)

	if err != nil {
		return err
	}

	// the rest of the message is the compressedLength
	compressedLength := int(op.MsgHeader.MessageLength) - (int(in.Size()) - in.Len())
	op.CompressedMessage = make([]byte, compressedLength)

	n, err := in.Read(op.CompressedMessage)
	if err != nil {
		return err
	}

	if n != compressedLength {
		return errors.New("didn't read correct number of bytes")
	}

	return nil
}

func (op OpCompressed) ToOpReply() (*OpReply, error) {

	// don't pass by reference :)
	op.MsgHeader.OpCode = op.OriginalOpCode

	rawReplyHeader, err := op.MsgHeader.Serialize()
	if err != nil {
		return nil, err
	}

	rawReplyBody, err := snappy.Decode(nil, op.CompressedMessage)
	if err != nil {
		return nil, err
	}

	rawReply := append(rawReplyHeader, rawReplyBody...)

	out := OpReply{}
	err = out.Deserialize(bytes.NewReader(rawReply))
	return &out, err
}

// func (c OpCompressed) Serialize() ([]byte, error) {
// 	data, err := bson.SerializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query})
// }

// func (c *OpCompressed) Deserialize(in *bytes.Reader) error {
// 	return bson.DeserializeArray([]bson.Serializable{&q.MsgHeader, &q.Flags, &q.FullCollectionName, &q.NumberToSkip, &q.NumberToReturn, &q.Query}, in)
// }
