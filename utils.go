package raftbadger

import (
	"bytes"
	"encoding/binary"

	"github.com/hashicorp/go-msgpack/codec"
)

// decodeMsgpack reverses the encode operation on a byte slice input
func decodeMsgpack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// encodeMsgpack writes an encoded object to a new bytes buffer
func encodeMsgpack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// bytesToUint64 converts bytes to an uint64
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// uint64ToBytes converts a uint64 to []byte
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
