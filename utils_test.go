package raftbadger

import (
	"math/rand"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	ralog := &raft.Log{
		Term:  rand.Uint64(),
		Index: 11,
	}
	buf, err := encodeMsgpack(ralog)
	assert.Nil(t, err)

	tolog := &raft.Log{}
	err = decodeMsgpack(buf.Bytes(), tolog)
	assert.Nil(t, err)

	assert.EqualValues(t, ralog, tolog)
}

func TestConverts(t *testing.T) {
	var num uint64 = 512
	bs := uint64ToBytes(num)
	to := bytesToUint64(bs)
	assert.EqualValues(t, 512, to)
}
