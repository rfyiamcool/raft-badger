package raftbadger

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func buildTestConfig() Config {
	cfg := Config{
		DataPath:      "/tmp/raft",
		Compression:   "zstd",
		DisableLogger: true,
	}

	return cfg
}

func BenchmarkSet(b *testing.B) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	defer store.Close()

	assert.Nil(b, err)
	for n := 0; n < b.N; n++ {
		store.Set(uint64ToBytes(uint64(n)), []byte("val"))
	}
}

func BenchmarkGet(b *testing.B) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	defer store.Close()

	assert.Nil(b, err)
	for n := 0; n < b.N; n++ {
		store.Set(uint64ToBytes(uint64(n)), []byte("xiaorui.cc"))
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		store.Get(uint64ToBytes(uint64(n)))
	}
}

func BenchmarkStoreLogs(b *testing.B) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	defer store.Close()

	assert.Nil(b, err)
	for n := 0; n < b.N; n++ {
		store.StoreLogs([]*raft.Log{
			{
				Index: uint64(n),
				Term:  uint64(n),
			},
		})
	}
}

func BenchmarkGetLog(b *testing.B) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	defer store.Close()

	assert.Nil(b, err)
	for n := 0; n < b.N; n++ {
		store.StoreLogs([]*raft.Log{
			{
				Index: uint64(n),
				Term:  uint64(n),
			},
		})
	}

	b.ResetTimer()

	ralog := new(raft.Log)
	for n := 0; n < b.N; n++ {
		store.GetLog(uint64(n), ralog)
	}
}

func TestFirstLastIndex(t *testing.T) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	defer store.Close()

	assert.Nil(t, err)

	var (
		count uint64 = 1000
		start uint64 = 10
	)
	for i := start; i < count; i++ {
		err := store.StoreLogs([]*raft.Log{
			{
				Index: uint64(i),
				Term:  uint64(i),
			},
		})
		assert.Nil(t, err)

		first, err := store.FirstIndex()
		assert.Equal(t, uint64(10), first)

		last, err := store.LastIndex()
		assert.Equal(t, uint64(i), last)
	}
}

func TestGetLog(t *testing.T) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	defer store.Close()

	assert.Nil(t, err)

	var (
		count uint64 = 1000
		start uint64 = 10
	)
	for i := start; i < count; i++ {
		err := store.StoreLogs([]*raft.Log{
			{
				Index: uint64(i),
				Term:  uint64(i),
			},
		})
		assert.Nil(t, err)

		ralog := new(raft.Log)
		err = store.GetLog(uint64(i), ralog)
		assert.Nil(t, err)
		assert.Equal(t, uint64(i), ralog.Index)
	}
}

func TestPipieline(t *testing.T) {
	cfg := buildTestConfig()
	defer os.RemoveAll(cfg.DataPath)

	store, err := New(cfg, nil)
	assert.Nil(t, err)

	// test dropall api
	err = store.DropAll()
	assert.Nil(t, err)

	// test set api
	err = store.Set([]byte("blog"), []byte("xiaorui.cc"))
	assert.Nil(t, err)

	// test get api
	value, err := store.Get([]byte("blog"))
	assert.Nil(t, err)
	assert.EqualValues(t, value, "xiaorui.cc")

	// test SetUint64 api
	err = store.SetUint64([]byte("index"), 111)
	assert.Nil(t, err)

	// test GetUint64 api
	index, err := store.GetUint64([]byte("index"))
	assert.EqualValues(t, 111, index)
	assert.Nil(t, err)

	var logs []*raft.Log
	for i := 0; i < 20; i++ {
		logs = append(logs, &raft.Log{
			Index: uint64(i),
			Term:  uint64(i),
			Type:  0,
			Data:  []byte("a"),
		})
	}

	// test StoreLogs api
	err = store.StoreLogs(logs)
	assert.Nil(t, err)

	// test FirstIndex api
	index, err = store.FirstIndex()
	assert.Nil(t, err)
	assert.EqualValues(t, 0, index)

	// test LastIndex api
	index, err = store.LastIndex()
	assert.Nil(t, err)
	assert.EqualValues(t, 19, index)

	// test GetLog api
	for i := 0; i < 10; i++ {
		logptr := new(raft.Log)
		err = store.GetLog(uint64(i), logptr)
		assert.Nil(t, err)
		assert.EqualValues(t, i, logptr.Index)
	}

	// test DeleteRange api
	err = store.DeleteRange(0, 10)
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		logptr := new(raft.Log)
		err = store.GetLog(uint64(i), logptr)
		assert.ErrorIs(t, err, raft.ErrLogNotFound)
	}

	logptr := new(raft.Log)
	err = store.GetLog(11, logptr)
	assert.Nil(t, err)
	assert.EqualValues(t, 11, logptr.Index)
}

func TestParseLogsKey(t *testing.T) {
	idx := uint64(1024)
	bs := buildLogsKey(idx)
	curidx := parseIndexByLogsKey(bs)
	assert.Equal(t, idx, curidx)
}
