package raftbadger

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/hashicorp/raft"
)

var (
	// bucket for raft logs
	prefixDBLogs = []byte("logs-")

	// bucket for raft config
	prefixDBMeta = []byte("meta-")

	// not found the key
	ErrNotFoundKey        = raft.ErrLogNotFound
	ErrNotFoundFirstIndex = errors.New("not found first index")
	ErrNotFoundLastIndex  = errors.New("not found last index")
)

// buildConfKey prefixDBLogs + key
func buildLogsKey(idx uint64) []byte {
	bs := append([]byte{}, prefixDBLogs...)
	return append(bs, uint64ToBytes(idx)...)
}

// buildMetaKey prefixDBConfig + key
func buildMetaKey(key []byte) []byte {
	return []byte(fmt.Sprintf("%s%d", prefixDBMeta, key))
}

// parseIndexByLogsKey parse the index from logs key
func parseIndexByLogsKey(key []byte) uint64 {
	rawkey := key[len(prefixDBLogs):]
	idx := bytesToUint64(rawkey)
	return idx
}

// getPrefixDBLogs clone new prefixDBLogs object
func getPrefixDBLogs() []byte {
	return append([]byte{}, prefixDBLogs...)
}

type Config struct {
	DataPath    string `yaml:"dbpath" json:"dbpath" toml:"dbpath"`
	Compression string `yaml:"compression" json:"compression" toml:"compression"`
}

// Storage
type Storage struct {
	config Config
	opts   *badger.Options
	db     *badger.DB
}

// New badger storage object with config and badger options.
func New(config Config, opts *badger.Options) (*Storage, error) {
	if config.DataPath == "" {
		config.DataPath = os.TempDir()
	}

	// reset dir config
	fpath := path.Join(config.DataPath)
	if opts == nil {
		pv := badger.DefaultOptions(fpath)
		opts = &pv
	}
	if opts.Dir == "" {
		opts.Dir = fpath
	}
	if opts.ValueDir == "" {
		opts.ValueDir = fpath
	}

	switch config.Compression {
	case "zstd":
		opts.Compression = options.ZSTD
	case "snappy":
		opts.Compression = options.Snappy
	}

	// init storage
	store := &Storage{
		config: config,
		opts:   opts,
	}

	var err error
	store.db, err = badger.Open(*store.opts)
	if err != nil {
		return nil, err
	}
	return store, err
}

// FirstIndex get the first index from the Raft log.
func (s *Storage) FirstIndex() (uint64, error) {
	var (
		first = uint64(0)
		err   error
	)

	err = s.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions) // order asc
		defer iter.Close()

		var has bool
		iter.Seek(prefixDBLogs)
		if iter.ValidForPrefix(prefixDBLogs) {
			item := iter.Item()
			first = parseIndexByLogsKey(item.Key())
			has = true
		}
		if !has {
			return ErrNotFoundFirstIndex
		}
		return nil
	})
	return first, err
}

var maxSeekKey = append(getPrefixDBLogs(), uint64ToBytes(math.MaxUint64)...)

// LastIndex get the last index from the Raft log.
func (s *Storage) LastIndex() (uint64, error) {
	var (
		last = uint64(0)
		err  error
	)

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			PrefetchValues: true, // prefetch values
			PrefetchSize:   1,    // default 100
			Reverse:        true, // order desc
		}

		iter := txn.NewIterator(opts)
		defer iter.Close()

		var has bool
		iter.Seek(maxSeekKey)
		if iter.ValidForPrefix(prefixDBLogs) {
			item := iter.Item()
			key := item.Key()[len(prefixDBLogs):]
			last = bytesToUint64(key)
			has = true
		}
		if !has {
			return ErrNotFoundLastIndex
		}
		return nil
	})
	return last, err
}

// GetLog is used to get a log from Badger by a given index.
func (s *Storage) GetLog(idx uint64, log *raft.Log) error {
	return s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(buildLogsKey(idx)))
		if item == nil {
			return raft.ErrLogNotFound
		}

		var val []byte
		val, err = item.ValueCopy(val)
		if err != nil {
			return err
		}

		return decodeMsgpack(val, log)
	})
}

// StoreLog is used to store a single raft log
func (s *Storage) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (s *Storage) StoreLogs(logs []*raft.Log) error {
	maxBatchSize := s.db.MaxBatchSize()
	min := uint64(0)
	max := uint64(len(logs))
	ranges := s.generateRanges(min, max, maxBatchSize)
	for _, r := range ranges {
		txn := s.db.NewTransaction(true)
		defer txn.Discard()

		for index := r.from; index < r.to; index++ {
			log := logs[index]
			key := buildLogsKey(log.Index)
			out, err := encodeMsgpack(log)
			if err != nil {
				return err
			}
			if err := txn.Set(key, out.Bytes()); err != nil {
				return err
			}
		}
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type iteratorRange struct{ from, to uint64 }

func (s *Storage) generateRanges(min, max uint64, batchSize int64) []iteratorRange {
	nSegments := int(math.Round(float64((max - min) / uint64(batchSize))))
	segments := []iteratorRange{}
	if (max - min) <= uint64(batchSize) {
		segments = append(segments, iteratorRange{from: min, to: max})
		return segments
	}
	for len(segments) < nSegments {
		nextMin := min + uint64(batchSize)
		segments = append(segments, iteratorRange{from: min, to: nextMin})
		min = nextMin + 1
	}
	segments = append(segments, iteratorRange{from: min, to: max})
	return segments
}

// DeleteRange is used to delete logs within a given range.
func (s *Storage) DeleteRange(min, max uint64) error {
	txn := s.db.NewTransaction(true)
	defer func() {
		txn.Discard()
	}()

	// create iterator, reset seek offset
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	iter.Rewind()

	minKey := buildLogsKey(min)
	for iter.Seek(minKey); iter.ValidForPrefix(prefixDBLogs); iter.Next() {
		item := iter.Item()
		// parse the index from logs key
		idx := parseIndexByLogsKey(item.Key())
		if idx > max {
			break
		}

		// del kv
		delKey := buildLogsKey(idx)
		if err := txn.Delete(delKey); err != nil {
			iter.Close()
			return err
		}
	}
	iter.Close()
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

// Set is used to set kv
func (s *Storage) Set(key, val []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		key = buildMetaKey(key)
		return txn.Set(key, val)
	})
	return err
}

// Get is used to get value by key
func (s *Storage) Get(key []byte) ([]byte, error) {
	var (
		val []byte
		err error
	)

	err = s.db.View(func(txn *badger.Txn) error {
		key = buildMetaKey(key)
		item, err := txn.Get(key)
		if err != nil {
			return raft.ErrLogNotFound
		}

		val, err = item.ValueCopy(val)
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}

// SetUint64 key and val
func (s *Storage) SetUint64(key []byte, val uint64) error {
	key = buildMetaKey(key)
	s.Set(key, uint64ToBytes(val))

	return nil
}

// GetUint64 returns uint64 value of key
func (s *Storage) GetUint64(key []byte) (uint64, error) {
	key = buildMetaKey(key)
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}

	return bytesToUint64(val), nil
}

// Delete kv of the key
func (s *Storage) Delete(key []byte) error {
	key = buildMetaKey(key)
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	return err
}

// DropAll drop all kvs in the badger db.
func (s *Storage) DropAll() error {
	return s.db.DropAll()
}

// Sync db to disk
func (s *Storage) Sync() error {
	return s.db.Sync()
}

// Close the storage connection
func (s *Storage) Close() error {
	if s.db == nil {
		return nil
	}

	return s.db.Close()
}

// GetDB return badger database instance.
func (s *Storage) GetDB() *badger.DB {
	return s.db
}

// DeleteFiles delete badgerDB files
func (s *Storage) DeleteFiles() {
	if s.db != nil {
		s.Close() // close() is safe call
	}

	os.RemoveAll(s.db.Opts().Dir)
	os.RemoveAll(s.db.Opts().ValueDir)
}
