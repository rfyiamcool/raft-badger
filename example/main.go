package main

import (
	"fmt"

	"github.com/hashicorp/raft"
	raftbadger "github.com/rfyiamcool/raft-badger"
)

func main() {
	cfg := raftbadger.Config{
		DataPath: "/tmp/raft",
	}
	store, err := raftbadger.New(cfg, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create raft badger storage, err: %s", err.Error()))
	}

	// test dropall api
	err = store.DropAll()
	checkError(err)

	// test set api
	err = store.Set([]byte("blog"), []byte("xiaorui.cc"))
	checkError(err)

	// test get api
	value, err := store.Get([]byte("blog"))
	checkError(err)
	fmt.Println("value is ", string(value))

	// test SetUint64 api
	err = store.SetUint64([]byte("index"), 111)
	checkError(err)

	// test GetUint64 api
	index, err := store.GetUint64([]byte("index"))
	checkError(err)
	fmt.Println("index is ", index)

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
	checkError(err)

	// test FirstIndex api
	index, err = store.FirstIndex()
	checkError(err)
	fmt.Println("the first index is ", index)

	// test LastIndex api
	index, err = store.LastIndex()
	checkError(err)
	fmt.Println("the last index is ", index)

	for i := 0; i < 10; i++ {
		logptr := new(raft.Log)
		err = store.GetLog(uint64(i), logptr)
		checkError(err)
		fmt.Printf("the index of the No.%v log is %v\n", i, logptr.Index)
	}

	err = store.DeleteRange(0, 10)
	checkError(err)

	for i := 0; i < 10; i++ {
		logptr := new(raft.Log)
		err = store.GetLog(uint64(i), logptr)
		if err != nil {
			fmt.Printf("not found the No.%v log\n", i)
		}
		if err == nil {
			panic(fmt.Sprintf("found No.%v log, but tne log is deleted \n", i))
		}
	}

	logptr := new(raft.Log)
	err = store.GetLog(11, logptr)
	checkError(err)
	fmt.Printf("get No.11 log, index is %v\n", logptr.Index)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
