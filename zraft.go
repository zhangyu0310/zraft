package zraft

import (
	"zraft/entry/statemachine"
	"zraft/raft"
)

func InitZRaft(config func(*raft.Config)) {
	raft.InitializeConfig(config)
	_ = raft.InitService()
	_ = raft.Start()
}

func Close() {
	raft.Stop()
}

func GetLocal(key []byte) ([]byte, error) {
	return statemachine.S.Get(key, nil)
}

func Get(key []byte) ([]byte, error) {
	return raft.Get(key)
}

func Put(key, value []byte) error {
	return raft.Put(key, value)
}

func Delete(key []byte) error {
	return raft.Delete(key)
}

func IsLeader() bool {
	return raft.IsLeader()
}
