package zraft

import (
	"zraft/raft"
	"zraft/statemachine"
)

func InitZRaft(config func(*raft.Config)) {
	raft.InitializeConfig(config)
	// FIXME: Init log is service.
	_ = raft.InitService()
	_ = raft.Start()
}

func Close() {
	raft.Stop()
}

func Get(key []byte) ([]byte, error) {
	// FIXME: If not leader, send req to get result.
	return statemachine.S.Get(key, nil)
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
