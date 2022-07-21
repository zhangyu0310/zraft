package main

import (
	"time"

	"zraft"
	"zraft/raft"
)

func main() {
	zraft.InitZRaft(func(config *raft.Config) {
		config.LocalIP = "127.0.0.1"
		config.HeartbeatGap = 200
		config.ServerPort = 1219
		config.ServerList[1] = "127.0.0.1:1219"
	})
	for {
		time.Sleep(10 * time.Second)
		_ = zraft.Put([]byte("T1"), []byte("This is a test."))
	}
}
