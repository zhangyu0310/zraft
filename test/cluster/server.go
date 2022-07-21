package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/zhangyu0310/zlogger"

	"zraft"
	"zraft/raft"
)

var id = flag.Int("id", 0, "ID of raft service")
var size = flag.Int("size", 1, "Size of raft node")

func initConfig(config *raft.Config) {
	config.LocalIP = "127.0.0.1"
	config.ServerPort = 10000 + *id
	config.HeartbeatGap = 200
	for i := 0; i < *size; i++ {
		config.ServerList[raft.ZID(i)] = fmt.Sprintf("127.0.0.1:%d", 10000+i)
	}
}

func main() {
	flag.Parse()
	zraft.InitZRaft(initConfig)
	exitChan := make(chan os.Signal)
	signal.Notify(exitChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	autoIncrement := 0
	go func() {
		for {
			if zraft.IsLeader() {
				key := fmt.Sprintf("%d-%d", *id, autoIncrement)
				err := zraft.Put([]byte(key), []byte("Random data: "+uuid.New().String()))
				if err != nil {
					zlogger.Error("Put key", key, "failed.")
				} else {
					zlogger.Debug("Put key", key, "success.")
					autoIncrement++
				}
				fmt.Printf("My id is [%d]. I am leader.\n", *id)
			}
			time.Sleep(time.Second * 2)
		}
	}()
	<-exitChan
	zraft.Close()
}
