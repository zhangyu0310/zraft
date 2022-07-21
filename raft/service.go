package raft

import (
	"fmt"
	"net"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	zlog "github.com/zhangyu0310/zlogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"zraft/entry"
	"zraft/entry/statemachine"
	"zraft/rpc"
)

var ServerIsNil = errors.New("raft server is nil")
var ServiceIsNotRunning = errors.New("service is not running")
var NotLeader = errors.New("not leader")

func InitService() error {
	cfg := GetGlobalConfig()
	// TODO: Use better grpc options.
	server := grpc.NewServer(grpc.EmptyServerOption{})
	NewZRaft(len(cfg.ServerList))
	rpc.RegisterRaftServiceServer(server, gZRaft)
	gZRaft.Srv = server
	return nil
}

func CreateConnection(host string) rpc.RaftServiceClient {
	// TODO: Use better dial option
	conn, err := grpc.Dial(host,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		zlog.Error("Create gRPC client failed.", err)
		return nil
	}
	cli := rpc.NewRaftServiceClient(conn)
	return cli
}

func Start() error {
	gZRaft.Invalid.Store(false)
	cfg := GetGlobalConfig()
	statemachine.InitStateMachine()
	// Start server
	if gZRaft.Srv != nil {
		addr := fmt.Sprintf("0.0.0.0:%d", cfg.ServerPort)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		go func() {
			err := gZRaft.Srv.Serve(lis)
			if err != nil {
				zlog.Error("Raft server return err.", err)
			}
			Stop()
		}()
	} else {
		zlog.Error("Raft server is nil, must call InitService first.")
		return ServerIsNil
	}
	// Create connection with other server.
	addr := fmt.Sprintf("%s:%d", cfg.LocalIP, cfg.ServerPort)
	gZRaft.clusterSize = len(cfg.ServerList)
	for zid, host := range cfg.ServerList {
		gZRaft.nextIndex[zid] = 1
		if addr == host {
			gZRaft.ID = zid
			continue
		}
		cli := CreateConnection(host)
		gZRaft.Clients[zid] = cli
	}
	if gZRaft.ID == -1 {
		zlog.Fatal("Self ID is invalid.")
	} else {
		gZRaft.log[0] = &entry.Entry{
			Term:     gZRaft.currentTerm,
			Index:    0,
			Op:       -1,
			Key:      nil,
			Value:    nil,
			Callback: nil,
		}
	}
	// Run raft handle loop.
	go func() {
		for IsRunning() {
			gZRaft.Handle()
			time.Sleep(time.Duration(cfg.HeartbeatGap) * time.Millisecond)
		}
	}()
	// Run loop for apply log to leveldb
	go func() {
		for {
			entries := gZRaft.commitEntries.PopAll()
			if entries == nil {
				continue
			}
			for _, e := range entries {
				if e == nil {
					continue
				}
				var err error
				switch e.Op {
				case entry.OpPutEntry:
					err = statemachine.S.Put(e.Key, e.Value, nil)
					if e.Callback != nil {
						e.Callback(nil)
					}
				case entry.OpDelEntry:
					err = statemachine.S.Delete(e.Key, nil)
					if e.Callback != nil {
						e.Callback(nil)
					}
				case entry.OpGetEntry:
					value := make([]byte, 0)
					value, err = statemachine.S.Get(e.Key, nil)
					if e.Callback != nil {
						e.Callback(value)
					}
				}
				if err != nil {
					zlog.Error("Do", entry.GetOpStr(e.Op), "error.", err)
				}
			}
		}
	}()
	return nil
}

func IsRunning() bool {
	return !gZRaft.Invalid.Load().(bool)
}

func IsLeader() bool {
	return gZRaft.role == Leader
}

func Stop() {
	gZRaft.Invalid.Store(true)
	statemachine.Close()
}

func Get(key []byte) ([]byte, error) {
	if !IsRunning() {
		return nil, ServiceIsNotRunning
	}
	value := make([]byte, 0)
	if gZRaft.role == Leader {
		finished := make(chan bool, 1)
		gZRaft.replEntries.Push(&entry.Entry{
			Term:  0,
			Index: 0,
			Op:    entry.OpPutEntry,
			Key:   key,
			Value: nil,
			Callback: func(v []byte) {
				value = v
				finished <- true
				close(finished)
			},
		})
		<-finished
	} else {
		// FIXME: send request to leader.
		return nil, NotLeader
	}
	return value, nil
}

func Put(key, value []byte) error {
	if !IsRunning() {
		return ServiceIsNotRunning
	}
	if gZRaft.role == Leader {
		finished := make(chan bool, 1)
		gZRaft.replEntries.Push(&entry.Entry{
			Term:  0,
			Index: 0,
			Op:    entry.OpPutEntry,
			Key:   key,
			Value: value,
			Callback: func([]byte) {
				finished <- true
				close(finished)
			},
		})
		<-finished
	} else {
		// FIXME: send request to leader.
		return NotLeader
	}
	return nil
}

func Delete(key []byte) error {
	if !IsRunning() {
		return ServiceIsNotRunning
	}
	if gZRaft.role == Leader {
		finished := make(chan bool, 1)
		gZRaft.replEntries.Push(&entry.Entry{
			Term:  0,
			Index: 0,
			Op:    entry.OpDelEntry,
			Key:   key,
			Value: nil,
			Callback: func([]byte) {
				finished <- true
				close(finished)
			},
		})
		<-finished
	} else {
		// FIXME: send request to leader.
		return NotLeader
	}
	return nil
}
