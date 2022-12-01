package raft

import (
	"fmt"
	"net"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	zlog "github.com/zhangyu0310/zlogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"zraft/log"
	"zraft/queue"
	"zraft/rpc"
	"zraft/statemachine"
)

var ServerIsNil = errors.New("raft server is nil")
var ServiceIsNotRunning = errors.New("service is not running")
var NotLeader = errors.New("not leader")

func InitService() error {
	cfg := GetGlobalConfig()
	// TODO: Use better grpc options.
	server := grpc.NewServer(grpc.EmptyServerOption{})
	if cfg.Log == nil {
		cfg.Log = log.NewZRaftLog()
		err := cfg.Log.Init()
		if err != nil {
			zlog.Error("Init raft log failed.", err)
			return err
		}
	}
	NewZRaft(len(cfg.ServerList), cfg.Log)
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
	err := statemachine.InitStateMachine(cfg.StateMachineEngine)
	if err != nil {
		zlog.Error("Start raft service failed, init state machine failed. err:", err)
	}
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
		firstEntry, err := gZRaft.log.Load(0)
		if err != nil {
			zlog.Error("Get first entry failed.", err)
			return err
		}
		if firstEntry == nil {
			err = gZRaft.log.Store(&log.Entry{
				Term:  gZRaft.currentTerm,
				Index: 0,
				Op:    log.OpPutEntry,
				Key:   nil,
				Value: nil,
			})
			if err != nil {
				zlog.Error("Init raft log failed.", err)
				return err
			}
		}
	}
	// Run raft handle loop.
	go func() {
		for IsRunning() {
			gZRaft.Handle()
			time.Sleep(time.Duration(cfg.HeartbeatGap) * time.Millisecond)
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
	err := gZRaft.log.Destroy()
	if err != nil {
		zlog.Error("zraft log destroy failed.", err)
	}
	statemachine.Close()
}

func Put(key, value []byte) error {
	if !IsRunning() {
		return ServiceIsNotRunning
	}
	if gZRaft.role == Leader {
		zlog.Debug("Put", string(key), "begin.")
		success := make(chan error, 1)
		gZRaft.replEntries.Push(
			&queue.Wrapper{
				Entry: &log.Entry{
					Term:  0,
					Index: 0,
					Op:    log.OpPutEntry,
					Key:   key,
					Value: value,
				},
				Callback: func(err error) {
					success <- err
					close(success)
				}})
		err := <-success
		zlog.Debug("Put", string(key), "end.")
		return err
	} else {
		// FIXME: send request to leader.
		return NotLeader
	}
}

func Delete(key []byte) error {
	if !IsRunning() {
		return ServiceIsNotRunning
	}
	if gZRaft.role == Leader {
		zlog.Debug("Delete", string(key), "begin.")
		success := make(chan error, 1)
		gZRaft.replEntries.Push(
			&queue.Wrapper{
				Entry: &log.Entry{
					Term:  0,
					Index: 0,
					Op:    log.OpDelEntry,
					Key:   key,
					Value: nil,
				},
				Callback: func(err error) {
					success <- err
					close(success)
				}})
		err := <-success
		zlog.Debug("Delete", string(key), "end.")
		return err
	} else {
		// FIXME: send request to leader.
		return NotLeader
	}
}
