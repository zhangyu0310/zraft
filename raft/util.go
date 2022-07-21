package raft

import (
	"math/rand"
	"time"

	zlog "github.com/zhangyu0310/zlogger"

	"zraft/entry"
	"zraft/rpc"
)

func getRandomTimeout() time.Duration {
	cfg := GetGlobalConfig()
	timeout := (cfg.HeartbeatGap * 10) + rand.Intn(10*cfg.HeartbeatGap)
	return time.Duration(timeout) * time.Millisecond
}

func makeRpcEntries(entries []*entry.Entry) []*rpc.Entry {
	rpcEntries := make([]*rpc.Entry, 0, 100)
	for _, e := range entries {
		rpcEntries = append(rpcEntries, &rpc.Entry{
			Op:    rpc.Operate(e.Op),
			Key:   e.Key,
			Value: e.Value,
		})
	}
	return rpcEntries
}

func createConnectionForRpc(z *ZRaft, zid ZID, rpcName string) {
	cfg := GetGlobalConfig()
	z.Clients[zid] = CreateConnection(cfg.ServerList[zid])
	zlog.InfoF("Send %s rpc zid:[%d] create connection.", rpcName, zid)
}
