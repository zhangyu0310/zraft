package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"zraft/entry"
	"zraft/rpc"
)

func TestGetRandomTimeout(t *testing.T) {
	gap := 100
	StoreGlobalConfig(&Config{
		ServerList:   nil,
		LocalIP:      "",
		ServerPort:   0,
		HeartbeatGap: gap,
	})
	timeout := getRandomTimeout()
	assert.Greater(t, timeout, time.Duration(gap*10)*time.Millisecond)
}

func TestMakeRpcEntries(t *testing.T) {
	entries := []*entry.Entry{
		{Term: 1, Index: 1, Op: entry.OpPutEntry, Key: []byte("put"), Value: []byte("1")},
		{Term: 1, Index: 2, Op: entry.OpDelEntry, Key: []byte("del"), Value: []byte("1")},
		{Term: 2, Index: 3, Op: entry.OpPutEntry, Key: []byte("put"), Value: []byte("2")},
		{Term: 3, Index: 4, Op: entry.OpDelEntry, Key: []byte("del"), Value: []byte("2")},
	}
	rpcEntries := makeRpcEntries(entries)
	for i := 0; i < 4; i++ {
		switch i {
		case 0:
			assert.Equal(t, []byte("put"), rpcEntries[i].Key)
			assert.Equal(t, []byte("1"), rpcEntries[i].Value)
			assert.Equal(t, rpc.Operate_Put, rpcEntries[i].Op)
		case 1:
			assert.Equal(t, []byte("del"), rpcEntries[i].Key)
			assert.Equal(t, []byte("1"), rpcEntries[i].Value)
			assert.Equal(t, rpc.Operate_Delete, rpcEntries[i].Op)
		case 2:
			assert.Equal(t, []byte("put"), rpcEntries[i].Key)
			assert.Equal(t, []byte("2"), rpcEntries[i].Value)
			assert.Equal(t, rpc.Operate_Put, rpcEntries[i].Op)
		case 3:
			assert.Equal(t, []byte("del"), rpcEntries[i].Key)
			assert.Equal(t, []byte("2"), rpcEntries[i].Value)
			assert.Equal(t, rpc.Operate_Delete, rpcEntries[i].Op)
		}
	}
}
