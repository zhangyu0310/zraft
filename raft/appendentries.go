package raft

import (
	"context"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	zlog "github.com/zhangyu0310/zlogger"

	"zraft/log"
	"zraft/rpc"
	"zraft/statemachine"
)

func (z *ZRaft) AppendEntries(
	_ context.Context,
	req *rpc.AppendEntriesRequest) (reply *rpc.AppendEntriesReply, err error) {
	reply = new(rpc.AppendEntriesReply)
	reply.Term = z.currentTerm
	reply.Success = false
	if z.currentTerm > req.Term {
		return
	}
	if z.currentTerm < req.Term {
		z.currentTerm = req.Term
		if z.role == Leader || z.role == Candidate {
			z.ChangeToFollower()
		}
	}
	z.lastRecvTime = time.Now()
	prevLog, err := z.log.Load(req.PrevLogIndex)
	if err != nil || prevLog == nil {
		zlog.ErrorF("Log index [%ld] is not exist", req.PrevLogIndex)
		return
	} else if prevLog.Term != req.PrevLogTerm {
		z.SetNextIndex(req.PrevLogIndex)
		zlog.DebugF("Log index [%ld] have different term [%ld] with leader [%ld]",
			req.PrevLogIndex, prevLog.Term, req.PrevLogTerm)
		return
	}
	z.SetNextIndex(req.PrevLogIndex + 1)
	for _, e := range req.Entries {
		err = z.log.Store(&log.Entry{
			Term:  req.Term,
			Index: z.GetNextIndex(),
			Op:    int8(e.Op),
			Key:   e.Key,
			Value: e.Value,
		})
		if err != nil {
			zlog.ErrorF("Store log failed, index [%ld], term [%ld], err: %s",
				z.GetNextIndex(), req.Term, err)
			return
		}
		z.NextIndexAdd(1)
	}
	oldCommitIndex := z.commitIndex
	z.commitIndex = req.LeaderCommit
	batch := leveldb.Batch{}
	for i := oldCommitIndex; i <= z.commitIndex; i++ {
		entry, err := z.log.Load(i)
		if err != nil {
			zlog.Error("Get commit log from raft log failed.", err)
			return reply, err
		}
		switch entry.Op {
		case log.OpPutEntry:
			batch.Put(entry.Key, entry.Value)
		case log.OpDelEntry:
			batch.Delete(entry.Key)
		}
	}
	err = statemachine.S.Write(&batch, nil)
	if err != nil {
		zlog.Error("State machine apply committed log failed.", err)
		return
	}
	reply.Success = true
	return
}

func sendAppendEntriesRpc(z *ZRaft, zid ZID, cli rpc.RaftServiceClient, wg *sync.WaitGroup) {
	defer wg.Done()
	if cli == nil {
		createConnectionForRpc(z, zid, "AppendEntries")
		return
	}
	// Get index of entry which need to send
	nextIndex := z.nextIndex[zid]
	entries := make([]*log.Entry, 0, 100)
	for {
		e, err := z.log.Load(nextIndex)
		if err != nil {
			zlog.Error("Raft log load failed.", err)
			z.ChangeToFollower()
		}
		if e == nil {
			break
		}
		entries = append(entries, e)
		nextIndex++
	}
	rpcEntries := makeRpcEntries(entries)
	// Get prev log entry
	prevLogIndex := z.nextIndex[zid] - 1
	prevEntry, err := z.log.Load(prevLogIndex)
	if prevEntry == nil || err != nil {
		zlog.Panic("Last entry can't be nil.",
			"Last log index is", prevLogIndex)
	}
	// Send entries
	reply, err := cli.AppendEntries(context.Background(),
		&rpc.AppendEntriesRequest{
			Term:         z.currentTerm,
			LeaderId:     uint32(z.ID),
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevEntry.Term,
			Entries:      rpcEntries,
			LeaderCommit: z.commitIndex,
		})
	if err != nil {
		zlog.Error("AppendEntries rpc failed.", err)
		z.Clients[zid] = nil
		return
	}
	if reply.Term > z.currentTerm {
		z.ChangeToFollower()
		z.currentTerm = reply.Term
		return
	}
	if reply.Success {
		z.nextIndex[zid] = nextIndex
		z.matchIndex[zid] = nextIndex - 1
	} else {
		z.nextIndex[zid] = z.nextIndex[zid] - 1
	}
}
