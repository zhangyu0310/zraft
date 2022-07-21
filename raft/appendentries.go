package raft

import (
	"context"
	"sync"
	"time"

	zlog "github.com/zhangyu0310/zlogger"

	"zraft/entry"
	"zraft/rpc"
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
	prevLog, ok := z.log[req.PrevLogIndex]
	if !ok {
		zlog.DebugF("Log index [%ld] is not exist", req.PrevLogIndex)
		return
	} else if prevLog.Term != req.PrevLogTerm {
		z.log[req.PrevLogIndex] = nil
		z.nextEntryId = req.PrevLogIndex
		zlog.DebugF("Log index [%ld] have different term [%ld] with leader [%ld]",
			req.PrevLogIndex, prevLog.Term, req.PrevLogTerm)
		return
	}
	z.nextEntryId = req.PrevLogIndex + 1
	for _, e := range req.Entries {
		z.log[z.nextEntryId] = &entry.Entry{
			Term:  req.Term,
			Index: z.nextEntryId,
			Op:    int8(e.Op),
			Key:   e.Key,
			Value: e.Value,
		}
		z.nextEntryId++
	}
	oldCommitIndex := z.commitIndex
	z.commitIndex = req.LeaderCommit
	for i := oldCommitIndex; i <= z.commitIndex; i++ {
		z.commitEntries.Push(z.log[i])
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
	entries := make([]*entry.Entry, 0, 100)
	for {
		e, ok := z.log[nextIndex]
		if !ok {
			break
		}
		entries = append(entries, e)
		nextIndex++
	}
	rpcEntries := makeRpcEntries(entries)
	// Get prev log entry
	prevLogIndex := z.nextIndex[zid] - 1
	prevEntry := z.log[prevLogIndex]
	if prevEntry == nil {
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
		z.nextIndex[zid] = z.matchIndex[zid]
		z.matchIndex[zid] -= 1
	}
}
