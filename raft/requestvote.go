package raft

import (
	"context"
	"sync"

	zlog "github.com/zhangyu0310/zlogger"

	"zraft/rpc"
)

func (z *ZRaft) RequestVote(
	_ context.Context,
	req *rpc.RequestVoteRequest) (reply *rpc.RequestVoteReply, err error) {
	z.voteCbLock.Lock()
	defer z.voteCbLock.Unlock()
	reply = new(rpc.RequestVoteReply)
	reply.VoteGranted = false
	reply.Term = z.currentTerm
	if req.Term > gZRaft.currentTerm {
		z.currentTerm = req.Term
		z.votedFor = -1
		if z.role != Follower {
			z.ChangeToFollower()
		}
	}
	if req.Term < z.currentTerm {
		return
	} else if reply.Term == z.currentTerm &&
		z.votedFor != -1 &&
		z.votedFor != ZID(req.CandidateId) {
		// This server has voted to other candidate
		zlog.InfoF("This term [%d] had vote to [%d]",
			z.currentTerm, z.votedFor)
		return
	} else {
		lastLogIndex := z.GetNextIndex() - 1
		lastEntry, err := z.log.Load(lastLogIndex)
		if lastEntry == nil || err != nil {
			zlog.Panic("Last entry can't be nil.",
				"Last log index is", lastLogIndex)
		}
		if req.LastLogTerm >= lastEntry.Term &&
			req.LastLogIndex >= lastLogIndex {
			z.votedFor = ZID(req.CandidateId)
			reply.VoteGranted = true
		}
	}
	return
}

func sendRequestVoteRpc(z *ZRaft, zid ZID, cli rpc.RaftServiceClient, wg *sync.WaitGroup) {
	defer wg.Done()
	if cli == nil {
		createConnectionForRpc(z, zid, "RequestVote")
		return
	}
	lastLogIndex := z.GetNextIndex() - 1
	lastEntry, err := z.log.Load(lastLogIndex)
	if lastEntry == nil || err != nil {
		zlog.Panic("Last entry can't be nil.",
			"Last log index is", lastLogIndex)
	}
	request := &rpc.RequestVoteRequest{
		Term:         z.currentTerm,
		CandidateId:  uint32(z.ID),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastEntry.Term,
	}
	reply, err := cli.RequestVote(context.Background(), request)
	if err != nil {
		zlog.Error("RequestVote rpc failed.", err)
		z.Clients[zid] = nil
		return
	}
	if reply.Term > z.currentTerm {
		z.ChangeToFollower()
		z.currentTerm = reply.Term
		return
	}
	if reply.VoteGranted {
		z.voteResult[zid] = true
	}
}
