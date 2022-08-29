package raft

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	zlog "github.com/zhangyu0310/zlogger"

	"zraft/queue"
)

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

var ErrChangeToFollower = errors.New("change to follower")

func (z *ZRaft) ChangeToFollower() {
	zlog.Info("Change to follower.")
	z.role = Follower
	z.replEntries = queue.NewUnblockingQueue()
	z.lastRecvTime = time.Now()
	for index, cb := range z.writeCbMap {
		zlog.Info("Index %ld callback not exec. Change to follower.", index)
		cb(ErrChangeToFollower)
	}
}

func (z *ZRaft) ChangeToCandidate() {
	zlog.Info("Change to candidate.")
	z.role = Candidate
	z.resetVoteResult()
}

func (z *ZRaft) ChangeToLeader() {
	zlog.Info("Change to leader.")
	z.role = Leader
	for zid := range z.Clients {
		z.nextIndex[zid] = z.GetNextIndex()
		z.matchIndex[zid] = 0
	}
}

func Role2Str(role uint8) string {
	switch role {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return ""
}
