package raft

import (
	"time"

	"zraft/entry"
)

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

func (z *ZRaft) ChangeToFollower() {
	z.role = Follower
	z.replEntries = entry.NewUnblockingQueue()
	z.lastRecvTime = time.Now()
}

func (z *ZRaft) ChangeToCandidate() {
	z.role = Candidate
	z.resetVoteResult()
}

func (z *ZRaft) ChangeToLeader() {
	z.role = Leader
	for zid := range z.Clients {
		z.nextIndex[zid] = z.nextEntryId
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
