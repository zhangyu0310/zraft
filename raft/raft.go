package raft

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	zlog "github.com/zhangyu0310/zlogger"
	"google.golang.org/grpc"

	"zraft/entry"
	"zraft/rpc"
)

var gZRaft *ZRaft

type ZID int32

type ZRaft struct {
	// For gRPC register
	rpc.UnimplementedRaftServiceServer
	// raft params
	currentTerm uint64
	votedFor    ZID
	log         map[uint64]*entry.Entry
	commitIndex uint64
	lastApplied uint64
	nextIndex   []uint64
	matchIndex  []uint64
	// Implement
	role          uint8
	ID            ZID
	Invalid       atomic.Value
	Srv           *grpc.Server
	Clients       []rpc.RaftServiceClient
	clusterSize   int
	commitEntries *entry.BlockingQueue
	replEntries   *entry.UnblockingQueue
	nextEntryId   uint64
	lastRecvTime  time.Time
	randomTimeout time.Duration
	voteResult    []bool
	readMap       map[uint64][]*entry.Entry
	voteCbLock    sync.Mutex
}

func NewZRaft(size int) {
	gZRaft = &ZRaft{
		currentTerm:   1,
		votedFor:      -1,
		log:           make(map[uint64]*entry.Entry),
		commitIndex:   1,
		lastApplied:   1,
		nextIndex:     make([]uint64, size),
		matchIndex:    make([]uint64, size),
		role:          Follower,
		ID:            -1,
		Srv:           nil,
		Clients:       make([]rpc.RaftServiceClient, size),
		clusterSize:   size,
		commitEntries: entry.NewBlockingQueue(),
		replEntries:   entry.NewUnblockingQueue(),
		nextEntryId:   1,
		lastRecvTime:  time.Time{},
		randomTimeout: getRandomTimeout(),
		readMap:       make(map[uint64][]*entry.Entry),
	}
	gZRaft.resetVoteResult()
	gZRaft.Invalid.Store(true)
}

func (z *ZRaft) resetVoteResult() {
	z.voteResult = make([]bool, z.clusterSize)
}

func (z *ZRaft) Handle() {
	zlog.DebugF("My Role is %s", Role2Str(z.role))
	switch z.role {
	case Follower:
		// Check election timeout
		if z.lastRecvTime.Add(z.randomTimeout).Before(time.Now()) {
			zlog.Debug("Timeout to change to candidate.",
				"Current term is", z.currentTerm)
			z.ChangeToCandidate()
			z.currentTerm++
		}
	case Candidate:
		z.votedFor = z.ID
		// Send vote rpc
		z.resetVoteResult()
		var wg sync.WaitGroup
		for zid, cli := range z.Clients {
			if ZID(zid) == z.ID {
				z.voteResult[zid] = true
				continue
			}
			wg.Add(1)
			go sendRequestVoteRpc(z, ZID(zid), cli, &wg)
		}
		wg.Wait()
		if z.role != Candidate {
			break
		}
		// Check quorum vote grant
		quorum := z.clusterSize/2 + 1
		for zid, result := range z.voteResult {
			if result {
				zlog.InfoF("ID [%d] voted to me.", zid)
				quorum -= 1
			}
			if quorum == 0 {
				break
			}
		}
		if quorum == 0 {
			zlog.Debug("Get quorum vote, become leader.",
				"Term:", z.currentTerm)
			z.ChangeToLeader()
		} else {
			zlog.Debug("Not get quorum vote, Term:", z.currentTerm)
			z.currentTerm++
		}
	case Leader:
		// Put new entries to entry map
		rEntries := z.replEntries.PopAll()
		if rEntries != nil {
			for _, e := range rEntries {
				if e.Op == entry.OpGetEntry {
					if z.nextEntryId == 0 {
						z.commitEntries.Push(e)
					} else {
						e.Term = z.currentTerm
						e.Index = z.nextEntryId - 1
						z.readMap[z.nextEntryId-1] = append(z.readMap[z.nextEntryId-1], e)
					}
				} else {
					e.Term = z.currentTerm
					e.Index = z.nextEntryId
					z.log[z.nextEntryId] = e
					z.nextEntryId++
				}
			}
		}
		// Send entries or heartbeat to all client
		var wg sync.WaitGroup
		for zid, cli := range z.Clients {
			if ZID(zid) == z.ID {
				z.nextIndex[zid] = z.nextEntryId
				continue
			}
			wg.Add(1)
			go sendAppendEntriesRpc(z, ZID(zid), cli, &wg)
		}
		wg.Wait()
		if z.role != Leader {
			break
		}
		// Update commit index
		quorum := z.clusterSize/2 + 1
		indexVec := make([]uint64, 0, 10)
		for _, index := range z.nextIndex {
			indexVec = append(indexVec, index)
		}
		sort.Slice(indexVec, func(i, j int) bool {
			return indexVec[i] > indexVec[j]
		})
		oldCommitIndex := z.commitIndex
		quorumIndex := indexVec[quorum]
		z.commitIndex = quorumIndex
		for i := oldCommitIndex; i <= z.commitIndex; i++ {
			z.commitEntries.Push(z.log[i])
			es, ok := z.readMap[i]
			if ok {
				for _, e := range es {
					z.commitEntries.Push(e)
				}
			}
		}
	default:
		panic("What's the fuck?")
	}
}
