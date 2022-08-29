package raft

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	zlog "github.com/zhangyu0310/zlogger"
	"google.golang.org/grpc"

	"zraft/log"
	"zraft/queue"
	"zraft/rpc"
	"zraft/statemachine"
)

var gZRaft *ZRaft

type ZID int32

type ZRaft struct {
	// For gRPC register
	rpc.UnimplementedRaftServiceServer
	// raft params
	currentTerm uint64
	votedFor    ZID
	log         log.Log
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
	replEntries   *queue.UnblockingQueue
	lastRecvTime  time.Time
	randomTimeout time.Duration
	voteResult    []bool
	writeCbMap    map[uint64]func(error)
	voteCbLock    sync.Mutex
}

func NewZRaft(size int, l log.Log) {
	gZRaft = &ZRaft{
		currentTerm:   1,
		votedFor:      -1,
		log:           l,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]uint64, size),
		matchIndex:    make([]uint64, size),
		role:          Follower,
		ID:            -1,
		Srv:           nil,
		Clients:       make([]rpc.RaftServiceClient, size),
		clusterSize:   size,
		replEntries:   queue.NewUnblockingQueue(),
		lastRecvTime:  time.Time{},
		randomTimeout: getRandomTimeout(),
		writeCbMap:    make(map[uint64]func(error)),
	}
	gZRaft.resetVoteResult()
	gZRaft.Invalid.Store(true)
}

func (z *ZRaft) resetVoteResult() {
	z.voteResult = make([]bool, z.clusterSize)
}

func (z *ZRaft) GetZID() ZID {
	return z.ID
}

func (z *ZRaft) GetNextIndex() uint64 {
	return z.nextIndex[z.ID]
}

func (z *ZRaft) SetNextIndex(n uint64) {
	z.nextIndex[z.ID] = n
}

func (z *ZRaft) NextIndexAdd(n uint64) {
	z.nextIndex[z.ID] += n
}

func (z *ZRaft) NextIndexSub(n uint64) {
	z.nextIndex[z.ID] -= n
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
		wrappers := z.replEntries.PopAll()
		if wrappers != nil {
			for _, wrapper := range wrappers {
				entry := wrapper.Entry
				entry.Term = z.currentTerm
				entry.Index = z.GetNextIndex()
				err := z.log.Store(entry)
				if err != nil {
					z.ChangeToFollower()
					return
				}
				z.NextIndexAdd(1)
				z.writeCbMap[entry.Index] = wrapper.Callback
			}
		}
		// Send entries or heartbeat to all client
		var wg sync.WaitGroup
		for zid, cli := range z.Clients {
			if ZID(zid) == z.ID {
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
			indexVec = append(indexVec, index-1)
		}
		sort.Slice(indexVec, func(i, j int) bool {
			return indexVec[i] > indexVec[j]
		})
		oldCommitIndex := z.commitIndex
		quorumIndex := indexVec[quorum-1]
		z.commitIndex = quorumIndex
		batch := leveldb.Batch{}
		for i := oldCommitIndex + 1; i <= z.commitIndex; i++ {
			entry, err := z.log.Load(i)
			if err != nil || entry == nil {
				zlog.Error("log load entry failed.", err)
				z.commitIndex = i - 1
				z.ChangeToFollower()
				return
			}
			switch entry.Op {
			case log.OpPutEntry:
				batch.Put(entry.Key, entry.Value)
			case log.OpDelEntry:
				batch.Delete(entry.Key)
			}
		}
		err := statemachine.S.Write(&batch, nil)
		if err != nil {
			zlog.Error("State machine write batch failed.", err)
			z.ChangeToFollower()
			return
		}
		for i := oldCommitIndex + 1; i <= z.commitIndex; i++ {
			cb, ok := z.writeCbMap[i]
			if ok && cb != nil {
				cb(nil)
			}
		}
	default:
		panic("What's the fuck?")
	}
}
