package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same order.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// warning: the ticking granularity may acquire to be increased if there're more raft peers.
const tickInterval = 50 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond
const None = -1 // to indicate a peer has not voted to anyone at the current term.

// TODO: change to string type.
type PeerState int

const (
	Follower PeerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state   PeerState
	term    uint64
	votedTo int
	votedMe []bool // true if a peer has voted to me at the current election.

	electionTimeout time.Duration
	lastElection    time.Time

	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time

	log Log

	peerTrackers []PeerTracker // keeps track of each peer's next index and match index.

	applyCh          chan<- ApplyMsg
	claimToBeApplied sync.Cond

	logger Logger
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.logger = *makeLogger(false, "out")
	rf.logger.r = rf

	rf.applyCh = applyCh
	rf.claimToBeApplied = *sync.NewCond(&rf.mu)

	rf.log = makeLog()
	rf.log.logger = &rf.logger

	if rf.persister.RaftStateSize() > 0 {
		rf.readPersist(rf.persister.ReadRaftState())

	} else {
		rf.term = 0
		rf.votedTo = None
	}

	// update tracker indexes with the restored log entries.
	rf.peerTrackers = make([]PeerTracker, len(rf.peers))
	rf.resetTrackedIndexes()

	rf.state = Follower
	rf.resetElectionTimer()
	rf.heartbeatTimeout = heartbeatTimeout
	rf.logger.stateToFollower(rf.term)

	go rf.ticker()
	go rf.committer()

	return rf
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		switch rf.state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() {
				rf.logger.elecTimeout()
				rf.becomeCandidate()
				rf.broadcastRequestVote()
			}

		case Leader:
			if !rf.quorumActive() {
				rf.logger.stepDown()
				rf.becomeFollower(rf.term)
				break
			}

			forced := false
			if rf.pastHeartbeatTimeout() {
				forced = true
				rf.resetHeartbeatTimer()
			}
			rf.broadcastAppendEntries(forced)
		}

		rf.mu.Unlock()
		time.Sleep(tickInterval)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// warning: `rf.killed` checking is not necessary.
	isLeader := !rf.killed() && rf.state == Leader
	if !isLeader {
		return 0, 0, false
	}

	index := rf.log.lastIndex() + 1
	entry := Entry{Index: index, Term: rf.term, Data: command}
	rf.log.append([]Entry{entry})
	rf.persist()

	rf.broadcastAppendEntries(true)

	// warning: the returned index and term are only used by tests.
	return int(index), int(rf.term), true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// warning: `rf.killed` checking is not necessary.
	return int(rf.term), !rf.killed() && rf.state == Leader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
