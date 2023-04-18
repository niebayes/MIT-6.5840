package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeFollower(term uint64) bool {
	rf.state = Follower
	if term > rf.term {
		rf.term = term
		rf.votedTo = None
		return true
	}
	return false
}

func (rf *Raft) becomeCandidate() {
	defer rf.persist()
	rf.state = Candidate
	rf.term++
	rf.votedMe = make([]bool, len(rf.peers))
	rf.votedTo = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.resetTrackedIndexes()
}

func (rf *Raft) makeRequestVoteArgs(to int) *RequestVoteArgs {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	args := &RequestVoteArgs{From: rf.me, To: to, Term: rf.term, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	return args
}

func (rf *Raft) sendRequestVote(args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	// note: `Call` has an internal timeout mechanism.
	if ok := rf.peers[args.To].Call("Raft.RequestVote", args, &reply); ok {
		rf.handleRequestVoteReply(args, &reply)
	}
}

func (rf *Raft) broadcastRequestVote() {
	for i := range rf.peers {
		if i != rf.me {
			args := rf.makeRequestVoteArgs(i)
			go rf.sendRequestVote(args)
		}
	}
}

func (rf *Raft) eligibleToGrantVote(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	return candidateLastLogTerm > lastLogTerm || (candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term
	reply.Voted = false

	m := Message{Type: Vote, From: args.From, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	if !ok {
		return
	}

	if (rf.votedTo == None || rf.votedTo == args.From) && rf.eligibleToGrantVote(args.LastLogIndex, args.LastLogTerm) {
		rf.votedTo = args.From
		rf.resetElectionTimer()
		reply.Voted = true
	}
}

func (rf *Raft) quorumVoted() bool {
	votes := 1
	for i, votedMe := range rf.votedMe {
		if i != rf.me && votedMe {
			votes++
		}
	}
	return 2*votes > len(rf.peers)
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	m := Message{Type: VoteReply, From: reply.From, Term: reply.Term, ArgsTerm: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	if reply.Voted {
		rf.votedMe[reply.From] = true
		if rf.quorumVoted() {
			rf.becomeLeader()
		}
	}
}
