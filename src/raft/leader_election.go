package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetVote() {
	rf.votedMe = make([]bool, len(rf.peers))
	rf.votedMe[rf.me] = true
	rf.votedTo = None
}

func (rf *Raft) becomeFollower(term uint64) {
	oldTerm := rf.term

	// FIXME: how about leader?
	if term > rf.term || rf.state == Candidate {
		rf.term = term
		rf.logger.stateToFollower(oldTerm)
		rf.state = Follower
		rf.resetVote()
	}
	// reset election timer to not immediately start a new round of election to compete with the current leader.
	rf.resetElectionTimer()

}

func (rf *Raft) becomeCandidate() {
	rf.term += 1
	rf.logger.stateToCandidate()
	rf.resetVote()
	rf.votedTo = rf.me
	rf.state = Candidate
}

func (rf *Raft) becomeLeader() {
	rf.resetTrackers()
	rf.logger.stateToLeader()
	rf.state = Leader
}

func (rf *Raft) makeRequestVoteArgs(to int) *RequestVoteArgs {
	args := new(RequestVoteArgs)
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	*args = RequestVoteArgs{From: rf.me, To: to, Term: rf.term, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	return args
}

func (rf *Raft) sendRequestVote(args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	if ok := rf.peers[args.To].Call("Raft.RequestVote", args, &reply); ok {
		rf.handleRequestVoteReply(args, &reply)
	}
}

func (rf *Raft) broadcastRequestVote() {
	rf.logger.bcastRVOT()
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

// RequestVote request handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvRVOT(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term
	reply.VotedTo = rf.votedTo

	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term {
		rf.becomeFollower(args.Term)
	}

	if (rf.votedTo == None || rf.votedTo == args.From) && rf.eligibleToGrantVote(args.LastLogIndex, args.LastLogTerm) {
		rf.votedTo = args.From
		reply.VotedTo = args.From
		rf.logger.voteTo(args.From)
	} else {
		lastLogIndex := rf.log.lastIndex()
		lastLogTerm, _ := rf.log.term(lastLogIndex)
		rf.logger.rejectVoteTo(args.From, args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm)
	}

	reply.Term = rf.term
}

func (rf *Raft) receivedMajorityVotes() bool {
	votes := 0
	for _, votedMe := range rf.votedMe {
		if votedMe {
			votes++
		}
	}
	if 2*votes > len(rf.peers) {
		rf.logger.recvVoteQuorum(uint64(votes))
	}
	return 2*votes > len(rf.peers)
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvRVOTRes(reply)

	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term)
	}

	if args.Term == reply.Term && rf.state == Candidate && reply.VotedTo == rf.me {
		rf.votedMe[reply.From] = true
		if rf.receivedMajorityVotes() {
			rf.becomeLeader()
		}
	}
}
