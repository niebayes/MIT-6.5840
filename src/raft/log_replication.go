package raft

import (
	"time"
)

func (rf *Raft) pastHeartbeatTimeout() bool {
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) makeAppendEntriesArgs(to int) *AppendEntriesArgs {
	args := new(AppendEntriesArgs)

	nextIndex := rf.peerTrackers[to].nextIndex
	entries, _ := rf.log.slice(nextIndex, rf.log.lastIndex()+1)

	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := rf.log.term(prevLogIndex)

	*args = AppendEntriesArgs{From: rf.me, To: to, Term: rf.term, CommittedIndex: rf.log.committed, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries}
	return args
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.peers[args.To].Call("Raft.AppendEntries", args, &reply); ok {
		rf.handleAppendEntriesReply(args, &reply)
	}
}

func (rf *Raft) hasNewEntries(to int) bool {
	return rf.log.lastIndex() >= rf.peerTrackers[to].nextIndex
}

func (rf *Raft) broadcastAppendEntries(forced bool) {
	for i := range rf.peers {
		if i != rf.me && (forced || rf.hasNewEntries(i)) {
			args := rf.makeAppendEntriesArgs(i)
			rf.logger.sendEnts(args.PrevLogIndex, args.PrevLogTerm, args.Entries, i)
			go rf.sendAppendEntries(args)
		}
	}
}

func (rf *Raft) checkLogPrefixMatched(leaderPrevLogIndex, leaderPrevLogTerm uint64) Err {
	prevLogTerm, err := rf.log.term(leaderPrevLogIndex)
	if err != nil {
		return IndexNotMatched
	}
	if prevLogTerm != leaderPrevLogTerm {
		return TermNotMatched
	}
	return Matched
}

func (rf *Raft) maybeCommittedTo(index uint64) {
	if index := min(index, rf.log.lastIndex()); index > rf.log.committed {
		rf.log.committedTo(index)
		rf.hasNewCommittedEntries.Signal()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		rf.logger.recvAENT(args)
	} else {
		rf.logger.recvHBET(args)
	}

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term

	if args.Term < rf.term {
		return
	}

	rf.becomeFollower(args.Term, false)
	reply.Term = rf.term

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		// TODO: add accelerated log backup.
		rf.logger.rejectEnts(args.From)
		return
	}
	rf.logger.acceptEnts(args.From)

	for i, entry := range args.Entries {
		if term, err := rf.log.term(entry.Index); err != nil || term != entry.Term {
			rf.log.truncateSuffix(entry.Index)
			rf.log.append(args.Entries[i:])
			break
		}
	}

	rf.maybeCommittedTo(args.CommittedIndex)
}

func (rf *Raft) quorumMatched(index uint64) bool {
	matched := 1
	for _, tracker := range rf.peerTrackers {
		if tracker.matchIndex >= index {
			matched++
		}
	}
	return 2*matched > len(rf.peers)
}

func (rf *Raft) maybeCommitMatched(index uint64) {
	for i := index; i > rf.log.committed; i-- {
		if term, err := rf.log.term(i); err == nil && term == rf.term && rf.quorumMatched(i) {
			rf.log.committedTo(i)
			rf.hasNewCommittedEntries.Signal()
			break
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvAENTRes(reply)

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term, false)
		return
	}

	oldNext := rf.peerTrackers[reply.From].nextIndex
	oldMatch := rf.peerTrackers[reply.From].matchIndex

	switch reply.Err {
	case Matched:
		rf.peerTrackers[reply.From].nextIndex = max(rf.peerTrackers[reply.From].nextIndex, args.PrevLogIndex+uint64(len(args.Entries))+1)
		rf.peerTrackers[reply.From].matchIndex = max(rf.peerTrackers[reply.From].matchIndex, rf.peerTrackers[reply.From].nextIndex-1)

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)

		rf.maybeCommitMatched(rf.peerTrackers[reply.From].matchIndex)

	default:
		// TODO: add accelerated log backup.
		rf.peerTrackers[reply.From].nextIndex--

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)
	}

}
