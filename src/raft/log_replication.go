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

			if len(args.Entries) > 0 {
				// FIXME: figure out why this would log twice for a peer?
				rf.logger.sendEnts(args.PrevLogIndex, args.PrevLogTerm, args.Entries, i)
			} else {
				rf.logger.sendBeat(args.PrevLogIndex, args.PrevLogTerm, i)
			}

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

func (rf *Raft) truncateLogSuffix(index uint64) {
	if rf.log.truncateSuffix(index) {
		rf.persist()
	}
}

func (rf *Raft) appendLog(entries []Entry) {
	rf.log.append(entries)
	rf.persist()
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
	reply.Err = Rejected

	if args.Term < rf.term {
		return
	}

	rf.becomeFollower(args.Term, true)
	reply.Term = rf.term

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		// TODO: add accelerated log backup.
		rf.logger.rejectEnts(args.From)
		return
	}
	rf.logger.acceptEnts(args.From)

	if reply.Err != Matched {
		panic("not matched")
	}

	for i, entry := range args.Entries {
		if term, err := rf.log.term(entry.Index); err != nil || term != entry.Term {
			rf.truncateLogSuffix(entry.Index)
			rf.appendLog(args.Entries[i:])
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

	// we must ensure that the peer is in the same state as when sending the args.
	if rf.term != args.Term || rf.state != Leader || rf.peerTrackers[reply.From].nextIndex-1 != args.PrevLogIndex {
		return
	}

	oldNext := rf.peerTrackers[reply.From].nextIndex
	oldMatch := rf.peerTrackers[reply.From].matchIndex

	switch reply.Err {
	case Rejected:
		// do nothing.

	case Matched:
		rf.peerTrackers[reply.From].matchIndex = args.PrevLogIndex + uint64(len(args.Entries))
		rf.peerTrackers[reply.From].nextIndex = rf.peerTrackers[reply.From].matchIndex + 1

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		if newNext != oldNext || newMatch != oldMatch {
			rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)
		}

		rf.maybeCommitMatched(rf.peerTrackers[reply.From].matchIndex)

	case IndexNotMatched:
		fallthrough
		// TODO: add accelerated log backup.

	case TermNotMatched:
		// TODO: add accelerated log backup.

		rf.peerTrackers[reply.From].nextIndex--

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)

	default:
		panic("invalid Err type")
	}
}
