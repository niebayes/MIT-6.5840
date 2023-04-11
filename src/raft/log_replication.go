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

func (rf *Raft) findFirstConflict(index uint64) (uint64, uint64) {
	conflictTerm, _ := rf.log.term(index)
	firstConflictIndex := index
	// warning: skip the snapshot index since it cannot conflict if all goes well.
	for i := index - 1; i > rf.log.firstIndex(); i-- {
		if term, _ := rf.log.term(i); term != conflictTerm {
			break
		}
		firstConflictIndex = i
	}
	return conflictTerm, firstConflictIndex
}

func (rf *Raft) maybeCommittedTo(index uint64) {
	// FIXME: doubt this `min` is necessary.
	if index := min(index, rf.log.lastIndex()); index > rf.log.committed {
		// TODO: add persistence for committed index and applied index.
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
	reply.Err = Rejected

	if args.Term < rf.term {
		return
	}

	termChanged := rf.becomeFollower(args.Term)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	defer rf.resetElectionTimer()

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		if reply.Err == IndexNotMatched {
			reply.LastLogIndex = rf.log.lastIndex()
		} else {
			reply.ConflictTerm, reply.FirstConflictIndex = rf.findFirstConflict(args.PrevLogIndex)
		}

		rf.logger.rejectEnts(args.From)
		return
	}
	rf.logger.acceptEnts(args.From)

	for i, entry := range args.Entries {
		if term, err := rf.log.term(entry.Index); err != nil || term != entry.Term {
			rf.log.truncateSuffix(entry.Index)
			rf.log.append(args.Entries[i:])
			if !termChanged {
				rf.persist()
			}
			break
		}
	}

	lastNewLogIndex := min(args.CommittedIndex, args.PrevLogIndex+uint64(len(args.Entries)))
	rf.maybeCommittedTo(lastNewLogIndex)
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

	if len(args.Entries) > 0 {
		rf.logger.recvAENTRes(reply)
	} else {
		rf.logger.recvHBETRes(reply)
	}

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term < rf.term {
		return
	}

	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term)
		rf.persist()
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
		rf.peerTrackers[reply.From].nextIndex = reply.LastLogIndex + 1

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		if newNext != oldNext || newMatch != oldMatch {
			rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)
		}

	case TermNotMatched:
		newNextIndex := reply.FirstConflictIndex
		// warning: skip the snapshot index since it cannot conflict if all goes well.
		for i := rf.log.lastIndex(); i > rf.log.firstIndex(); i-- {
			if term, _ := rf.log.term(i); term == reply.ConflictTerm {
				newNextIndex = i
				break
			}
		}

		// FIXME: figure out whether the next index can be advanced.
		rf.peerTrackers[reply.From].nextIndex = newNextIndex

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		if newNext != oldNext || newMatch != oldMatch {
			rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)
		}
	}
}
