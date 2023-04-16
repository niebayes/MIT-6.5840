package raft

import (
	"fmt"
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

	// there's a bug that next index may go out of the last index.
	// that's because if forced is set to true, i.e. heartbeat timeout,
	// we have to make append entries args as well.
	nextIndex := rf.peerTrackers[to].nextIndex
	entries, err := rf.log.slice(nextIndex, rf.log.lastIndex()+1)
	if err != nil {
		fmt.Printf("N%v start=%v end=%v FI=%v LI=%v -> N%v\n", rf.me, nextIndex, rf.log.lastIndex()+1, rf.log.firstIndex(), rf.log.lastIndex(), to)
		panic(err)
	}

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
		if i == rf.me {
			continue
		}

		if rf.lagBehindSnapshot(i) {
			args := rf.makeInstallSnapshot(i)
			rf.logger.sendISNP(i, args.Snapshot.Index, args.Snapshot.Term)
			go rf.sendInstallSnapshot(args)

		} else if forced || rf.hasNewEntries(i) {
			args := rf.makeAppendEntriesArgs(i)

			if len(args.Entries) > 0 {
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
	if index > rf.log.committed {
		rf.log.committedTo(index)
		rf.claimToBeApplied.Signal()
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

	m := Message{Type: Append, From: args.From, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	if !ok {
		return
	}

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

func (rf *Raft) maybeCommitMatched(index uint64) bool {
	for i := index; i > rf.log.committed; i-- {
		if term, _ := rf.log.term(i); term == rf.term && rf.quorumMatched(i) {
			rf.log.committedTo(i)
			rf.claimToBeApplied.Signal()
			return true
		}
	}
	return false
}

func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		rf.logger.recvAENTRes(reply)
	} else {
		rf.logger.recvHBETRes(reply)
	}

	m := Message{Type: AppendReply, From: reply.From, Term: reply.Term, ArgsTerm: args.Term, PrevLogIndex: args.PrevLogIndex}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
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

		if rf.maybeCommitMatched(rf.peerTrackers[reply.From].matchIndex) {
			rf.broadcastAppendEntries(true)
		}

	case IndexNotMatched:
		// warning: only if the follower's log is actually shorter than the leader's,
		// the leader could adopt the follower's last log index.
		// in any cases, the next index cannot be larger than the leader's last log index + 1.
		if reply.LastLogIndex < rf.log.lastIndex() {
			rf.peerTrackers[reply.From].nextIndex = reply.LastLogIndex + 1
		} else {
			rf.peerTrackers[reply.From].nextIndex = rf.log.lastIndex() + 1
		}

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

		// FIXME: figure out whether the next index is eligible to be advanced.
		rf.peerTrackers[reply.From].nextIndex = newNextIndex

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		if newNext != oldNext || newMatch != oldMatch {
			rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)
		}
	}
}
