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

func (rf *Raft) makeHeartbeatArgs(to int) *HeartbeatArgs {
	args := new(HeartbeatArgs)
	*args = HeartbeatArgs{From: rf.me, To: to, Term: rf.term, CommittedIndex: rf.log.committed}
	return args
}

func (rf *Raft) sendHeartbeat(args *HeartbeatArgs) {
	reply := HeartbeatReply{}
	if ok := rf.peers[args.To].Call("Raft.Heartbeat", args, &reply); ok {
		rf.handleHeartbeatReply(args, &reply)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.logger.bcastHBET()
	for i := range rf.peers {
		if i != rf.me {
			args := rf.makeHeartbeatArgs(i)
			go rf.sendHeartbeat(args)
		}
	}
}

// RequestVote request handler.
func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvHBET(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term

	if args.Term < rf.term {
		return
	}

	rf.becomeFollower(args.Term, false)
	lastNewEntryIndex := uint64(0)
	if args.CommittedIndex > rf.log.committed {
		index := min(args.CommittedIndex, lastNewEntryIndex)
		rf.log.committedTo(index)
	}

	reply.Term = rf.term
}

func (rf *Raft) handleHeartbeatReply(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term, false)
		return
	}
}
