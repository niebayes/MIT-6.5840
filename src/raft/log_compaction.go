package raft

import (
	"time"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := uint64(index)
	snapshotTerm, err := rf.log.term(snapshotIndex)
	// FIXME: doubt the err checking is necessary.
	if err != nil && snapshotIndex > rf.log.snapshot.Index {
		rf.log.compactedTo(Snapshot{Data: snapshot, Index: snapshotIndex, Term: snapshotTerm})
		rf.persist()
	}
}

func (rf *Raft) makeInstallSnapshot(to int) *InstallSnapshotArgs {
	args := new(InstallSnapshotArgs)
	*args = InstallSnapshotArgs{From: rf.me, To: to, Term: rf.term, Snapshot: rf.log.clonedSnapshot()}
	return args
}

func (rf *Raft) sendInstallSnapshot(args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if ok := rf.peers[args.To].Call("Raft.InstallSnapshot", args, &reply); ok {
		rf.handleInstallSnapshotReply(args, &reply)
	}
}

func (rf *Raft) lagBehindSnapshot(to int) bool {
	return rf.peerTrackers[to].nextIndex <= rf.log.firstIndex()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISNP(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term
	reply.Installed = false

	if args.Term < rf.term {
		return
	}

	termChanged := rf.becomeFollower(args.Term)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	defer rf.resetElectionTimer()

	if args.Snapshot.Index > rf.log.snapshot.Index {
		rf.log.compactedTo(args.Snapshot)
		if !termChanged {
			rf.persist()
		}
	}
}

func (rf *Raft) handleInstallSnapshotReply(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISNPRes(reply)

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
	if rf.term != args.Term || rf.state != Leader || rf.peerTrackers[reply.From].nextIndex <= args.Snapshot.Index {
		return
	}

	if reply.Installed {
		rf.peerTrackers[reply.From].nextIndex = args.Snapshot.Index + 1
	}
}
