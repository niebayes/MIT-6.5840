package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.pullSnap(uint64(index))

	// FIXME: doubt this checking is necessary.
	if rf.log.hasPendingSnapshot {
		return
	}

	snapshotIndex := uint64(index)
	snapshotTerm, err := rf.log.term(snapshotIndex)
	// FIXME: doubt the err checking is necessary.
	if err == nil && snapshotIndex > rf.log.snapshot.Index {
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

	m := Message{Type: Snap, From: args.From, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	if !ok {
		return
	}

	// reject stale snapshots.
	if args.Snapshot.Index <= rf.log.snapshot.Index {
		// but return Installed true to handle unreliable network, i.e. dup, reorder.
		reply.Installed = true
		return
	}

	rf.log.compactedTo(args.Snapshot)
	reply.Installed = true
	if !termChanged {
		defer rf.persist()
	}

	rf.log.hasPendingSnapshot = true
	rf.claimToBeApplied.Signal()
}

func (rf *Raft) handleInstallSnapshotReply(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISNPRes(reply)

	m := Message{Type: SnapReply, From: reply.From, Term: reply.Term, ArgsTerm: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	if reply.Installed {
		oldNext := rf.peerTrackers[reply.From].nextIndex
		oldMatch := rf.peerTrackers[reply.From].matchIndex

		rf.peerTrackers[reply.From].matchIndex = args.Snapshot.Index
		rf.peerTrackers[reply.From].nextIndex = rf.peerTrackers[reply.From].matchIndex + 1

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		if newNext != oldNext || newMatch != oldMatch {
			rf.logger.updateProgOf(reply.From, oldNext, oldMatch, newNext, newMatch)
		}
	}
}
