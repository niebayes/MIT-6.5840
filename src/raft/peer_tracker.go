package raft

type PeerTracker struct {
	nextIndex  uint64
	matchIndex uint64
}

func (rf *Raft) resetTrackers() {
	for i := range rf.peerTrackers {
		rf.peerTrackers[i] = PeerTracker{nextIndex: rf.log.lastIndex(), matchIndex: rf.log.applied}
	}
}
