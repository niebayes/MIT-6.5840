package raft

import (
	"time"
)

type PeerTracker struct {
	nextIndex  uint64
	matchIndex uint64

	lastAck time.Time
}

func (rf *Raft) resetTrackedIndexes() {
	for i := range rf.peerTrackers {
		rf.peerTrackers[i].nextIndex = rf.log.lastIndex() + 1
		// FIXME: set to snapshot index?
		rf.peerTrackers[i].matchIndex = 0
	}
}

func (rf *Raft) quorumActive() bool {
	activePeers := 1
	for i, tracker := range rf.peerTrackers {
		if i != rf.me && time.Since(tracker.lastAck) <= 2*baseElectionTimeout*time.Millisecond {
			activePeers++
		}
	}
	return 2*activePeers > len(rf.peers)
}
