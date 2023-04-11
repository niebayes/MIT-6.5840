package raft

import "fmt"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

func (rf *Raft) committer() {
	rf.mu.Lock()
	for !rf.killed() {
		if rf.log.hasPendingSnapshot {
			fmt.Printf("N%v has pending snap\n", rf.me)
			snapshot := rf.log.clonedSnapshot()
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: snapshot.Data, SnapshotIndex: int(snapshot.Index), SnapshotTerm: int(snapshot.Term)}

			rf.mu.Lock()
			rf.log.hasPendingSnapshot = false
			rf.logger.pushSnap(snapshot.Index, snapshot.Term)

		} else if newCommittedEntries := rf.log.newCommittedEntries(); len(newCommittedEntries) > 0 {
			fmt.Printf("N%v has new committed entries. LN=%v FI=%v LI=%v\n", rf.me, len(newCommittedEntries), newCommittedEntries[0].Index, newCommittedEntries[len(newCommittedEntries)-1].Index)
			rf.mu.Unlock()

			for _, entry := range newCommittedEntries {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Data, CommandIndex: int(entry.Index)}
			}

			rf.mu.Lock()
			// TODO: rewrite committedTo and appliedTo with if.
			applied := max(rf.log.applied, newCommittedEntries[len(newCommittedEntries)-1].Index)
			rf.log.appliedTo(applied)

		} else {
			fmt.Printf("N%v waits\n", rf.me)
			rf.claimToBeApplied.Wait()
			fmt.Printf("N%v awakes\n", rf.me)
		}
	}
	// warning: this unlock is necessary.
	// assume the committer thread just awakes up from `Wait` and grabs the lock.
	// if happens the raft peer is killed and hence the committer will go out of the loop.
	// if there's no such a unlock, then the lock is held by a killed raft peer.
	// It seems a restart will reuse the same lock. Therefore, all subsequent locking operations
	// will block forever since a killed raft peer holds the lock.
	// I don't know what is actually under the hood, but problem arises by not using this unlock.
	rf.mu.Unlock()
}
