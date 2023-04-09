package raft

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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) committer() {
	rf.mu.Lock()
	for !rf.killed() {
		if newCommittedEntries := rf.log.newCommittedEntries(); len(newCommittedEntries) > 0 {
			rf.mu.Unlock()

			for _, entry := range newCommittedEntries {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Data, CommandIndex: int(entry.Index)}
			}

			rf.mu.Lock()
			rf.log.appliedTo(newCommittedEntries[len(newCommittedEntries)-1].Index)

		} else {
			rf.hasNewCommittedEntries.Wait()
		}
	}
}
