package raft

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
			snapshot := rf.log.clonedSnapshot()
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: snapshot.Data, SnapshotIndex: int(snapshot.Index), SnapshotTerm: int(snapshot.Term)}

			rf.mu.Lock()
			rf.log.hasPendingSnapshot = false

		} else if newCommittedEntries := rf.log.newCommittedEntries(); len(newCommittedEntries) > 0 {
			rf.mu.Unlock()

			for _, entry := range newCommittedEntries {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Data, CommandIndex: int(entry.Index)}
			}

			rf.mu.Lock()
			rf.log.appliedTo(newCommittedEntries[len(newCommittedEntries)-1].Index)

		} else {
			rf.claimToBeApplied.Wait()
		}
	}
	rf.mu.Unlock()
}
