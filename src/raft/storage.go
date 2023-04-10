package raft

import (
	"6.5840/labgob"
	"bytes"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.term) == nil && e.Encode(rf.votedTo) == nil && e.Encode(rf.log.entries) == nil && e.Encode(rf.log.committed) == nil && e.Encode(rf.log.applied) == nil {
		raftstate := w.Bytes()
		rf.persister.Save(raftstate, nil)

		rf.logger.persist()

	} else {
		panic("failed to encode some fields")
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil || d.Decode(&rf.votedTo) != nil || d.Decode(&rf.log.entries) != nil || d.Decode(&rf.log.committed) != nil || d.Decode(&rf.log.applied) != nil {
		panic("failed to decode some fields")
	}

	rf.logger.restore()
}
