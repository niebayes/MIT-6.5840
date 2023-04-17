package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.term) == nil && e.Encode(rf.votedTo) == nil && e.Encode(rf.log.entries) == nil && e.Encode(rf.log.snapshot.Index) == nil && e.Encode(rf.log.snapshot.Term) == nil {
		raftstate := w.Bytes()
		// warning: since the persister provides a very simple interface, there's no way to not persist
		// raftstate and snapshot together while ensures they're in sync.
		rf.persister.Save(raftstate, rf.log.snapshot.Data)

		rf.logger.persist()

	} else {
		panic("failed to encode some fields")
	}
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil || d.Decode(&rf.votedTo) != nil || d.Decode(&rf.log.entries) != nil || d.Decode(&rf.log.snapshot.Index) != nil || d.Decode(&rf.log.snapshot.Term) != nil {
		panic("failed to decode some fields")
	}

	// warning: on recovery, raft has to also restore the snapshot.
	// that's because a leader might need to send a snapshot to followers after restarted.
	rf.log.compactedTo(Snapshot{Data: rf.persister.ReadSnapshot(), Index: rf.log.snapshot.Index, Term: rf.log.snapshot.Term})

	fmt.Printf("N%v rs (T:%v V:%v LI:%v CI:%v AI:%v SI:%v ST:%v)\n", rf.me, rf.term, rf.votedTo, rf.log.lastIndex(), rf.log.committed, rf.log.applied, rf.log.snapshot.Index, rf.log.snapshot.Term)
	rf.logger.restore()
}
