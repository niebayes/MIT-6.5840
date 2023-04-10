package raft

type RequestVoteArgs struct {
	From         int
	To           int
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	From    int
	To      int
	Term    uint64
	VotedTo int
}

type AppendEntriesArgs struct {
	From           int
	To             int
	Term           uint64
	CommittedIndex uint64
	PrevLogIndex   uint64
	PrevLogTerm    uint64
	Entries        []Entry
}

type Err int

const (
	Rejected Err = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	From               int
	To                 int
	Term               uint64
	Err                Err
	LastLogIndex       uint64
	ConflictTerm       uint64
	FirstConflictIndex uint64
}
