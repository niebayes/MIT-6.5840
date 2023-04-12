package kvraft

type Err string

const (
	OK             = "OK"
	ErrNotApplied  = "ErrNotApplied"
	ErrWrongLeader = "ErrWrongLeader"
)

type PutAppendArgs struct {
	ClerkId int64
	OpId    int
	OpType  string // "Put" or "Append"
	Key     string
	Value   string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkId int64
	OpId    int
	Key     string
}

type GetReply struct {
	Err   Err
	Value string
}
