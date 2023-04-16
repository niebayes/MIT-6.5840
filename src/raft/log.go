package raft

import (
	"errors"
	"fmt"
)

var ErrOutOfBound = errors.New("index out of bound")

type Entry struct {
	Index uint64
	Term  uint64
	Data  interface{}
}

type Snapshot struct {
	Data  []byte
	Index uint64
	Term  uint64
}

// Log manages log entries, its struct look like:
//
//	     snapshot/first.....applied....committed.....last
//	-------------|--------------------------------------|
//	  compacted           persisted log entries
type Log struct {
	// compacted log entries.
	snapshot           Snapshot
	hasPendingSnapshot bool // true if the snapshot is not yet delivered to the application.

	// persisted log entries.
	entries []Entry

	// TODO: rename applied with delivered. Update comments and docs as well.
	applied   uint64 // the highest log index of the log entry raft knows that the application has applied.
	committed uint64 // the highest log index of the log entry raft knows that the raft cluster has committed.

	logger *Logger
}

func makeLog() Log {
	log := Log{
		snapshot:           Snapshot{Data: nil, Index: 0, Term: 0},
		hasPendingSnapshot: false,
		entries:            make([]Entry, 1),
		applied:            0,
		committed:          0,
	}

	log.setDummy()
	return log
}

func (log *Log) setDummy() {
	log.entries[0].Index = log.snapshot.Index
	log.entries[0].Term = log.snapshot.Term
}

func (log *Log) toArrayIndex(index uint64) uint64 {
	// warning: be careful of integer underflow. (No way to occur in my implementation)
	return index - log.firstIndex()
}

// always return the snapshot index.
func (log *Log) firstIndex() uint64 {
	return log.entries[0].Index
}

// return the index of the last log entry that has not yet been compacted, if there're any.
// otherwise, return the snapshot index.
func (log *Log) lastIndex() uint64 {
	return log.entries[len(log.entries)-1].Index
}

func (log *Log) term(index uint64) (uint64, error) {
	if index < log.firstIndex() || index > log.lastIndex() {
		return 0, ErrOutOfBound
	}

	index = log.toArrayIndex(index)
	return log.entries[index].Term, nil
}

func (log *Log) clone(entries []Entry) []Entry {
	cloned := make([]Entry, len(entries))
	copy(cloned, entries)
	return cloned
}

func (log *Log) slice(start, end uint64) ([]Entry, error) {
	if start == end {
		return nil, nil
	}

	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)
	return log.clone(log.entries[start:end]), nil
}

func (log *Log) truncateSuffix(index uint64) bool {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return false
	}

	index = log.toArrayIndex(index)
	if len(log.entries[index:]) > 0 {
		log.logger.discardEnts(log.entries[index:])
		log.entries = log.entries[:index]
		return true
	}
	return false
}

func (log *Log) append(entries []Entry) {
	log.entries = append(log.entries, entries...)
	log.logger.appendEnts(entries)
}

func (log *Log) committedTo(index uint64) {
	if index > log.committed {
		oldCommitted := log.committed
		log.committed = index
		log.logger.updateCommitted(oldCommitted)
	}
}

func (log *Log) newCommittedEntries() []Entry {
	start := log.toArrayIndex(log.applied + 1)
	end := log.toArrayIndex(log.committed + 1)
	if start >= end {
		// note: len(nil slice) == 0.
		return nil
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) appliedTo(index uint64) {
	if index > log.applied {
		oldApplied := log.applied
		log.applied = index
		log.logger.updateApplied(oldApplied)
	}
}

func (log *Log) compactedTo(snapshot Snapshot) {
	suffix := make([]Entry, 0)
	suffixStart := snapshot.Index + 1
	if suffixStart <= log.lastIndex() {
		suffixStart = log.toArrayIndex(suffixStart)
		fmt.Printf("suffixStart=%v FI=%v LI=%v SI=%v\n", suffixStart, log.firstIndex(), log.lastIndex(), snapshot.Index)
		suffix = log.entries[suffixStart:]
	}

	log.entries = append(make([]Entry, 1), suffix...)
	log.snapshot = snapshot
	log.setDummy()

	log.committedTo(log.snapshot.Index)
	log.appliedTo(log.snapshot.Index)

	lastLogIndex := log.lastIndex()
	lastLogTerm, _ := log.term(lastLogIndex)
	log.logger.compactedTo(lastLogIndex, lastLogTerm)
}

// FIXME: doubt the clone is necessary for working around races.
func (log *Log) clonedSnapshot() Snapshot {
	cloned := Snapshot{Data: make([]byte, len(log.snapshot.Data)), Index: log.snapshot.Index, Term: log.snapshot.Term}
	copy(cloned.Data, log.snapshot.Data)
	return cloned
}
