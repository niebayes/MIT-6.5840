package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// true to turn on debugging/logging.
const debug = true
const LOGTOFILE = false
const printEnts = false

func (l *Logger) printEnts(topic logTopic, me int, ents []Entry) {
	if printEnts {
		for _, ent := range ents {
			if ent.Index != 0 {
				l.printf(topic, "N%v    (I:%v T:%v D:%v)", me, ent.Index, ent.Term, ent.Data.(int))
				// l.printf(topic, "N%v    (I:%v T:%v)", me, ent.Index, ent.Term)
			}
		}
	}
}

// what topic the log message is related to.
// logs are organized by topics which further consists of events.
type logTopic string

const (
	// the typical route of leader election is:
	// 	becomeFollower
	//		election time out
	//		send MsgHup to self
	//	becomeCandidate
	//  bcastRequestVote
	//		other peers: handleRequestVote
	//			grant vote
	//			deny vote
	//	handleRequestVoteResponse
	//		receive a majority of votes
	// 	becomeLeader
	//		append a noop entry
	//		bcast the noop entry
	ELEC logTopic = "ELEC"

	// the typical route of log replication is:
	//	receive MsgProp
	//		append these log entries
	//		update leader's own progress
	//	bcastAppendEntries
	//		other peers: handleAppendEntries
	//			reject the whole entries due to index conflict or term conflict
	//			accept the whole entries but discard conflicting entries and only append missing entries.
	//	handleAppendEntriesResponse
	//		leader update follower's progress: next index and match index
	//		leader knows which entries are committed
	//	bcastHeartbeat
	//		other peers know which entries are committed
	// 	handleHeartbeatResponse
	//		leader notifys slow followers and send AppendEntries to make them catch up.
	//		...
	//		all alive followers commit the log entries
	//
	LRPE logTopic = "LRPE"

	// heartbeat events:
	// leader heartbeat time out
	// leader broadcast HeartBeat
	// others receive HeartBeat
	// leader receive HeartBeatResponse
	BEAT logTopic = "BEAT"

	// persistence events:
	// restore stable entries from stable storage.
	// restore term, vote, commit from hardstate.
	// restore nodes config from confstate.
	// persist unstable log entrie.
	// update and save hardstate
	// update and save applystate.
	PERS logTopic = "PERS"

	// peer handling events:
	//	start raft module
	//  propose new raft cmd
	//  detect ready raft states.
	//  notify clients stale proposals.
	//  process committed log entry/raft cmd
	//  advance raft state
	PEER logTopic = "PEER"

	// snapshotting events:
	// TODO: add document for log compaction and snapshotting.
	// the typical route of snapshotting is:
	//
	// service sends a snapshot
	// server snapshots
	// leader detects a follower is lagging hebind
	// leader sends InstallSnapshot to lagged follower
	// follower forwards snapshot to service
	// service conditionally installs a snapshot by asking Raft.
	SNAP logTopic = "SNAP"
)

type Logger struct {
	logToFile      bool
	logFile        *os.File
	verbosityLevel int // logging verbosity is controlled over environment verbosity variable.
	startTime      time.Time
	r              *Raft
}

func makeLogger(logToFile bool, logFileName string) *Logger {
	logger := &Logger{}
	logger.init(LOGTOFILE, logFileName)
	return logger
}

func (logger *Logger) init(logToFile bool, logFileName string) {
	logger.logToFile = logToFile
	logger.verbosityLevel = getVerbosityLevel()
	logger.startTime = time.Now()

	// set log config.
	if logger.logToFile {
		logger.setLogFile(logFileName)
	}
	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime)) // not show date and time.
}

func (logger *Logger) setLogFile(filename string) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to create file %v", filename)
	}
	log.SetOutput(f)
	logger.logFile = f
}

func (logger *Logger) printf(topic logTopic, format string, a ...interface{}) {
	// print iff debug is set.
	if debug {
		// time := time.Since(logger.startTime).Milliseconds()
		time := time.Since(logger.startTime).Microseconds()
		// e.g. 008256 VOTE ...
		prefix := fmt.Sprintf("%010d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// not delete this for backward compatibility.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

// retrieve the verbosity level from an environment variable
// VERBOSE=0/1/2/3 <=>
func getVerbosityLevel() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

//
// leader election events.
//

var stmap = [...]string{
	"F", // follower
	"C", // candidate
	"L", // leader
}

func (st PeerState) String() string {
	return stmap[uint64(st)]
}

func (l *Logger) elecTimeout() {
	r := l.r
	l.printf(ELEC, "N%v ETO (S:%v T:%v)", r.me, r.state, r.term)
}

func (l *Logger) stepDown() {
	r := l.r
	l.printf(ELEC, "N%v STD (T:%v)", r.me, r.term)
}

func (l *Logger) stateToCandidate() {
	r := l.r
	l.printf(ELEC, "N%v %v->%v (T:%v)", r.me, r.state, Candidate, r.term)
}

func (l *Logger) bcastRVOT() {
	r := l.r
	l.printf(ELEC, "N%v @ RVOT (T:%v)", r.me, r.term)
}

func (l *Logger) recvRVOT(m *RequestVoteArgs) {
	r := l.r
	l.printf(ELEC, "N%v <- N%v RVOT (T:%v)", r.me, m.From, m.Term)
}

func (l *Logger) voteTo(to int) {
	r := l.r
	l.printf(ELEC, "N%v v-> N%v", r.me, to)
}

var denyReasonMap = [...]string{
	"GRT", // grant the vote.
	"VTD", // I've granted the vote to another one.
	"STL", // you're stale.
}

func (l *Logger) rejectVoteTo(to int, CandidatelastLogIndex, CandidatelastLogTerm, lastLogIndex, lastLogTerm uint64) {
	r := l.r
	l.printf(ELEC, "N%v !v-> N%v (CLI:%v CLT:%v LI:%v LT:%v)", r.me, to,
		CandidatelastLogIndex, CandidatelastLogTerm, lastLogIndex, lastLogTerm)
}

func (l *Logger) recvRVOTRes(m *RequestVoteReply) {
	r := l.r
	l.printf(ELEC, "N%v <- N%v RVOT RES (T:%v V:%v)", r.me, m.From, m.Term, m.Voted)
}

func (l *Logger) recvVoteQuorum() {
	r := l.r
	l.printf(ELEC, "N%v <- VOTE QUORUM (T:%v)", r.me, r.term)
}

func (l *Logger) stateToLeader() {
	r := l.r
	l.printf(ELEC, "N%v %v->%v (T:%v)", r.me, r.state, Leader, r.term)
}

func (l *Logger) stateToFollower(oldTerm uint64) {
	r := l.r
	l.printf(ELEC, "N%v %v->%v (T:%v) -> (T:%v)", r.me, r.state, Follower, oldTerm, r.term)
}

//
// log replication events.
//

func (l *Logger) appendEnts(ents []Entry) {
	r := l.r
	l.printf(LRPE, "N%v +e (LN:%v)", r.me, len(ents))
}

func (l *Logger) sendEnts(prevLogIndex, prevLogTerm uint64, ents []Entry, to int) {
	r := l.r
	l.printf(LRPE, "N%v e-> N%v (T:%v CI:%v PI:%v PT:%v LN:%v)", r.me, to, r.term, r.log.committed, prevLogIndex, prevLogTerm, len(ents))
	l.printEnts(LRPE, r.me, ents)
}

func (l *Logger) recvAENT(m *AppendEntriesArgs) {
	r := l.r
	l.printf(LRPE, "N%v <- N%v AENT (T:%v CI:%v PI:%v PT:%v LN:%v)", r.me, m.From, m.Term, m.CommittedIndex, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries))
}

type RejectReason int

const (
	Accepted RejectReason = iota
	IndexConflict
	TermConflict
)

var reasonMap = [...]string{
	"NO", // not reject
	"IC", // index conflict.
	"TC", // term conflict.
}

func (l *Logger) rejectEnts(from int) {
	r := l.r
	l.printf(LRPE, "N%v !e<- N%v", r.me, from)
}

func (l *Logger) acceptEnts(from int) {
	r := l.r
	l.printf(LRPE, "N%v &e<- N%v", r.me, from)
}

func (l *Logger) discardEnts(ents []Entry) {
	r := l.r
	l.printf(LRPE, "N%v -e (LN:%v)", r.me, len(ents))
	l.printEnts(LRPE, r.me, ents)
}

var errMap = [...]string{
	"RJ", // rejected.
	"MT", // matched.
	"IN", // index not matched.
	"TN", // term not matched.
}

func (err Err) String() string {
	return errMap[err]
}

func (l *Logger) recvAENTRes(m *AppendEntriesReply) {
	r := l.r
	l.printf(LRPE, "N%v <- N%v AENT RES (T:%v E:%v CT:%v FCI:%v LI:%v)", r.me, m.From, m.Term, errMap[m.Err], m.ConflictTerm, m.FirstConflictIndex, m.LastLogIndex)
}

func (l *Logger) updateProgOf(peer int, oldNext, oldMatch, newNext, newMatch uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^pr N%v (NI:%v MI:%v) -> (NI:%v MI:%v)", r.me, peer, oldNext, oldMatch, newNext, newMatch)
}

func (l *Logger) updateCommitted(oldCommitted uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^ci (CI:%v) -> (CI:%v)", r.me, oldCommitted, r.log.committed)
}

func (l *Logger) updateApplied(oldApplied uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^ai (AI:%v) -> (AI:%v)", r.me, oldApplied, r.log.applied)
}

//
// heartbeat events.
//

func (l *Logger) sendBeat(prevLogIndex, prevLogTerm uint64, to int) {
	r := l.r
	l.printf(LRPE, "N%v b-> N%v (T:%v CI:%v PI:%v PT:%v)", r.me, to, r.term, r.log.committed, prevLogIndex, prevLogTerm)
}

func (l *Logger) recvHBET(m *AppendEntriesArgs) {
	r := l.r
	l.printf(BEAT, "N%v <- N%v HBET (T:%v CI:%v)", r.me, m.From, m.Term, m.CommittedIndex)
}

func (l *Logger) recvHBETRes(m *AppendEntriesReply) {
	r := l.r
	l.printf(LRPE, "N%v <- N%v HBET RES (T:%v E:%v CT:%v FCI:%v LI:%v)", r.me, m.From, m.Term, errMap[m.Err], m.ConflictTerm, m.FirstConflictIndex, m.LastLogIndex)
}

//
// persistence events.
//

func (l *Logger) restore() {
	r := l.r
	l.printf(PERS, "N%v rs (T:%v V:%v LI:%v CI:%v AI:%v SI:%v ST:%v)", r.me, r.term, r.votedTo, r.log.lastIndex(), r.log.committed, r.log.applied, r.log.snapshot.Index, r.log.snapshot.Term)
	if printEnts {
		l.printEnts(PERS, r.me, r.log.entries)
	}
}

func (l *Logger) persist() {
	r := l.r
	l.printf(PERS, "N%v sv (T:%v V:%v LI:%v CI:%v AI:%v SI:%v ST:%v)", r.me, r.term, r.votedTo, r.log.lastIndex(), r.log.committed, r.log.applied, r.log.snapshot.Index, r.log.snapshot.Term)
	if printEnts {
		l.printEnts(PERS, r.me, r.log.entries)
	}
}

//
// snapshot events
//

func (l *Logger) compactedTo(lastLogIndex, lastLogTerm uint64) {
	r := l.r
	l.printf(SNAP, "N%v cp (SI:%v ST:%v LI:%v LT:%v)", r.me, r.log.snapshot.Index, r.log.snapshot.Term, lastLogIndex, lastLogTerm)
}

func (l *Logger) sendISNP(to int, snapshotIndex, snapshotTerm uint64) {
	r := l.r
	l.printf(SNAP, "N%v s-> N%v (SI:%v ST:%v)", r.me, to, snapshotIndex, snapshotTerm)
}

func (l *Logger) recvISNP(m *InstallSnapshotArgs) {
	r := l.r
	l.printf(SNAP, "N%v <- N%v ISNP (SI:%v ST:%v)", r.me, m.From, m.Snapshot.Index, m.Snapshot.Term)
}

func (l *Logger) recvISNPRes(m *InstallSnapshotReply) {
	r := l.r
	l.printf(SNAP, "N%v <- N%v ISNP RES (IS:%v)", r.me, m.From, m.Installed)
}

func (l *Logger) pullSnap(snapshotIndex uint64) {
	r := l.r
	l.printf(SNAP, "N%v pull SNP (SI:%v)", r.me, snapshotIndex)
}

func (l *Logger) pushSnap(snapshotIndex, snapshotTerm uint64) {
	r := l.r
	l.printf(SNAP, "N%v push SNP (SI:%v ST:%v)", r.me, snapshotIndex, snapshotTerm)
}
