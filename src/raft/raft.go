package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

var debugFlag = false

func logDebug(format string, v ...interface{}) {
	if debugFlag {
		log.Printf(format, v...)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// import "bytes"
// import "labgob"

var heartbeatTime int64 = 50 * 1e6
var baseElectionTimeout int64 = 400 * 1e6

// ApplyMsg is the struct to apply a message
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry log entry of logs
type LogEntry struct {
	Term    int
	Command interface{}
}

// AppendEntriesArgs Log Replication and HeartBeat
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

// ReplyAppendEntries is the reply structure of append entry request.
type ReplyAppendEntries struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
	From         int
	ReqType      string
}

// Raft struct
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	CurrentTerm int        // latest term server has seen
	VotedFor    int        // candidateId that received vote in current term
	Logs        []LogEntry // log entries

	// Volatile state on all servers:
	commitIndex int    // index of highest log entry known to be committed.
	lastApplied int    // index of highest log entry applied to state machine.
	role        string // role of this node
	//myVotes     int    // count of leader election votes for this node
	lastTs int64

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to the server
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

	stop    bool
	applyCh chan ApplyMsg

	commitCond *sync.Cond
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	// logDebug("Get state %d\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.role == "l"
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// should be called when lock hold.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.CurrentTerm)
	if err == nil {
		err = e.Encode(rf.VotedFor)
	}
	if err == nil {
		err = e.Encode(rf.Logs)
	}
	if err != nil {
		log.Fatalf("Server %d, read persist failed, error message %v", rf.me, err)
	}

	//logDebug("Server %d, persist, CurrentTerm: %d, VotedFor: %d, logs: %v, err: %v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Logs, err)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var err = d.Decode(&currentTerm)
	if err == nil {
		err = d.Decode(&votedFor)
	}

	if err == nil {
		err = d.Decode(&rf.Logs)
	}

	if err != nil {
		log.Fatalf("readPersist failed: %v\n", err)
	}
	//logDebug("Server %d, logs: %p-%v, length: %d\n", rf.me, rf.Logs, rf.Logs, len(rf.Logs))
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
}

// RequestVoteArgs is the request struct for voting.
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// ReplyVote struct
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type ReplyVote struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	From        int
}

// RequestVote handles request vote from candidate
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *ReplyVote) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stop {
		return
	}
	//logDebug("Server %d: Got vote requst from %d, %v, my term: %d\n", rf.me, args.CandidateID, *args, rf.CurrentTerm)
	if rf.CurrentTerm >= args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		logDebug("Server %d: deny vote request from %d due to larger CurrentTerm\n", rf.me, args.CandidateID)
		//rf.lastTs = time.Now().UnixNano()
		reply.From = rf.me
		return
	}

	rf.CurrentTerm = args.Term
	// rf.VotedFor = -1
	rf.turnToFollower()
	reply.From = rf.me
	reply.VoteGranted = false

	//logDebug("Server %d: &logs: %p, logLength: %d\n", rf.me, &rf.Logs, len(rf.Logs))
	lastLogIndex := len(rf.Logs) - 1
	lastLogTerm := rf.Logs[lastLogIndex].Term

	// the RPC
	// includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
			rf.lastTs = time.Now().UnixNano()
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true
			reply.Term = args.Term
		}
	}
	rf.persist()
}

// must be called when lock is hold
func (rf *Raft) turnToFollower() {
	if rf.role == "l" {
		logDebug("Server %d turnToFollower\n", rf.me)
	}
	rf.role = "f"
	rf.VotedFor = -1
}

/*RequestAppendEntries r
 */
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *ReplyAppendEntries) {
	//logDebug("Server %d, rpc call from peer %d, %v\n", rf.me, args.LeaderID, *args)
	if rf.isStop() {
		logDebug("Server %d, shutdown, reject rpc call\n", rf.me)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.From = rf.me
	reply.ReqType = "heartbeat"
	if rf.CurrentTerm > args.Term /*|| (rf.CurrentTerm == args.Term && rf.VotedFor != args.LeaderID && rf.VotedFor != -1)*/ {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	//logDebug("Server %d, rpc call from peer %d, %v\n", rf.me, args.LeaderID, *args)
	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm

	if rf.VotedFor != args.LeaderID {
		rf.VotedFor = args.LeaderID
	}

	rf.lastTs = time.Now().UnixNano()

	prevLogIdx, prevLogTerm := 0, 0
	if args.PrevLogIndex < len(rf.Logs) {
		prevLogIdx = args.PrevLogIndex
		prevLogTerm = rf.Logs[prevLogIdx].Term
	}

	if prevLogIdx == args.PrevLogIndex && prevLogTerm == args.PrevLogTerm {
		reply.Success = true
		rf.turnToFollower()
		rf.Logs = rf.Logs[:prevLogIdx+1]
		rf.Logs = append(rf.Logs, args.Entries...)
		last := len(rf.Logs) - 1
		if args.LeaderCommit > rf.commitIndex {
			//logDebug("Server %d, args.LeaderCommit %d, rf.commitIndex %d, last %d\n", rf.me, args.LeaderCommit, rf.commitIndex, last)
			rf.commitIndex = min(args.LeaderCommit, last)
			go func() { rf.commitCond.Broadcast() }()
		} else {
			//logDebug("Server %d, commitIndex is not changed: %d\n", rf.me, rf.commitIndex)
		}
		reply.ConflictTerm = rf.Logs[last].Term
		reply.FirstIndex = last

		if len(args.Entries) > 0 {
			//logDebug("[%d]: AE success from leader %d (%d cmd @ %d), commit index: l->%d, f->%d.\n", rf.me, args.LeaderID, len(args.Entries), prevLogIdx+1, args.LeaderCommit, rf.commitIndex)
			reply.ReqType = "AppendEntries"
		} else {
			//logDebug("heartbeat\n")
		}
	} else {
		reply.Success = false
		first := 1
		logDebug("Server %d: index doesn't match, prevLogIdx: %d vs %d, prevLogTerm: %d vs %d\n", rf.me, prevLogIdx, args.PrevLogIndex, prevLogTerm, args.PrevLogTerm)
		reply.ConflictTerm = prevLogTerm
		if reply.ConflictTerm == 0 {
			// leader has more logs or follower has no log at all
			first = len(rf.Logs)
			reply.ConflictTerm = rf.Logs[first-1].Term
		} else {
			idx := prevLogIdx - 1
			for ; idx > 0; idx-- {
				if rf.Logs[idx].Term != prevLogTerm {
					first = idx + 1
					break
				}
			}
		}
		reply.FirstIndex = first
	}
	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *ReplyVote) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *ReplyAppendEntries) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

// Start method is called by client to start an agreement
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//logDebug("Server %d: Start is called", rf.me)
	// Your code here (2B).
	index, term, isLeader := -1, 0, false
	//log.Printf("Server %d: Start, command is %v\n", rf.me, command)
	rf.mu.Lock()

	defer rf.mu.Unlock()
	isLeader = rf.role == "l"
	if !isLeader {
		return index, term, isLeader
	}

	term = rf.CurrentTerm
	//logDebug("Server %d: Start, command is %v\n", rf.me, command)
	//log.Printf("Server %d: Start, command is %v\n", rf.me, command)
	rf.Logs = append(rf.Logs, LogEntry{Term: term, Command: command})
	index = len(rf.Logs) - 1
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	rf.persist()
	go rf.syncData()
	return index, term, isLeader
}

// Kill method stops the current node
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//logDebug("Server %d killed.\n", rf.me)
	rf.stop = true
	rf.commitCond.Broadcast()
}

// Make function
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.stop = false
	rf.applyCh = applyCh

	unixNano := time.Now().UnixNano()
	// logDebug("server %d, nano: %d\n", me, unixNano)
	rand.Seed(unixNano)

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = "f" //initial follower
	rf.VotedFor = -1
	// rf.myVotes = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastTs = 0
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{0, nil} //the first log is just a placeholder

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionDaemon()
	go rf.applyLogEntryDaemon()
	//go rf.syncDataDaemon()

	return rf
}

func (rf *Raft) waitCond() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.commitIndex == rf.lastApplied {
		rf.commitCond.Wait()
	}
}

func (rf *Raft) applyLogEntryDaemon() {
	for {
		if rf.isStop() {
			return
		}
		rf.waitCond()
		if rf.isStop() {
			return
		}

		var logs []LogEntry
		rf.mu.Lock()
		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur {
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, cur-last)
			copy(logs, rf.Logs[last+1:cur+1])
		}
		rf.mu.Unlock()
		logDebug("Server %d, start to apply messages: %v.\n", rf.me, logs)
		for idx := 0; idx < cur-last; idx++ {
			reply := ApplyMsg{
				CommandValid: true,
				CommandIndex: last + idx + 1,
				Command:      logs[idx].Command,
			}
			rf.applyCh <- reply
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	match := make([]int, len(rf.matchIndex))
	copy(match, rf.matchIndex)
	sort.Ints(match)

	target := match[len(rf.peers)/2]
	if rf.commitIndex < target {
		// Read Figure 8 in the Raft extension paper (https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf).
		// The leader in a new term may try to finish replicating log entries that
		// haven't been committed yet.
		if rf.Logs[target].Term == rf.CurrentTerm {
			logDebug("[%d]: Server %d update commit index %d -> %d @ term %d\n", rf.me, rf.me, rf.commitIndex, target, rf.CurrentTerm)
			rf.commitIndex = target
			go func() { rf.commitCond.Broadcast() }()
		} else {
			log.Printf("[%d]: Server %d update commit index %d failed (log term %d != current Term %d)\n", rf.me, rf.me, rf.commitIndex, rf.Logs[target].Term, rf.CurrentTerm)
			//logDebug("[%d]: Server %d update commit index %d failed (log term %d != current Term %d)\n", rf.me, rf.me, rf.commitIndex, rf.Logs[target].Term, rf.CurrentTerm)
		}
	}
}

func (rf *Raft) syncDataForServer(server int) {

	syncDataHandler := func(req *AppendEntriesArgs, reply *ReplyAppendEntries) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != "l" {
			return
		}

		//logDebug("Leader %d, Got ReplyAppendEntries from %d: %v\n", rf.me, reply.From, reply)
		if reply.Success {
			rf.matchIndex[server] = reply.FirstIndex
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.updateCommitIndex() // try to update commitIndex
		}

		if (!reply.Success) && reply.Term > req.Term {
			logDebug("Leader %d, turnToFollower due to larger term from follower %d\n", rf.me, server)
			rf.turnToFollower()
			return
		}

		if !reply.Success {
			know, lastIndex := false, 0
			if reply.ConflictTerm != 0 {
				for i := len(rf.Logs) - 1; i > 0; i-- {
					if rf.Logs[i].Term == reply.ConflictTerm {
						know = true
						lastIndex = i
						break
					}
				}
				if know {
					rf.nextIndex[server] = min(lastIndex, reply.FirstIndex)
				} else {
					rf.nextIndex[server] = reply.FirstIndex
				}
			} else {
				rf.nextIndex[server] = reply.FirstIndex
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIndex := rf.nextIndex[server]
	prev := nextIndex - 1
	entriesArgs := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.Logs[prev].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	//logDebug("Server %d, rf.Logs has %d elements, nextIndex[%d] is %d\n", rf.me, len(rf.Logs), server, nextIndex)
	if len(rf.Logs) > nextIndex {
		entriesArgs.Entries = append(entriesArgs.Entries, rf.Logs[nextIndex:]...)
	}

	go func() {
		reply := ReplyAppendEntries{}
		ok := rf.sendRequestAppendEntries(server, &entriesArgs, &reply)
		if ok {
			syncDataHandler(&entriesArgs, &reply)
		}
	}()
}

func (rf *Raft) syncData() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != "l" {
		return
	}

	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go rf.syncDataForServer(server)
		}
	}
}

func (rf *Raft) heartbeatDaemon() {
	for true {
		if rf.isStop() {
			return
		}
		rf.syncData()
		time.Sleep(time.Duration(heartbeatTime) * time.Nanosecond)
	}
}

func (rf *Raft) isStop() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.stop
}

func (rf *Raft) electionDaemon() {
	var randns = (int64)(rand.Intn(150) * 1e6)
	for {
		if rf.isStop() {
			return
		}
		time.Sleep(time.Duration(randns) * time.Nanosecond)
		rf.checkLeader(randns)
		sleepns := baseElectionTimeout - randns
		if sleepns > 0 {
			time.Sleep(time.Duration(sleepns) * time.Nanosecond)
		}
	}
}

// should be called when holding the lock
// the raft instance becomes leader
func (rf *Raft) turnToLeader() {
	rf.role = "l"
	length := len(rf.Logs)
	// reset the followers info
	serverCnt := len(rf.peers)
	for idx := 0; idx < serverCnt; idx++ {
		rf.nextIndex[idx] = length
		rf.matchIndex[idx] = 0
		if idx == rf.me {
			rf.matchIndex[idx] = length - 1
		}
	}
}

func (rf *Raft) checkLeader(randns int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	votes := 1
	voteReplyHandler := func(req *RequestVoteArgs, reply *ReplyVote) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		peerLength := len(rf.peers)

		logDebug("server %d, role %s - Got vote reply, %v of req %v\n", rf.me, rf.role, *reply, *req)
		if rf.role != "c" {
			return
		}
		if reply.Term > req.Term {
			rf.CurrentTerm = reply.Term
			rf.turnToFollower()
			rf.lastTs = time.Now().UnixNano()
			return
		}
		if reply.VoteGranted {
			votes++
			// logDebug("Server %d, votes: %d, peerLength: %d\n", rf.me, votes, peerLength)
			if votes > peerLength/2 {
				rf.turnToLeader()
				logDebug("[Server %d]: peer %d become new leader.\n", rf.me, rf.me)
				go rf.heartbeatDaemon()

				return
			}
		}
	}

	now := time.Now().UnixNano()
	if rf.role != "l" && (now-rf.lastTs) > (randns+baseElectionTimeout) {
		rf.CurrentTerm++
		rf.lastTs = now
		rf.role = "c"
		logLength := len(rf.Logs)

		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {

				lastLogIndex := 0
				lastLogTerm := 0
				if logLength != 0 {
					lastLogTerm = rf.Logs[logLength-1].Term
					lastLogIndex = logLength - 1
				}
				CurrentTerm := rf.CurrentTerm

				req := &RequestVoteArgs{CurrentTerm, rf.me, lastLogIndex, lastLogTerm}
				go func(serverIdx int) {
					reply := new(ReplyVote)
					ok := rf.sendRequestVote(serverIdx, req, reply)
					//logDebug("Server %d call %d - end\n", rf.me, serverIdx)
					if ok {
						voteReplyHandler(req, reply)
					}
				}(server)
			}
		}
	}
}
