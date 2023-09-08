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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"strconv"

	//	"bytes"
	//"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Role = byte

const (
	follower  = byte(0)
	candidate = byte(1)
	leader    = byte(2)
)

const (
	persistState = iota
	persistAll
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	muLog     sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	receiveHeartbeat bool
	ConsumerStopped  bool
	freshLeader      bool
	t0               time.Time
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	log              []raftLog
	mrole            Role

	//persistence
	currentTerm   int
	votedFor      int
	logFirstIndex int
	snapshot      []byte
	snapshotTerm  int
	snapshotIndex int

	//timeout
	chticker          chan struct{}
	chVoteWin         chan struct{}
	chHeartbeatTicker chan struct{}
	chStop            chan struct{}
	chPersist         chan int

	chApplier chan ApplyMsg
}

type raftLog struct {
	Term int
	Cmd  interface{}
}

type rpclog struct {
	Log   raftLog
	Index int
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.mrole == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persistTicker() {
	for rf.killed() == false {
		select {
		case <-rf.chStop:
			Debug2(dTimer, "S%d Persist Exit ", rf.me)
			return
		case p := <-rf.chPersist:
			wg := sync.WaitGroup{}
			wg.Add(1)
			cht := make(chan struct{}, 2)
			go func(pp *int, w *sync.WaitGroup) {
				count := 1
				for {
					select {
					case <-cht:
						wg.Done()
						Debug2(dPersist, "S%d Persist Req Count:%d ", rf.me, count)
						return
					case n := <-rf.chPersist:
						count++
						if n == persistAll {
							p = persistAll
						}
					}
				}
			}(&p, &wg)
			time.Sleep(10 * time.Millisecond)
			cht <- struct{}{}
			wg.Wait()

			if p == persistAll {
				rf.persistStateAndSnapshot()
			} else if p == persistState {
				rf.persist()
			} else {
				Debug2(dWarn, "S%d Wrong Persist Int %d ", rf.me, p)
			}
		}
	}
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.log)
	if err != nil {
		//fmt.Printf("persist log error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		//fmt.Printf("persist votedFor error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.currentTerm)
	if err != nil {
		//fmt.Printf("persist currentTerm error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.logFirstIndex)
	if err != nil {
		//fmt.Printf("persist logFirstIndex error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.snapshotTerm)
	if err != nil {
		//Debug2(dWarn,"S%d Persist Encode snapshotTerm")
		return
	}
	var logtoPrint []raftLog
	if len(rf.log) > 0 {
		logtoPrint = append(logtoPrint, rf.log[0])
		if len(rf.log) > 1 {
			logtoPrint = append(logtoPrint, rf.log[len(rf.log)-1])
		}
	}
	Debug2(dPersist, "S%d Saved State T:%d VF:%d FI:%d LL:%d ST:%d SI:%d LOG:%v ",
		rf.me, rf.currentTerm, rf.votedFor, rf.logFirstIndex, len(rf.log), rf.snapshotTerm, rf.snapshotIndex, logtoPrint)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.log)
	if err != nil {
		//fmt.Printf("persist log error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		//fmt.Printf("persist votedFor error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.currentTerm)
	if err != nil {
		//fmt.Printf("persist currentTerm error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.logFirstIndex)
	if err != nil {
		//fmt.Printf("persist logFirstIndex error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.snapshotTerm)
	if err != nil {
		//fmt.Printf("persist snapshotTerm error.err=%v \n", err)
		return
	}
	snapdata := make([]byte, len(rf.snapshot))
	copy(snapdata, rf.snapshot)
	Debug2(dPersist, "S%d Saved All T:%d VF:%d FI:%d LL:%d ST:%d SI:%d ",
		rf.me, rf.currentTerm, rf.votedFor, rf.logFirstIndex, len(rf.log), rf.snapshotTerm, rf.snapshotIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapdata)
}

func (rf *Raft) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotIndex int
	if d.Decode(&snapshotIndex) != nil {
		//fmt.Printf("decode error!\n")
	} else {
		rf.mu.Lock()
		rf.snapshot = data
		rf.snapshotIndex = snapshotIndex - 1

		//var cmds []interface{}

		//if len(xlog) > 0 {
		//	cmds = append(cmds, xlog[0])
		//	if len(xlog) > 1 {
		//		cmds = append(cmds, xlog[len(xlog)-1])
		//	}
		//}

		Debug2(dPersist, "S%d Read Snap T:%d SI:%d ", rf.me, rf.currentTerm, rf.snapshotIndex)
		rf.mu.Unlock()
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []raftLog
	var votedfor int
	var term int
	var firstIdx int
	var snapterm int
	if d.Decode(&logs) != nil ||
		d.Decode(&votedfor) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&firstIdx) != nil ||
		d.Decode(&snapterm) != nil {
		//fmt.Printf("decode error!\n")
	} else {
		rf.mu.Lock()
		rf.log = logs
		rf.votedFor = votedfor
		rf.currentTerm = term
		rf.logFirstIndex = firstIdx
		rf.snapshotTerm = snapterm

		var logtoPrint []raftLog
		if len(rf.log) > 0 {
			logtoPrint = append(logtoPrint, rf.log[0])
			if len(rf.log) > 1 {
				logtoPrint = append(logtoPrint, rf.log[len(rf.log)-1])
			}
		}

		Debug2(dPersist, "S%d Read State T:%d VF:%d FI:%d LL:%d ST:%d SI:%d LOG:%v ",
			rf.me, rf.currentTerm, rf.votedFor, rf.logFirstIndex, len(rf.log), rf.snapshotTerm, rf.snapshotIndex, logtoPrint)
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//w := bytes.NewBuffer(snapshot)
	//e := labgob.NewDecoder(w)
	//var i int
	//var xlog []interface{}
	//e.Decode(&i)
	//e.Decode(&xlog)

	rf.mu.Lock()
	if index-1 >= rf.logFirstIndex {
		rf.snapshot = snapshot
		rf.snapshotIndex = index - 1
		rf.snapshotTerm = rf.log[index-1-rf.logFirstIndex].Term
		rf.log = rf.log[index-1-rf.logFirstIndex+1:]
		rf.logFirstIndex = index
		//var cmds []interface{}
		//if len(xlog) > 0 {
		//	cmds = append(cmds, xlog[0])
		//	if len(xlog) > 1 {
		//		cmds = append(cmds, xlog[len(xlog)-1])
		//	}
		//}

		Debug2(dSnap, "S%d Compact Snap SI=%d ST=%d ", rf.me, rf.snapshotIndex, rf.snapshotTerm)
		rf.persistStateAndSnapshot()
	}
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []raftLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	ConflicTerm    int
	TermFirstIndex int
	LogLen         int
}

func (rf *Raft) PersistState(changed *bool) {
	if !(*changed) {
		return
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LogLen = len(rf.log) + rf.logFirstIndex
	reply.ConflicTerm = -1
	reply.TermFirstIndex = -1
	logCount := len(args.Entries)
	stateChanged := new(bool)
	*stateChanged = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		if rf.mrole == leader {
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
		}
		rf.mrole = follower
		rf.currentTerm = args.Term
		*stateChanged = true
	}
	rf.chticker <- struct{}{}

	Debug2(dLog, "S%d Receive Log, Args = [PrevIdx:%d PrevTerm:%d Len:%d] MyState = LogLen:%d FI:%d Logs:%v ",
		rf.me, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.LogLen-rf.logFirstIndex, rf.logFirstIndex, rf.log)
	//check prevlog
	if args.PrevLogIndex == -1 && logCount == 0 {
		reply.Success = true
		if *stateChanged == true {
			rf.persist()
		}
		return
	}

	idx := args.PrevLogIndex - rf.logFirstIndex

	if idx < -1 {
		//fmt.Printf("follower'log > leader's! args.PrevLogIndex = %d,rf.logFirstIndex=%d \n", args.PrevLogIndex, rf.logFirstIndex)
		if *stateChanged == true {
			rf.persist()
		}
		return
	} else if idx >= len(rf.log) {
		if *stateChanged == true {
			rf.persist()
		}
		return
	} else if idx == -1 && args.PrevLogTerm != rf.snapshotTerm {
		if *stateChanged == true {
			rf.persist()
		}
		return
	}

	if idx >= 0 && rf.log[idx].Term != args.PrevLogTerm {
		reply.ConflicTerm = rf.log[idx].Term
		i := idx
		for {
			if i == 0 || rf.log[i-1].Term != reply.ConflicTerm {
				break
			}
			i--
		}
		reply.TermFirstIndex = i
		if *stateChanged == true {
			rf.persist()
		}
		return
	}
	reply.Success = true
	//delete conflict log
	newLog := logCount
	logToCheck := len(rf.log) - idx - 1 // idx's range:[-1:len(rf.log))
	if newLog < logToCheck {
		logToCheck = newLog
	}
	start := idx + 1
	i := 0
	for ; i < logToCheck; i++ {
		if rf.log[start+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:start+i]
			*stateChanged = true
			break
		}
	}

	//append log
	if i < newLog {
		lens := len(rf.log)
		rf.log = append(rf.log, args.Entries[i:]...)
		Debug2(dLog, "S%d Copy Log FI:%d START:%d END:%d LOG SIZE:%d LOG:%v ", rf.me, rf.logFirstIndex, rf.logFirstIndex+lens, rf.logFirstIndex+lens+len(args.Entries)-i-1, len(args.Entries)-i, rf.log)
		*stateChanged = true
	}

	//update commit index
	if args.LeaderCommit > rf.commitIndex {
		cmdi := rf.commitIndex
		rf.commitIndex = args.PrevLogIndex + newLog
		if args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
		Debug2(dCommit, "S%d Commit Change %d->%d ", rf.me, cmdi, rf.commitIndex)
	}
	if *stateChanged == true {
		rf.persist()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	stateChanged := new(bool)
	*stateChanged = false

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	llog := make([]raftLog, 0)
	if len(rf.log) > 0 {
		llog = append(llog, rf.log[len(rf.log)-1])
	}
	Debug2(dVote, "S%d Receive Elect.Args=[Candidate:S%d T:%d LastLog:[idx:%d,term:%d]] MyState = T:%d FI:%d ST:%d LastLog=%v ",
		rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.currentTerm, rf.logFirstIndex, rf.snapshotTerm, llog)

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.mrole = follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		*stateChanged = true
	}
	//check log
	lenl := len(rf.log)
	if lenl > 0 {
		switch rf.log[lenl-1].Term == args.LastLogTerm {
		case true:
			if args.LastLogIndex < rf.logFirstIndex+lenl-1 {
				if *stateChanged == true {
					rf.persist()
				}
				return
			}
		case false:
			if args.LastLogTerm < rf.log[lenl-1].Term {
				if *stateChanged == true {
					rf.persist()
				}
				return
			}
		}
	} else if rf.snapshotIndex >= 0 {
		switch rf.snapshotTerm == args.LastLogTerm {
		case true:
			if args.LastLogIndex < rf.snapshotIndex {
				if *stateChanged == true {
					rf.persist()
				}
				return
			}
		case false:
			if args.LastLogTerm < rf.snapshotTerm {
				if *stateChanged == true {
					rf.persist()
				}
				return
			}
		}
	}

	//vote for it
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		Debug2(dVote, "S%d Vote For S%d ", rf.me, args.CandidateId)
		rf.chticker <- struct{}{}
	}
	if *stateChanged == true {
		rf.persist()
	}
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.chticker <- struct{}{}
	if args.Term > rf.currentTerm {
		if rf.mrole == leader {
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
		}
		rf.mrole = follower
		rf.currentTerm = args.Term
	}
	Debug2(dSnap, "S%d Install Snap SI=%d ST=%d MyState = FI:%d LogLen:%d ",
		rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.logFirstIndex, len(rf.log))
	if rf.snapshotIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm

	lens := len(rf.log)

	if args.LastIncludedIndex < lens+rf.logFirstIndex && rf.log[args.LastIncludedIndex-rf.logFirstIndex].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex-rf.logFirstIndex+1:]
	} else {
		rf.log = []raftLog{}
	}

	rf.logFirstIndex = args.LastIncludedIndex + 1
	rf.persistStateAndSnapshot()
	rf.mu.Unlock()

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
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

	// Your code here (2B).
	rf.mu.Lock()
	index := len(rf.log) + rf.logFirstIndex
	term := rf.currentTerm
	isLeader := rf.mrole == leader

	if isLeader {
		Debug2(dLeader, "S%d Start(%v) CmdIndex:%d FI:%d LogLen:%d Logs:%v ", rf.me, command, index, rf.logFirstIndex, len(rf.log), rf.log)
		rf.log = append(rf.log, raftLog{rf.currentTerm, command})
		rf.persist()
		//wait for persist
		if rf.freshLeader {
			rf.freshLeader = false
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		} else {
			rf.mu.Unlock()
		}
		go func() {
			rf.chHeartbeatTicker <- struct{}{}
		}()
	} else {
		rf.mu.Unlock()
	}

	return index + 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Debug2(dInfo, "S%d Killed ", rf.me)
	close(rf.chStop)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) startElection(t0 int) {
	rf.chticker <- struct{}{}
	rf.mu.Lock()
	if rf.mrole == leader || t0 != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.mrole = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	Debug2(dTimer, "S%d Election Term:%d ", rf.me, rf.currentTerm)
	term := rf.currentTerm
	me := rf.me
	lenpeers := len(rf.peers)
	lastLogTerm := 0
	lastLogIndex := len(rf.log) - 1 + rf.logFirstIndex
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else if rf.snapshotIndex >= 0 {
		lastLogTerm = rf.snapshotTerm
		lastLogIndex = rf.snapshotIndex
	}
	args := RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}
	rf.persist()
	rf.mu.Unlock()
	votecount := 1
	imnew := true
	finished := 0
	majarity := lenpeers/2 + 1
	ch := make(chan int, 10)
	chStopElect := make(chan struct{})
	idx, newterm := -1, term
	var updatemu sync.Mutex
	for i := 0; i < lenpeers; i++ {
		if i == me {
			continue
		}
		server := i
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				ch <- 0
				return
			}
			if reply.Term > term {
				updatemu.Lock()
				if reply.Term > newterm {
					idx = i
					newterm = reply.Term
				}
				updatemu.Unlock()
				ch <- -1
			} else if reply.VoteGranted {
				ch <- 1
			} else {
				ch <- 0
			}
		}()
	}
	for votecount < majarity && finished < lenpeers-1 {
		select {
		case <-rf.chStop:
			return
		case v := <-ch:
			switch v {
			case -1:
				imnew = false
			case 1:
				votecount++
			}
			finished++
		}
	}
	close(chStopElect)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return
	}
	if !imnew && newterm > rf.currentTerm {
		rf.mrole = follower
		rf.votedFor = idx
		rf.currentTerm = newterm
		rf.persist()
	} else if votecount >= majarity && rf.votedFor == rf.me && rf.mrole == candidate {
		rf.mrole = leader
		for i := 0; i < lenpeers; i++ {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.log) + rf.logFirstIndex
		}
		rf.t0 = time.Now()
		rf.chVoteWin <- struct{}{}
		//rf.log = append(rf.log, raftLog{rf.currentTerm, 1})
		rf.persist()
		Debug2(dCandidate, "S%d Wins Term:%d Commit:%d FI%d LogLen:%d Logs:%v ",
			rf.me, rf.currentTerm, rf.commitIndex, rf.logFirstIndex, len(rf.log), rf.log)
		rf.freshLeader = true
	}
	return
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func GetGid() (gid uint64) {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		panic(err)
	}
	return n
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

//return false if it is not leader or cannot contact to majarity of followers
func (rf *Raft) SendHeartbeat() bool {
	if rf.mrole != leader {
		return false
	}
	res := false
	flag := false
	rf.mu.Lock()
	term := rf.currentTerm
	commit := rf.commitIndex
	rf.mu.Unlock()
	t0 := time.Now()
	ch := make(chan AppendMsg, 10)
	go func(res *bool, mFlag *bool) {
		peerCount := 1
		majarity := len(rf.peers)/2 + 1
		finished := 1
		for peerCount < majarity && finished != len(rf.peers) {
			select {
			case msg := <-ch:
				finished++
				if msg.rpcFailed {
					continue
				}
				peerCount++
				rf.mu.Lock()
				if msg.reply.Term > rf.currentTerm {
					rf.mrole = follower
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					rf.currentTerm = msg.reply.Term
					rf.chticker <- struct{}{}
					rf.persist()
					rf.mu.Unlock()
					*mFlag = true
					return
				}
				rf.mu.Unlock()
			case <-time.After(500 * time.Millisecond):
				*mFlag = true
				return
			}
		}
		*mFlag = true
		if peerCount >= majarity {
			*res = true
		}
	}(&res, &flag)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		prevTerm := 0
		nextI := rf.nextIndex[i]
		if len(rf.log) > 0 && rf.nextIndex[i] > rf.logFirstIndex {
			prevTerm = rf.log[rf.nextIndex[i]-1-rf.logFirstIndex].Term
		} else if rf.nextIndex[i] == rf.logFirstIndex {
			prevTerm = rf.snapshotTerm
		}
		rf.mu.Unlock()
		go func(t int, c int, ni int, n int) {
			args := AppendEntriesArgs{term, rf.me, ni - 1,
				t, nil, c}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(n, &args, &reply)
			msg := AppendMsg{}
			msg.peerIdx = n
			msg.rpcFailed = false
			msg.nextIdx = ni
			msg.reply = reply
			if !ok {
				msg.rpcFailed = true
			}
			select {
			case ch <- msg:
				return
			case <-time.After(100 * time.Millisecond):
				return
			}
		}(prevTerm, commit, nextI, i)
	}
	for time.Since(t0).Milliseconds() < 800 && flag == false {
		time.Sleep(50 * time.Millisecond)
	}
	return res
}

type AppendMsg struct {
	rpcFailed    bool
	sentSnapshot bool
	nextIdx      int
	peerIdx      int
	entriesLen   int
	reply        AppendEntriesReply
}

func (rf *Raft) handleAppendRely(t int, ch chan AppendMsg, chs chan struct{}) {
	for rf.killed() == false {
		select {
		case _, closed := <-chs:
			if !closed {
				return
			}
			log.Fatal("closed chStop but receive msg \n")
		case msg := <-ch:
			if t != rf.currentTerm || rf.mrole != leader {
				return
			}
			if msg.rpcFailed {
				continue
			}
			rf.mu.Lock()
			if msg.reply.Term > rf.currentTerm {
				Debug2(dLeader, "S%d Receive Higher Term(%d>%d) From S%d.Converting to Follower ", rf.me, msg.reply.Term, rf.currentTerm, msg.peerIdx)
				rf.mrole = follower
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				rf.currentTerm = msg.reply.Term
				rf.chticker <- struct{}{}
				rf.persist()
				rf.mu.Unlock()
				return
			}
			Debug2(dLeader, "S%d Update LeaderState FollowerReply=[S%d Success:%v IsSnap:%v LogLen:%d NI:%d Conflict:%d TFI:%d] CurNi:%d FI:%d Log:%v ",
				rf.me, msg.peerIdx, msg.reply.Success, msg.sentSnapshot, msg.reply.LogLen, msg.nextIdx, msg.reply.ConflicTerm, msg.reply.TermFirstIndex, rf.nextIndex[msg.peerIdx], rf.logFirstIndex, rf.log)
			if msg.sentSnapshot {
				rf.matchIndex[msg.peerIdx] = max(rf.matchIndex[msg.peerIdx], msg.entriesLen)
				rf.nextIndex[msg.peerIdx] = max(rf.nextIndex[msg.peerIdx], msg.entriesLen+1)
				rf.mu.Unlock()
				continue
			}
			if !msg.reply.Success {
				minIdx := min(msg.reply.LogLen, len(rf.log)+rf.logFirstIndex)
				n := msg.nextIdx - 1
				if msg.reply.ConflicTerm != -1 {
					for n >= msg.reply.TermFirstIndex && n < len(rf.log)+rf.logFirstIndex && n-rf.logFirstIndex >= 0 {
						if rf.log[n-rf.logFirstIndex].Term == msg.reply.ConflicTerm || (n == rf.logFirstIndex && rf.snapshotIndex < 0) {
							break
						}
						n--
					}
				}
				minIdx = min(minIdx, n)
				minIdx = min(msg.nextIdx-1, minIdx)
				rf.nextIndex[msg.peerIdx] = min(rf.nextIndex[msg.peerIdx], minIdx)
			} else {
				if msg.entriesLen > 0 {
					rf.nextIndex[msg.peerIdx] = max(msg.nextIdx+msg.entriesLen, rf.nextIndex[msg.peerIdx])
				}
				rf.matchIndex[msg.peerIdx] = max(rf.matchIndex[msg.peerIdx], msg.nextIdx+msg.entriesLen-1)
			}
			cmdi := rf.commitIndex
			for N := rf.commitIndex + 1; N < len(rf.log)+rf.logFirstIndex && N-rf.logFirstIndex >= 0; N++ {
				if rf.log[N-rf.logFirstIndex].Term != rf.currentTerm {
					continue
				}
				count := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						count++
					}
				}
				if count >= (len(rf.peers)/2)+1 {
					rf.commitIndex = N
				}
			}
			if cmdi != rf.commitIndex {
				Debug2(dCommit, "S%d Leader Commit Change %d->%d ", rf.me, cmdi, rf.commitIndex)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppend2(tt int, ch chan AppendMsg, chStop chan struct{}) {
	rf.mu.Lock()
	argsToSend := make([]interface{}, len(rf.peers))
	msgsToSend := make([]AppendMsg, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		nextI := rf.nextIndex[i]
		if nextI < rf.logFirstIndex {
			argsToSend[i] = InstallSnapshotArgs{
				Term:              tt,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Data:              rf.snapshot,
			}
			msgsToSend[i] = AppendMsg{
				rpcFailed:    false,
				sentSnapshot: true,
				nextIdx:      nextI,
				peerIdx:      i,
				entriesLen:   rf.snapshotIndex,
			}
		} else {
			prevTerm := -1
			msg := AppendMsg{
				rpcFailed:    false,
				sentSnapshot: false,
				nextIdx:      nextI,
				peerIdx:      i,
				entriesLen:   0,
			}
			arg := AppendEntriesArgs{tt, rf.me, nextI - 1,
				prevTerm, nil, rf.commitIndex}
			if nextI >= rf.logFirstIndex+len(rf.log) || nextI < rf.logFirstIndex {
				if len(rf.log) > 0 && nextI > rf.logFirstIndex {
					prevTerm = rf.log[nextI-1-rf.logFirstIndex].Term
				} else if nextI == rf.logFirstIndex {
					prevTerm = rf.snapshotTerm
				}
				arg.PrevLogTerm = prevTerm
			} else {
				arg.Entries = rf.log[nextI-rf.logFirstIndex : len(rf.log)]
				if nextI > rf.logFirstIndex {
					arg.PrevLogTerm = rf.log[nextI-1-rf.logFirstIndex].Term
				} else if nextI == rf.logFirstIndex {
					arg.PrevLogTerm = rf.snapshotTerm
				}
			}
			argsToSend[i] = arg
			msg.entriesLen = len(arg.Entries)
			msgsToSend[i] = msg
		}
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(n int) {
			switch argsToSend[n].(type) {
			case AppendEntriesArgs:
				args := argsToSend[n].(AppendEntriesArgs)
				reply := AppendEntriesReply{}
				Debug2(dLeader, "S%d -> S%d Append: Args=[T:%d PrevLogIndex:%d PrevLogTerm:%d Entries:%v] ",
					rf.me, n, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
				ok := rf.sendAppendEntries(n, &args, &reply)
				if !ok {
					msgsToSend[n].rpcFailed = true
				}
				msgsToSend[n].reply = reply
				select {
				case ch <- msgsToSend[n]:
					return
				case _, closed := <-chStop:
					if !closed {
						return
					}
				}
			case InstallSnapshotArgs:
				args := argsToSend[n].(InstallSnapshotArgs)
				reply := InstallSnapshotReply{}
				Debug2(dLeader, "S%d -> S%d SnapInstall: Args=[T:%d SI%d ST:%d] ",
					rf.me, n, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
				ok := rf.sendInstallSnapshot(n, &args, &reply)
				if !ok {
					msgsToSend[n].rpcFailed = true
				}
				msgsToSend[n].reply.Term = reply.Term
				select {
				case ch <- msgsToSend[n]:
					return
				case _, closed := <-chStop:
					if !closed {
						return
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) run2() {
	for rf.killed() == false {
		select {
		case <-rf.chVoteWin:
		case <-rf.chStop:
			return
		}
		tt := rf.currentTerm
		ch := make(chan AppendMsg, 10)
		chStop := make(chan struct{})
		created := false
		for rf.mrole == leader {
			if !created {
				created = true
				go rf.handleAppendRely(tt, ch, chStop)
			}
			if rf.killed() == true {
				break
			}
			Debug2(dLeader, "S%d Leader State= T:%d Commit:%d LogLen:%d Matches:%v NextIdx:%v SI:%d FI:%d ",
				rf.me, rf.currentTerm, rf.commitIndex, len(rf.log), rf.matchIndex, rf.nextIndex, rf.snapshotIndex, rf.logFirstIndex)
			go rf.sendAppend2(tt, ch, chStop)
			select {
			case <-rf.chHeartbeatTicker:
				continue
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		close(chStop)
		rf.chticker <- struct{}{}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleeptime := rand.Int31() % 400
		//t := rf.currentTerm
		select {
		case <-rf.chticker:
			//fmt.Println("RESET election timeout！")
		case <-time.After(time.Millisecond * (time.Duration(sleeptime) + 400)):
			//fmt.Printf("%d election time out!!\n", rf.me)
			go rf.startElection(rf.currentTerm)
		case <-rf.chStop:
			return
		}

	}
}
func (rf *Raft) msgBufferConsumer(buffer chan ApplyMsg, applyCh chan ApplyMsg) {
	for rf.killed() == false {
		select {
		case m := <-buffer:
			Debug2(dLog2, "S%d Consume Buffer Index:%d SI:%d CMD:%v ", rf.me, m.CommandIndex, m.SnapshotIndex, m.Command)
			applyCh <- m
		case <-rf.chStop:
			rf.ConsumerStopped = true
			return
		}
	}
	rf.ConsumerStopped = true
}

func (rf *Raft) applier2(applyCh chan ApplyMsg) {
	msgBuffer := make(chan ApplyMsg, 500)
	go rf.msgBufferConsumer(msgBuffer, applyCh)
	for rf.killed() == false {
		//rf.mu.Lock()
		if rf.lastApplied < rf.snapshotIndex {
			//rf.mu.Unlock()
			msg := ApplyMsg{}
			msg.SnapshotValid = true
			msg.CommandValid = false
			rf.mu.Lock()
			msg.SnapshotTerm = rf.snapshotTerm
			msg.SnapshotIndex = rf.snapshotIndex + 1
			msg.Snapshot = rf.snapshot
			rf.mu.Unlock()
			Debug2(dApply, "S%d Apply Snap Args=[SI:%d ST:%d ] ", rf.me, rf.snapshotIndex, rf.snapshotTerm)
			select {
			case msgBuffer <- msg:
			case <-rf.chStop:
				return
			}
			rf.lastApplied = rf.snapshotIndex
		} else if rf.commitIndex >= rf.logFirstIndex && rf.commitIndex < len(rf.log)+rf.logFirstIndex && rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			comIdx := rf.commitIndex
			firstidx := rf.logFirstIndex
			lastApp := rf.lastApplied
			count := 0
			for comIdx > lastApp && count < 150 {
				lastApp++
				if lastApp < firstidx || lastApp >= firstidx+len(rf.log) {
					//fmt.Printf("lack of log at %d to apply!my log:%v,first idx:%d \n", rf.lastApplied, logs, firstidx)
					break
				}
				count++
				msg := ApplyMsg{true, rf.log[lastApp-firstidx].Cmd, lastApp + 1, false, nil, -1, -1}
				Debug2(dApply, "S%d Apply Args=[Cmd:%v Index:%d ] ", rf.me, msg.Command, lastApp)
				select {
				case msgBuffer <- msg:
				case <-rf.chStop:
					return
				}
				rf.lastApplied = lastApp
			}
			rf.mu.Unlock()
		} else {
			//rf.mu.Unlock()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

//
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.logFirstIndex = 0
	rf.snapshotTerm = -1
	rf.snapshotIndex = -1
	rf.receiveHeartbeat = false
	rf.ConsumerStopped = false
	rf.freshLeader = false
	rf.chticker = make(chan struct{}, 1)
	rf.chVoteWin = make(chan struct{}, 1)
	rf.chHeartbeatTicker = make(chan struct{}, 50)
	rf.chStop = make(chan struct{}, 1)
	rf.chPersist = make(chan int, 100)
	rf.mrole = follower
	rf.chApplier = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapShot(persister.ReadSnapshot())

	if rf.snapshotIndex >= rf.logFirstIndex {
		if rf.snapshotIndex < rf.logFirstIndex+len(rf.log) {
			rf.log = rf.log[rf.snapshotIndex-rf.logFirstIndex+1:]
		} else {
			rf.log = []raftLog{}
		}
	}
	if rf.snapshotIndex >= 0 {
		rf.lastApplied = rf.snapshotIndex
		rf.commitIndex = rf.snapshotIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.run2()
	go rf.applier2(applyCh)
	return rf
}
