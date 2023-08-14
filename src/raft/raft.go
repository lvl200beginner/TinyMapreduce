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
	"fmt"
	"math/rand"
	"strconv"

	//	"bytes"
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
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	log              []raftLog
	mrole            Role

	//persistence
	currentTerm int
	votedFor    int

	//timeout
	chticker  chan struct{}
	chVoteWin chan struct{}
}

type raftLog struct {
	Term int
	Cmd  interface{}
}

type rpclog struct {
	Log   raftLog
	Index int
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []raftLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	logCount := len(args.Entries)
	if args.Term < rf.currentTerm {
		return
	}
	rf.chticker <- struct{}{}
	if args.Term > rf.currentTerm {
		rf.mrole = follower
		rf.currentTerm = args.Term
	}
	//check prevlog
	if args.PrevLogIndex == -1 && logCount == 0 {
		return
	}
	if args.PrevLogIndex == -1 {
		rf.log = append(rf.log, args.Entries[:]...)
		reply.Success = true
		return
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	reply.Success = true
	if logCount == 0 {
		return
	}

	//delete conflic log
	newLog := logCount
	logToCheck := len(rf.log) - args.PrevLogIndex - 1
	if newLog < logToCheck {
		logToCheck = newLog
	}
	start := args.PrevLogIndex + 1
	i := 0
	for ; i < logToCheck; i++ {
		if rf.log[start+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:start+i]
			break
		}
	}

	//append log
	if i < newLog {
		rf.log = append(rf.log, args.Entries[i:]...)
	}

	//update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = start + newLog - 1
		if args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.mrole = follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	}
	//check log
	if len(rf.log) > 0 && (args.LastLogTerm < rf.log[len(rf.log)-1].Term || args.LastLogIndex < len(rf.log)-1) {
		return
	}

	//vote for it
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.chticker <- struct{}{}
	}
	return
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
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.mrole == leader

	if isLeader {
		rf.log = append(rf.log, raftLog{rf.currentTerm, command})
	}

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	//fmt.Printf("%d is candidate! \n", rf.me)
	rf.chticker <- struct{}{}
	rf.mu.Lock()
	if rf.mrole == leader {
		rf.mu.Unlock()
		return
	}
	rf.mrole = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	term := rf.currentTerm
	me := rf.me
	lenpeers := len(rf.peers)
	args := RequestVoteArgs{rf.currentTerm, rf.me, 0, 0} //len(rf.log) - 1, rf.log[len(rf.log)-1].term}
	rf.mu.Unlock()
	votecount := 1
	imnew := true
	finished := 0
	majarity := lenpeers/2 + 1
	ch := make(chan int)
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
				fmt.Printf("TERM %d: %d in term %d vote for %d! \n", term, server, reply.Term, rf.me)
				ch <- 1
			} else {
				ch <- 0
			}
		}()
	}
	for votecount < majarity && finished < lenpeers-1 {
		v := <-ch
		switch v {
		case -1:
			imnew = false
		case 1:
			votecount++
		}
		finished++
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		fmt.Println("get new term while election!")
		return
	}
	if !imnew && newterm > rf.currentTerm {
		rf.mrole = follower
		rf.votedFor = idx
		rf.currentTerm = newterm
	}
	if votecount >= majarity && rf.votedFor == rf.me && rf.mrole == candidate {
		rf.mrole = leader
		rf.chVoteWin <- struct{}{}
		for i := 0; i < lenpeers; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = len(rf.log)
		}
		fmt.Printf("%d wins! term = %d \n", rf.me, rf.currentTerm)
	}
	return
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

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		if rf.mrole != leader {
			return
		}
		rf.mu.Lock()
		term := rf.currentTerm
		maxterm := rf.currentTerm
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			prevTerm := 0
			if len(rf.log) > 0 && rf.nextIndex[i] > 0 {
				prevTerm = rf.log[rf.nextIndex[i]-1].Term
			}
			args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i] - 1,
				prevTerm, nil, rf.commitIndex}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				continue
			}
			if reply.Term > maxterm {
				maxterm = reply.Term
			}
			if rf.nextIndex[i] != 0 && !reply.Success {
				rf.mu.Lock()
				rf.nextIndex[i] = rf.nextIndex[i] - 1
				rf.mu.Unlock()
			}
		}
		rf.mu.Lock()
		if term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		if maxterm > rf.currentTerm {
			rf.mrole = follower
			rf.currentTerm = maxterm
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) run() {
	for rf.killed() == false {
		_ = <-rf.chVoteWin
		//fmt.Printf("leader %d for term %d start! \n", rf.me, rf.currentTerm)
		go rf.heartbeat()
		for rf.mrole == leader {
			lenPeers := len(rf.peers)
			rf.mu.Lock()
			maxLogIdx := len(rf.log) - 1
			term := rf.currentTerm
			rf.mu.Unlock()
			for i := 0; i < lenPeers; i++ {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				nextI := rf.nextIndex[i]
				if maxLogIdx >= nextI {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					args.Term = term
					args.LeaderId = rf.me
					args.Entries = rf.log[nextI : maxLogIdx+1]
					args.PrevLogIndex = nextI - 1
					args.PrevLogTerm = -1
					if nextI > 0 {
						args.PrevLogTerm = rf.log[nextI-1].Term
					}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, &args, &reply)
					if !ok {
						continue
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.mrole = follower
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
						return
					}
					if !reply.Success {
						rf.nextIndex[i] = nextI - 1
					} else {
						rf.nextIndex[i] = nextI + 1
						rf.matchIndex[i] = nextI
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
				}
			}

		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleeptime := rand.Int31() % 200
		select {
		case <-rf.chticker:
			//fmt.Println("RESET election timeout！")
		case <-time.After(time.Millisecond * (time.Duration(sleeptime) + 300)):
			go rf.startElection()
		}

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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.receiveHeartbeat = false
	rf.chticker = make(chan struct{}, 1)
	rf.chVoteWin = make(chan struct{}, 1)
	rf.mrole = follower

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.run()
	return rf
}
