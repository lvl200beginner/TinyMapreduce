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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	err := e.Encode(rf.log)
	if err != nil {
		rf.mu.Unlock()
		fmt.Printf("persist log error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		rf.mu.Unlock()
		fmt.Printf("persist votedFor error.err=%v \n", err)
		return
	}
	err = e.Encode(rf.currentTerm)
	if err != nil {
		rf.mu.Unlock()
		fmt.Printf("persist currentTerm error.err=%v \n", err)
		return
	}
	rf.mu.Unlock()
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
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []raftLog
	var votedfor int
	var term int
	if d.Decode(&logs) != nil ||
		d.Decode(&votedfor) != nil ||
		d.Decode(&term) != nil {
		fmt.Printf("decode error!\n")
	} else {
		rf.mu.Lock()
		rf.log = logs
		rf.votedFor = votedfor
		rf.currentTerm = term
		rf.mu.Unlock()
	}
	//fmt.Printf("server %d:log:%v,votefor:%v,term:%v \n", rf.me, rf.log, rf.votedFor, rf.currentTerm)
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
	go rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%d(%d) rec heart from %d(%d) \n ", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LogLen = len(rf.log)
	reply.ConflicTerm = -1
	reply.TermFirstIndex = -1
	logCount := len(args.Entries)
	stateChanged := new(bool)
	*stateChanged = false
	defer rf.PersistState(stateChanged)

	rf.chticker <- struct{}{}
	if args.Term > rf.currentTerm {
		if rf.mrole == leader {
			//rf.log = rf.log[:rf.commitIndex+1]
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
		}
		rf.mrole = follower
		rf.currentTerm = args.Term
		*stateChanged = true
	}
	if args.Term < rf.currentTerm {
		return
	}

	//fmt.Printf("%d: recv append from %d,logconut = %d,term = %d,PrevLogIndex = %d, prevterm = %d,LeaderCommit = %d \n my state:log len:%d,commit:%d, log:%v \n", rf.me,
	//args.LeaderId, logCount, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(rf.log), rf.commitIndex, rf.log)
	//check prevlog
	if args.PrevLogIndex == -1 && logCount == 0 {
		return
	}
	//if args.PrevLogIndex == -1 {
	//	//fmt.Printf("PrevLogIndex == -1:%d in term %d recv log form %d \n", rf.me, rf.currentTerm, rf.votedFor)
	//	rf.log = append(rf.log, args.Entries[:]...)
	//	reply.Success = true
	//	rf.commitIndex = args.LeaderCommit
	//	if rf.commitIndex > len(args.Entries)-1 {
	//		rf.commitIndex = len(args.Entries) - 1
	//	}
	//	*stateChanged = true
	//	return
	//}

	if args.PrevLogIndex >= len(rf.log) {
		return
	} else if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflicTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for {
			if i == 0 || rf.log[i-1].Term != reply.ConflicTerm {
				break
			}
			i--
		}
		reply.TermFirstIndex = i
		return
	}

	reply.Success = true

	//fmt.Printf("%d:commit:%d,log:%v \n", rf.me, rf.commitIndex, rf.log)
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
			*stateChanged = true
			break
		}
	}

	//append log
	if i < newLog {
		rf.log = append(rf.log, args.Entries[i:]...)
		*stateChanged = true
		//fmt.Printf("i < newLog :%d in term %d recv log form %d  \n", rf.me, rf.currentTerm, rf.votedFor)
	}

	//update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = start + newLog - 1
		if args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
	}
	//fmt.Printf("%d:leader%d,args.LeaderCommit:%d,start + newLog - 1=%d \ncommit CHANGE!:%d,log:%v \n", rf.me, args.LeaderId, args.LeaderCommit, start+newLog-1, rf.commitIndex, rf.log)
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
	defer rf.PersistState(stateChanged)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//fmt.Printf("%d(%d)get votereq from %d(%d).\n candidate last log:[idx:%d,%d]\n my log:%v \n", rf.me, rf.currentTerm, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.log)
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		if rf.mrole == leader {
			//rf.log = rf.log[:rf.commitIndex+1]
		}
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
			if args.LastLogIndex < lenl-1 {
				return
			}
		case false:
			if args.LastLogTerm < rf.log[lenl-1].Term {
				return
			}
		}
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
		go rf.persist()
		//fmt.Printf("leader %d append log in index %d comit = %d \n cmd:%v log:%v \n", rf.me, index+1, rf.commitIndex, command, rf.log)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.chticker <- struct{}{}
	rf.mu.Lock()
	if rf.mrole == leader {
		rf.mu.Unlock()
		return
	}

	//fmt.Printf("%d is candidate! \n", rf.me)
	rf.mrole = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	term := rf.currentTerm
	me := rf.me
	lenpeers := len(rf.peers)
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args := RequestVoteArgs{term, rf.me, len(rf.log) - 1, lastLogTerm}
	rf.mu.Unlock()
	go rf.persist()
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
				//fmt.Printf("TERM %d: %d in term %d vote for %d! \n", term, server, reply.Term, rf.me)
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
		return
	}
	if !imnew && newterm > rf.currentTerm {
		rf.mrole = follower
		rf.votedFor = idx
		rf.currentTerm = newterm
		go rf.persist()
	} else if votecount >= majarity && rf.votedFor == rf.me && rf.mrole == candidate {
		rf.mrole = leader
		//rf.log = rf.log[:rf.commitIndex+1]
		for i := 0; i < lenpeers; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = len(rf.log)
		}
		rf.chVoteWin <- struct{}{}
		go rf.persist()
		//fmt.Printf("%d wins! term = %d \nleader state:commit=%d,applied=%d,log:%v \n", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log)
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

func (rf *Raft) heartbeat(t1 int) {
	created := false
	for rf.killed() == false {
		if rf.mrole != leader || t1 != rf.currentTerm {
			return
		}
		rf.mu.Lock()
		term := rf.currentTerm
		commit := rf.commitIndex
		rf.mu.Unlock()
		ch := make(chan AppendMsg)
		if !created {
			created = true
			go func(t int) {
				for msg := range ch {
					if term != rf.currentTerm || rf.mrole != leader {
						return
					}
					if msg.rpcFailed {
						continue
					}
					rf.mu.Lock()
					if msg.reply.Term > rf.currentTerm {
						rf.mrole = follower
						//rf.log = rf.log[:rf.commitIndex+1]
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						rf.currentTerm = msg.reply.Term
						//fmt.Printf("%d 's term(%d) > %d 's term(%d)! \n", msg.peerIdx, msg.reply.Term, rf.me, rf.currentTerm)
						rf.chticker <- struct{}{}
						rf.mu.Unlock()
						go rf.persist()
						return
					} else if rf.nextIndex[msg.peerIdx] != 0 {
						switch msg.reply.Success {
						case false:
							minIdx := min(msg.reply.LogLen, len(rf.log))
							//rf.nextIndex[msg.peerIdx] = rf.nextIndex[msg.peerIdx] - 1
							n := msg.nextIdx
							if msg.reply.ConflicTerm != -1 {
								for n >= msg.reply.TermFirstIndex && n < len(rf.log) {
									if rf.log[n].Term == msg.reply.ConflicTerm {
										break
									}
									n--
								}
							}
							minIdx = min(minIdx, n)
							minIdx = min(msg.nextIdx-1, minIdx)
							rf.nextIndex[msg.peerIdx] = min(rf.nextIndex[msg.peerIdx], minIdx)
						case true:
							rf.matchIndex[msg.peerIdx] = max(msg.nextIdx-1, rf.matchIndex[msg.peerIdx])
						}

					}
					for N := rf.commitIndex + 1; N < len(rf.log); N++ {
						if rf.log[N].Term != rf.currentTerm {
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
					rf.mu.Unlock()
				}
			}(term)
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			prevTerm := 0
			nextI := rf.nextIndex[i]
			if len(rf.log) > 0 && rf.nextIndex[i] > 0 {
				prevTerm = rf.log[rf.nextIndex[i]-1].Term
			}
			rf.mu.Unlock()
			go func(t int, c int, ni int, n int) {
				args := AppendEntriesArgs{term, rf.me, ni - 1,
					t, nil, c}
				reply := AppendEntriesReply{}
				//fmt.Printf("leader %d:heart beat for %d in term %d \n", rf.me, n, term)
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
		time.Sleep(100 * time.Millisecond)
	}
}

type AppendMsg struct {
	rpcFailed  bool
	nextIdx    int
	peerIdx    int
	entriesLen int
	reply      AppendEntriesReply
}

func (rf *Raft) run() {
	for rf.killed() == false {
		_ = <-rf.chVoteWin
		//fmt.Printf("leader %d for term %d start! \n", rf.me, rf.currentTerm)
		tt := rf.currentTerm
		go rf.heartbeat(tt)
		chAppend := make(chan AppendMsg)
		created := false
		for rf.mrole == leader {
			lenPeers := len(rf.peers)
			rf.mu.Lock()
			maxLogIdx := len(rf.log) - 1
			term := rf.currentTerm
			rf.mu.Unlock()
			if !created {
				created = true
				go func(t int) {
					for msg := range chAppend {
						if t != rf.currentTerm || rf.mrole != leader {
							//fmt.Printf("no longer leader for term %d \n", t)
							return
						}
						if msg.rpcFailed {
							continue
						}
						rf.mu.Lock()
						if msg.reply.Term > rf.currentTerm {
							//fmt.Printf("%d 's term(%d) > %d 's term(%d)! \n", msg.peerIdx, msg.reply.Term, rf.me, rf.currentTerm)
							rf.mrole = follower
							//rf.log = rf.log[:rf.commitIndex+1]
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							rf.currentTerm = msg.reply.Term
							rf.mu.Unlock()
							go rf.persist()
							return
						}
						//fmt.Printf("%d:update for %d!msg.nextIdx =%d,msg.entriesLen = %d \n", rf.me, msg.peerIdx, msg.nextIdx, msg.entriesLen)
						if !msg.reply.Success {
							//rf.nextIndex[msg.peerIdx] = msg.nextIdx - 1
							minIdx := min(msg.reply.LogLen, len(rf.log))
							n := msg.nextIdx - 1
							if msg.reply.ConflicTerm != -1 {
								for n >= msg.reply.TermFirstIndex && n < len(rf.log) {
									if rf.log[n].Term == msg.reply.ConflicTerm {
										break
									}
									n--
								}
							}
							minIdx = min(minIdx, n)
							minIdx = min(msg.nextIdx-1, minIdx)
							rf.nextIndex[msg.peerIdx] = min(rf.nextIndex[msg.peerIdx], minIdx)
						} else {
							rf.nextIndex[msg.peerIdx] = max(msg.nextIdx+msg.entriesLen, rf.nextIndex[msg.peerIdx])
							rf.matchIndex[msg.peerIdx] = max(rf.matchIndex[msg.peerIdx], msg.nextIdx+msg.entriesLen-1)
						}
						for N := rf.commitIndex + 1; N < len(rf.log); N++ {
							//fmt.Printf("N = %d!rf.log[N].Term=%d,rf.currentTerm = %d,match = %v,next =%v \n", rf.commitIndex+1, rf.log[rf.commitIndex+1].Term, rf.currentTerm, rf.matchIndex, rf.nextIndex)
							if rf.log[N].Term != rf.currentTerm {
								continue
							}
							count := 1
							for i := 0; i < lenPeers; i++ {
								if i == rf.me {
									continue
								}
								if rf.matchIndex[i] >= N {
									count++
								}
							}
							if count >= (lenPeers/2)+1 {
								rf.commitIndex = N
							}
						}
						rf.mu.Unlock()
					}
				}(term)
			}
			for i := 0; i < lenPeers; i++ {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				nextIdx := rf.nextIndex[i]
				args := AppendEntriesArgs{}
				msg := AppendMsg{}
				msg.peerIdx = i
				msg.nextIdx = nextIdx
				args.LeaderCommit = rf.commitIndex
				args.Term = term
				args.Entries = rf.log[nextIdx : maxLogIdx+1]
				args.LeaderId = rf.me
				args.PrevLogIndex = nextIdx - 1
				args.PrevLogTerm = -1
				msg.entriesLen = len(args.Entries)
				if nextIdx > 0 {
					args.PrevLogTerm = rf.log[nextIdx-1].Term
				}
				rf.mu.Unlock()
				if maxLogIdx < nextIdx {
					continue
				}
				go func(a AppendEntriesArgs, n int) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(n, &a, &reply)
					if !ok {
						msg.rpcFailed = true
						select {
						case chAppend <- msg:
							return
						case <-time.After(100 * time.Millisecond):
							return
						}
					}
					msg.rpcFailed = false
					msg.reply = reply
					select {
					case chAppend <- msg:
						return
					case <-time.After(100 * time.Millisecond):
						return
					}
				}(args, i)
			}
			time.Sleep(30 * time.Millisecond)
			rf.mu.Lock()
			//fmt.Printf("N = %d!rf.log[N].Term=%d,rf.currentTerm = %d,match = %v,next =%v \n", rf.commitIndex+1, rf.log[rf.commitIndex+1].Term, rf.currentTerm, rf.matchIndex, rf.nextIndex)
			for N := rf.commitIndex + 1; N < len(rf.log); N++ {
				if rf.log[N].Term != rf.currentTerm {
					continue
				}
				count := 1
				for i := 0; i < lenPeers; i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						count++
					}
				}
				if count >= (lenPeers/2)+1 {
					rf.commitIndex = N
				}
			}
			rf.mu.Unlock()
			//fmt.Printf("leader %d state:commit = %d,loglen = %d,matches = %v ,nextidx = %v \n", rf.me, rf.commitIndex, len(rf.log), rf.matchIndex, rf.nextIndex)
		}
		//fmt.Printf("%d is not leader for term %d now! \n", rf.me, tt)
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
		sleeptime := rand.Int31() % 200
		select {
		case <-rf.chticker:
			//fmt.Println("RESET election timeout！")
		case <-time.After(time.Millisecond * (time.Duration(sleeptime) + 300)):
			go rf.startElection()
		}

	}
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex >= 0 && rf.commitIndex < len(rf.log) {
			comIdx := rf.commitIndex
			logs := make([]raftLog, len(rf.log))
			copy(logs, rf.log)
			rf.mu.Unlock()
			if comIdx > rf.lastApplied {
				//fmt.Printf("%d:Apply!,apply log=%v,commit=%d apply=%d\n", rf.me, logs, comIdx, rf.lastApplied)
			}
			for comIdx > rf.lastApplied {
				rf.lastApplied++
				msg := ApplyMsg{true, logs[rf.lastApplied].Cmd, rf.lastApplied + 1, false, nil, 0, 0}
				applyCh <- msg
			}
		} else {
			rf.mu.Unlock()
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
	go rf.applier(applyCh)
	return rf
}
