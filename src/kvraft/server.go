package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opAppend = iota
	opPut
	opGet
	Unknown
)

type OpMsg struct {
	Index int
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method   int
	ClientId int
	CmdId    uint
	Key      string
	Value    string
}

type KVServer struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	persist          *raft.Persister
	applyCh          chan raft.ApplyMsg
	stopCh           chan struct{}
	dead             int32 // set by Kill()
	statemachine     map[string]string
	latestCmdId      map[int]uint
	mapIdtoOpchannel map[string]chan OpMsg

	lastApplied  int
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	Debug(dKvserver, "KS%d From C%d Get Req.Args=[CmdId:%d Key%v] ", kv.me, args.Client, args.CommandId, args.Key)
	_, ok := kv.rf.GetState()
	reply.Success = false
	reply.Value = ""
	if !ok {
		reply.Err = "not leader"
		reply.LeaderId = kv.rf.GetLeaderId()
		return
	}
	kv.mu.Lock()
	cmdIdentifier := fmt.Sprintf("%d_%d", args.Client, args.CommandId)
	ch := make(chan OpMsg)
	kv.mapIdtoOpchannel[cmdIdentifier] = ch
	kv.mu.Unlock()
	idx, _, isLeader := kv.rf.Start(Op{
		Method:   opGet,
		ClientId: args.Client,
		CmdId:    args.CommandId,
		Key:      args.Key,
		Value:    "",
	})
	if !isLeader {
		reply.Err = "not leader"
		reply.LeaderId = kv.rf.GetLeaderId()
		return
	}
	t0 := time.Now()
	chStop := make(chan struct{})
	go func() {
		select {
		case msg := <-ch:
			reply.Success = true
			reply.Value = msg.Value
		case <-chStop:
			reply.Err = "not leader"
		}
	}()

	for kv.lastApplied < idx && time.Since(t0).Milliseconds() < 2000 {
		time.Sleep(20 * time.Millisecond)
	}
	close(chStop)

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug(dKvserver, "KS%d From C%d %v Req Args=[CmdId:%d Key:%v Value:%v] ", kv.me, args.Client, args.Op, args.CommandId, args.Key, args.Value)
	reply.Success = false
	_, ok := kv.rf.GetState()
	if !ok {
		reply.Err = "not leader"
		reply.LeaderId = kv.rf.GetLeaderId()
		return
	}
	kv.mu.Lock()
	v, isPresent := kv.latestCmdId[args.Client]
	if isPresent && args.CommandId == v {
		reply.Success = true
		//DPrintf("S%d for C%d's cmd%d:Put&Append done before.key:%v,value:%v.\n", kv.me, args.Client, args.CommandId, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}
	m := -1
	if args.Op == "Put" {
		m = opPut
	} else if args.Op == "Append" {
		m = opAppend
	}
	cmdIdentifier := fmt.Sprintf("%d_%d", args.Client, args.CommandId)
	ch := make(chan OpMsg)
	kv.mapIdtoOpchannel[cmdIdentifier] = ch
	kv.mu.Unlock()
	//t0 := time.Now()
	idx, _, isLeader := kv.rf.Start(Op{
		Method:   m,
		ClientId: args.Client,
		CmdId:    args.CommandId,
		Key:      args.Key,
		Value:    args.Value,
	})
	if !isLeader {
		reply.Err = "not leader"
		reply.LeaderId = kv.rf.GetLeaderId()
		return
	}
	t0 := time.Now()
	chStop := make(chan struct{})
	go func() {
		select {
		case <-ch:
			reply.Success = true
		case <-chStop:
			reply.Err = "not leader"
		}
	}()

	for kv.lastApplied < idx && time.Since(t0).Milliseconds() < 2000 {
		time.Sleep(20 * time.Millisecond)
	}
	close(chStop)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	Debug(dInfo, "KS%d Killed ")
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) ingestSnap(snapshot []byte, index int) {
	//if index != -1 && index != lastIncludedIndex {
	//	Debug(dWarn, "KS%d Snapshot Doesn't Match SnapshotIndex", kv.me)
	//	return
	//}
	kv.applySnapData(snapshot)

	return
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case <-kv.stopCh:
			return
		case m := <-kv.applyCh:
			if m.SnapshotValid {
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
					kv.ingestSnap(m.Snapshot, m.SnapshotIndex)
				}
			} else if m.CommandValid {
				switch m.Command.(type) {
				case Op:
					cmd := m.Command.(Op)
					kv.mu.Lock()
					kv.lastApplied = m.CommandIndex
					if cmd.Method != opGet && kv.latestCmdId[cmd.ClientId] != cmd.CmdId {
						Debug(dKvserver, "KS%d Commit Log CmdId:%d_%d ", kv.me, cmd.ClientId, cmd.CmdId)
						kv.latestCmdId[cmd.ClientId] = cmd.CmdId
						switch cmd.Method {
						case opAppend:
							kv.statemachine[cmd.Key] += cmd.Value
						case opPut:
							kv.statemachine[cmd.Key] = cmd.Value
						}
					}
					Identifier := fmt.Sprintf("%d_%d", cmd.ClientId, cmd.CmdId)
					ch, isPresent := kv.mapIdtoOpchannel[Identifier]
					kv.mu.Unlock()
					if isPresent {
						msg := OpMsg{}
						msg.Index = m.CommandIndex
						if cmd.Method == opGet {
							msg.Value = kv.statemachine[cmd.Key]
						}
						select {
						case ch <- msg:
						case <-time.After(100 * time.Millisecond):
						}
					}
					kv.mu.Lock()
					delete(kv.mapIdtoOpchannel, Identifier)
					kv.mu.Unlock()
					logSize := kv.persist.RaftStateSize()
					if kv.maxraftstate > 0 && logSize >= kv.maxraftstate {
						Debug(dKvserver, "KS%d LogSize=%d >%d LastIndex:%d ", kv.me, logSize, kv.maxraftstate, m.CommandIndex)
						w := new(bytes.Buffer)
						e := labgob.NewEncoder(w)
						e.Encode(m.CommandIndex)
						e.Encode(kv.statemachine)
						e.Encode(kv.latestCmdId)
						kv.rf.Snapshot(m.CommandIndex, w.Bytes())
					}
				}
			}
		}
	}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applySnapData(snapData []byte) {
	if snapData == nil {
		Debug(dWarn, "KS%d nil snapshot")
		return
	}
	r := bytes.NewBuffer(snapData)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var stateMachine map[string]string
	var clientCmds map[int]uint
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&stateMachine) != nil ||
		d.Decode(&clientCmds) != nil {
		Debug(dWarn, "snapshot decode error")
		return
	}

	Debug(dKvserver, "KS%d Apply Snap SI:%d ", kv.me, lastIncludedIndex)
	kv.statemachine = stateMachine
	kv.lastApplied = lastIncludedIndex
	kv.latestCmdId = clientCmds
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persist = persister

	// You may need initialization code here.
	kv.statemachine = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.latestCmdId = make(map[int]uint)
	kv.mapIdtoOpchannel = make(map[string]chan OpMsg)
	kv.stopCh = make(chan struct{})
	kv.lastApplied = -1
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applySnapData(kv.persist.ReadSnapshot())

	go kv.applier()

	// You may need initialization code here.

	return kv
}
