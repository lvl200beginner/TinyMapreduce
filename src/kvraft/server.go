package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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
	Method     int
	Identifier string
	Key        string
	Value      string
}

type KVServer struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
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
	DPrintf("Server%d rec Get request.Args:%v \n", kv.me, args)
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
		Method:     opGet,
		Identifier: cmdIdentifier,
		Key:        args.Key,
		Value:      "",
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

	//select {
	//case msg := <-ch:
	//	if msg.Index != idx {
	//		reply.Err = "Index not match"
	//	} else {
	//		reply.Success = true
	//		reply.Value = msg.Value
	//	}
	//case <-time.After(600 * time.Millisecond):
	//	reply.Err = "not leader"
	//	//DPrintf("Kvserver apply log time out!\n")
	//}

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("S%d rec PutAppend request.Args:%v \n", kv.me, args)
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
		DPrintf("S%d for C%d's cmd%d:Put&Append done before.key:%v,value:%v.\n", kv.me, args.Client, args.CommandId, args.Key, args.Value)
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
		Method:     m,
		Identifier: cmdIdentifier,
		Key:        args.Key,
		Value:      args.Value,
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
	//select {
	//case msg := <-ch:
	//	if msg.Index != idx {
	//		reply.Err = "Index not match"
	//	} else {
	//		//fmt.Printf("KV :S%d:%v ms passed between start and apply \n", kv.me, time.Since(t0).Milliseconds())
	//		reply.Success = true
	//		DPrintf("KV S%d->C%d:cmd %v done. \n", kv.me, args.CommandId, cmdIdentifier)
	//		//DPrintf("Kvserver apply log time out!\n")
	//	}
	//case <-time.After(600 * time.Millisecond):
	//	reply.Err = "not leader"
	//}
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
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case <-kv.stopCh:
			return
		case m := <-kv.applyCh:
			if m.CommandValid == false {
				// ignore other types of ApplyMsg
			} else {
				switch m.Command.(type) {
				case Op:
					cmd := m.Command.(Op)
					s := strings.Split(cmd.Identifier, "_")
					client, _ := strconv.Atoi(s[0])
					cmdId, _ := strconv.Atoi(s[1])
					DPrintf("KVServer%d Log! cmdid=%v \n", kv.me, cmd.Identifier)
					kv.mu.Lock()
					kv.lastApplied = m.CommandIndex
					if cmd.Method != opGet && kv.latestCmdId[client] != uint(cmdId) {
						//fmt.Printf("KVServer%d Apply! cmd=%v \n", kv.me, cmd)
						kv.latestCmdId[client] = uint(cmdId)
						switch cmd.Method {
						case opAppend:
							kv.statemachine[cmd.Key] += cmd.Value
						case opPut:
							kv.statemachine[cmd.Key] = cmd.Value
						}
					}
					ch, isPresent := kv.mapIdtoOpchannel[cmd.Identifier]
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
					delete(kv.mapIdtoOpchannel, cmd.Identifier)
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// You may need initialization code here.
	kv.statemachine = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.latestCmdId = make(map[int]uint)
	kv.mapIdtoOpchannel = make(map[string]chan OpMsg)
	kv.stopCh = make(chan struct{})
	kv.lastApplied = -1
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()

	// You may need initialization code here.

	return kv
}
