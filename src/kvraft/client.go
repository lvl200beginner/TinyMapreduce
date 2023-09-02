package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

var clientID int = 1
var mu sync.Mutex

type Clerk struct {
	servers      []*labrpc.ClientEnd
	name         int
	leaderServer int
	cmdId        uint
	mu           sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	mu.Lock()
	ck.name = clientID
	clientID++
	mu.Unlock()
	ck.leaderServer = int(nrand() % int64(len(servers)))
	ck.cmdId = 1
	// You'll have to add code here.

	//randomly chose a server and send rpc

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	i := ck.leaderServer
	args := GetArgs{
		Client:    ck.name,
		CommandId: ck.cmdId,
		Key:       key,
	}
	ck.cmdId++
	ck.mu.Unlock()
	DPrintf("Client%d get! args:%v\n", ck.name, args)
	for {
		reply := GetReply{}
		reply.Success = false
		reply.LeaderId = -1
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			i = (i + 1) % len(ck.servers)
			continue
		}
		//DPrintf("Client%d get reply:%v\n", ck.name, reply)
		if reply.Success {

			return reply.Value
		}
		if reply.Err == "not leader" {
			i = (i + 1) % len(ck.servers)
		}
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	i := ck.leaderServer
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		Client:    ck.name,
		CommandId: ck.cmdId,
	}
	ck.cmdId++
	ck.mu.Unlock()
	DPrintf("Client%d put&append! args:%v\n", ck.name, args)
	for {
		reply := PutAppendReply{}
		reply.Success = false
		reply.LeaderId = -1
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			i = (i + 1) % len(ck.servers)
			continue
		}
		//DPrintf("Client%d put&append reply! :%v\n", ck.name, reply)
		if reply.Success {
			return
		}
		if reply.Err == "not leader" {
			i = (i + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
