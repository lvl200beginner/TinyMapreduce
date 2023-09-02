package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	Client    int
	CommandId uint
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	Success  bool
	LeaderId int
}

type GetArgs struct {
	Client    int
	CommandId uint
	Key       string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Success  bool
	Value    string
	LeaderId int
}
