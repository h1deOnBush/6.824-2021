package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrMigrating		= "ErrMigrating"
	TIMEOUT             = 500
	FetchConfigInterval = 100
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq int
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateDataArgs struct {
	Shard int
	ConfigNum int
}

type MigrateDataReply struct {
	Success bool
	Err Err
	Shard int
	ConfigNum int
	Data map[string]string
	LatestCommand map[int64]int
}