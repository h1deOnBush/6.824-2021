package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = false
	StartTimeout = 2000
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string

	Name string
	ClientId int64
	Seq int
}

type Notification struct {
	ClientId int64
	Seq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//key: clientId ,value: latest commandSeq that has been execute successfully
	db map[string]string
	cache map[int64]int
	notifyChMap map[int]chan Notification
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op {
		Key:   args.Key,
		Value: "",
		Name: "Get",
		ClientId: args.Id,
		Seq: args.Seq,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	notifyCh := make(chan Notification)
	kv.notifyChMap[index] = notifyCh
	kv.mu.Unlock()
	for !kv.killed() {
		select {
		case notification := <- notifyCh:
			// leader has changed, original command has been discard.
			// in case the leader receive the command and then is replaced by new leader before replicate
			// the command to other servers.
			if notification.ClientId != args.Id || notification.Seq != args.Seq {
				reply.Err = ErrWrongLeader
				return
			}
			kv.mu.Lock()
			curTerm, isLeader := kv.rf.GetState()
			if !isLeader || curTerm != term {
				// in case of network partition, a server come back and thus changed leader's term or even change leader
				reply.Err = ErrWrongLeader
			} else {
				if value, ok := kv.db[args.Key]; ok {
					reply.Value = value
					reply.Err = OK
				} else {
					reply.Err = ErrNoKey
				}
			}
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
			return
		case <- time.After(StartTimeout * time.Millisecond):
			kv.mu.Lock()
			reply.Err = ErrWrongLeader
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op {
		Key:   args.Key,
		Value: args.Value,
		Name: args.Op,
		ClientId: args.Id,
		Seq: args.Seq,
	}
	kv.mu.Lock()
	// hit the cache
	if kv.getCache(args.Id, args.Seq) {
		reply.Err = OK
		kv.mu.Unlock()
		DPrintf("[client %v, seq %v] command(%v) hit the cache", args.Id, args.Seq, args.Seq)
		return
	}
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	notifyCh := make(chan Notification)
	kv.notifyChMap[index] = notifyCh
	kv.mu.Unlock()
	for !kv.killed() {
		select {
		case notification := <-notifyCh:
			if notification.ClientId != args.Id || notification.Seq != args.Seq {
				reply.Err = ErrWrongLeader
				return
			}
			kv.mu.Lock()
			curTerm, isLeader := kv.rf.GetState()
			if !isLeader || curTerm != term {
				reply.Err = ErrWrongLeader
			}
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
			return
		case <-time.After(StartTimeout * time.Millisecond):
			kv.mu.Lock()
			reply.Err = ErrWrongLeader
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
			DPrintf("wait for raft agreement timeout")
			return
		}
	}
}

func (kv *KVServer) getCache(clientId int64, seq int) bool {
	if latestSeq, ok := kv.cache[clientId]; ok {
		if latestSeq >= seq {
			return true
		}
	}
	return false
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) run() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			op := msg.Command.(Op)
			kv.mu.Lock()
			// update db
			switch op.Name {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}

			// update cache
			kv.cache[op.ClientId] = op.Seq

			if notifyCh, ok := kv.notifyChMap[msg.CommandIndex]; ok {
				notifyCh <- Notification {
					ClientId: op.ClientId,
					Seq:      op.Seq,
				}
			}
			kv.mu.Unlock()
		}
	}
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
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.notifyChMap = make(map[int]chan Notification)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.run()
	return kv
}
