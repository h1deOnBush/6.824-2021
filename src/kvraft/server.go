package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = false
	StartTimeout = 500
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
		Name: "Get",
		ClientId: args.Id,
		Seq: args.Seq,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := kv.getNotifyCh(index)
	select {
	case notification := <- notifyCh:
		// leader has changed, original command has been discard.
		// in case the leader receive the command and then is replaced by new leader before replicate
		// the command to other servers.
		if notification.ClientId != args.Id || notification.Seq != args.Seq {
			DPrintf("leader has changed, command has been replaced")
			reply.Err = ErrWrongLeader
			kv.mu.Lock()
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
			return
		}
		kv.mu.Lock()
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
		return
	case <- time.After(StartTimeout * time.Millisecond):
		kv.mu.Lock()
		DPrintf("wait for agreement timeout")
		reply.Err = ErrWrongLeader
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
		return
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
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("server [%v] is not leader", kv.me)
		return
	}
	notifyCh := kv.getNotifyCh(index)
	select {
	case notification := <-notifyCh:
		kv.mu.Lock()
		if notification.ClientId != args.Id || notification.Seq != args.Seq {
			DPrintf("leader has changed, original command has been replaced")
			reply.Err = ErrWrongLeader
			delete(kv.notifyChMap, index)
			kv.mu.Unlock()
			return
		}
		reply.Err = OK
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
		return
	case <-time.After(StartTimeout * time.Millisecond):
		kv.mu.Lock()
		if kv.getCache(args.Id, args.Seq) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
			DPrintf("wait for raft agreement timeout")
		}
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
		return
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

func (kv *KVServer) getNotifyCh(index int) chan Notification {
	kv.mu.Lock()
	if notifyCh, ok := kv.notifyChMap[index]; ok {
		kv.mu.Unlock()
		return notifyCh
	}
	notifyCh := make(chan Notification, 1)
	kv.notifyChMap[index] = notifyCh
	kv.mu.Unlock()
	return notifyCh
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
			if msg.SnapshotValid {
				kv.installSnapshot(msg.Snapshot)
				continue
			}
			// receive an apply msg that before snapshot index
			if msg.CommandIndex <= kv.rf.GetSnapshotIndex() {
				raft.DPrintf("[server %v],command(%v) has already applied", kv.me, msg.CommandIndex)
				continue
			}
			op := msg.Command.(Op)
			//DPrintf("command(%v) from [client:%v, seq:%v] committed at index(%v)", op.Name, op.ClientId, op.Seq, msg.CommandIndex)

			kv.mu.Lock()
			switch op.Name {
			case "Put":
				if kv.getCache(op.ClientId, op.Seq) {
					kv.mu.Unlock()
					DPrintf("command(%v) has been executed\n", op)
					continue
				}
				// update cache and db
				kv.db[op.Key] = op.Value
				raft.DPrintf("server %v,kv.db:%v", kv.me, kv.db)
				kv.cache[op.ClientId] = op.Seq
			case "Append":
				if kv.getCache(op.ClientId, op.Seq) {
					DPrintf("command(%v) has been executed\n", op)
					kv.mu.Unlock()
					continue
				}
				// update cache and db
				kv.db[op.Key] += op.Value
				//raft.DPrintf("server %v, append key:%v, value:%v", kv.me, op.Key, op.Value)
				//raft.DPrintf("server %v,kv.db:%v", kv.me, kv.db)
				kv.cache[op.ClientId] = op.Seq
			}
			// after update database can do snapshot, or will lose update
			kv.takeSnapshot(msg.CommandIndex)
			// notify client the result of execution
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

func (kv *KVServer) takeSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	statesize := kv.rf.GetRaftStateSize()
	if statesize >= kv.maxraftstate {
		// take snapshot
		//raft.DPrintf("[server %v], raftstatesize:%v", kv.me, statesize)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.db)
		e.Encode(kv.cache)
		data := w.Bytes()
		kv.rf.Snapshot(index, data)
		//raft.DPrintf("[server %v], finish snapshot raftstatesize:%v", kv.me, kv.rf.GetRaftStateSize())
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.notifyChMap = make(map[int]chan Notification)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.installSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.run()
	return kv
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil ||
		d.Decode(&kv.cache) != nil {
		DPrintf("kvserver [%v] fails to recover from snapshot", kv.me)
	}
	kv.mu.Unlock()
}