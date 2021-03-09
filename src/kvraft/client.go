package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"



type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// to identify a client, generate by nrand
	id int64
	// global command seq, to identify a command, start from 1
	seq int
	// get leader id
	leaderId int
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
	// You'll have to add code here.
	ck.id = nrand()
	ck.leaderId = 0
	ck.seq = 0
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
	// You will have to modify this function.
	ck.seq++
	args := GetArgs {
		Key: key,
		Id:  ck.id,
		Seq: ck.seq,
	}
	var reply GetReply
	for {
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) {
			DPrintf("connect to server [%v] failed", ck.leaderId)
			reply.Err = ErrWrongLeader
		}
		if reply.Err == ErrWrongLeader {
			DPrintf("[client %v, commandSeq %v] command(get) send to server(%v) is not leader", ck.id, ck.seq, ck.leaderId)
			ck.leaderId = ck.randomChooseLeader()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == OK {
			DPrintf("[client %v, commandSeq %v] execute get(key:%v, value:%v)", ck.id, ck.seq, key, reply.Value)
			return reply.Value
		}
		return ""
	}
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
	ck.seq++
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    ck.id,
		Seq:   ck.seq,
	}
	var reply PutAppendReply
	if op == "Put" || op == "Append" {
		for {
			if ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) && reply.Err==OK {
				DPrintf("[client %v, commandSeq %v] command(%v) execute, (key:%v, value %v)",ck.id, ck.seq, op, key, value)
				return
			}
			if reply.Err == ErrWrongLeader {
				DPrintf("[client %v, commandSeq %v] command(%v) send to server(%v) is not leader", ck.id, ck.seq, op, ck.leaderId)
			} else {
				DPrintf("cannot connect to server [%v], reply.Err(%v)", ck.leaderId, reply.Err)
			}
			ck.leaderId = ck.randomChooseLeader()
		}
	} else {
		DPrintf("[client %v, commandSeq %v] command (%v) not support", ck.id, ck.seq, op)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// random choose a leader
func (ck *Clerk) randomChooseLeader() int {
	n := len(ck.servers)
	return (ck.leaderId+1) % n
}