package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string

	Name string
	ClientId int64
	Seq int

	// similar to the term in raft
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	killCh		chan struct{}
	db 			map[string]string
	notifyChMap map[int]chan Op
	latestCommand map[int64]int
	clerk       *shardctrler.Clerk
	config      shardctrler.Config
	nextConfig 	shardctrler.Config
	// cfg num -> (shard -> db)
	shardsOut 	map[int]map[int]map[string]string
	// shard -> gid
	shardsIn	map[int]int
	shardsOwn 	map[int]bool
	// cfg num ->.(shard -> db)
	shardsInData map[int]map[int]map[string]string
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	originalOp := Op {
		Name:     "Get",
		Key:     args.Key,
		Seq :     -1,
		ConfigNum: args.ConfigNum,
	}
	reply.Err = kv.handle(originalOp)
	if reply.Err == OK {
		kv.lock("")
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		DPrintf("%v-%v get success,key:%v,value:%v, configNum:%v", kv.gid, kv.me, originalOp.Key, reply.Value, args.ConfigNum)
		kv.unlock("")
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	originalOp := Op {
		Key:      args.Key,
		Value:    args.Value,
		Name:     args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		ConfigNum: args.ConfigNum,
	}
	reply.Err = kv.handle(originalOp)
}

func (kv *ShardKV) handle(originalOp Op) Err {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return ErrWrongLeader
	}
	kv.lock("")
	// only leader hold the shardsOwn, so this check need to place here
	if !kv.holdKey(originalOp.Key) {
		kv.unlock("")
		return ErrWrongGroup
	}
	if originalOp.ConfigNum > kv.config.Num {
		kv.unlock("")
		return ErrWrongLeader
	}
	DPrintf("%v-%v, receive command(%+v), key2shard:%v, configNum:%v", kv.gid, kv.me, originalOp, key2shard(originalOp.Key), originalOp.ConfigNum)
	kv.unlock("")
	index, _, _ := kv.rf.Start(originalOp)
	notifyCh := kv.getNotifyCh(index)
	select {
	case op := <-notifyCh:
		close(notifyCh)
		kv.lock("")
		delete(kv.notifyChMap, index)
		kv.unlock("")
		if op.ClientId != originalOp.ClientId || op.Seq != originalOp.Seq {
			return ErrWrongLeader
		}

		return OK
	case <-time.After(time.Duration(TIMEOUT)*time.Millisecond):
		kv.lock("")
		defer  kv.unlock("")

		if kv.isDuplicate(originalOp.ClientId, originalOp.Seq) {
			return OK
		}
		return ErrWrongLeader
	}
}

func (kv *ShardKV) run() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.installSnapshot(msg.Snapshot)
				DPrintf("%v-%v install snapshot", kv.gid, kv.me)
				DPrintf("%v-%v, kv.db:%v", kv.gid, kv.me, kv.db)
				continue
			}
			// receive an apply msg that before snapshot index
			if msg.CommandIndex <= kv.rf.GetSnapshotIndex() {
				DPrintf("%v-%v, command(%v) has already applied", kv.gid, kv.me, msg.CommandIndex)
				continue
			}
			// config
			if config, ok := msg.Command.(shardctrler.Config); ok {
				// DPrintf("%v-%v receive applied config(%v)", kv.gid, kv.me, config.Num)
				kv.installConfig(config)
				continue
			}
			// migrateReply
			if reply, ok := msg.Command.(MigrateDataReply); ok {
				// DPrintf("%v-%v receive applied MDReply(shard:%v)", kv.gid, kv.me, reply.Shard)
				kv.installShard(reply)
				continue
			}
			op := msg.Command.(Op)
			//DPrintf("command(%v) from [client:%v, seq:%v] committed at index(%v)", op.Name, op.ClientId, op.Seq, msg.CommandIndex)

			kv.lock("")
			switch op.Name {
			case "Put":
				if kv.isDuplicate(op.ClientId, op.Seq) {
					DPrintf("%v-%v,command(%+v) has been executed\n", kv.gid, kv.me, op)
					kv.unlock("")
					continue
				}
				// update cache and db
				kv.db[op.Key] = op.Value
				DPrintf("%v-%v, Put (%+v) success", kv.gid, kv.me, op)
				raft.DPrintf("apply (%v) put key:%v, value:%v", msg.CommandIndex, op.Key, op.Value)
				raft.DPrintf("put server %v,kv.db:%v", kv.me, kv.db)
				kv.latestCommand[op.ClientId] = op.Seq
			case "Append":
				if kv.isDuplicate(op.ClientId, op.Seq) {
					DPrintf("%v-%v command(%+v) has been executed\n", kv.gid, kv.me, op)
					kv.unlock("")
					continue
				}
				// update cache and db
				kv.db[op.Key] += op.Value
				DPrintf("%v-%v, Append (%+v) success, key2shard(%v)", kv.gid, kv.me, op, key2shard(op.Key))
				//raft.DPrintf("apply (%v) append key:%v, value:%v", msg.CommandIndex, op.Key, op.Value)
				//raft.DPrintf("append server %v,kv.db:%v", kv.me, kv.db)
				kv.latestCommand[op.ClientId] = op.Seq
			}
			// after update database can do snapshot, or will lose update
			kv.takeSnapshot(msg.CommandIndex)
			// notify client the result of execution
			if notifyCh, ok := kv.notifyChMap[msg.CommandIndex]; ok {
				notifyCh <- op
			}
			kv.unlock("")
		}
	}
}

func (kv *ShardKV) installShard(reply MigrateDataReply) {
	kv.lock("installShard")

	if reply.ConfigNum != kv.config.Num+1 {
		DPrintf("%v-%v invalid installShard:%v, configNum:%v, currentConfigNum:%v", kv.gid, kv.me, reply.Shard, reply.ConfigNum, kv.config.Num)
		kv.unlock("installShard")
		return
	}
	// G1 hold shard1 in Config1, while G2 hold shard1 in Config2, and in Config2 shard1 become "",
	// G1 rehold shard1 in Config3, we need to update shard1 in G1 in Config3 because G1 won't delete
	// real data belong to shard1
	for k, v := range reply.Data {
		kv.db[k] = v
	}
	for k, v := range reply.LatestCommand {
		if _, ok := kv.latestCommand[k]; ok {
			if kv.latestCommand[k]<v {
				kv.latestCommand[k] = v
			}
		} else {
			kv.latestCommand[k] = v
		}
	}
	delete(kv.shardsIn, reply.Shard)
	kv.shardsOwn[reply.Shard] = true
	DPrintf("%v-%v install shard %v, data:%v, shard configNum:%v, current config num:%v", kv.gid, kv.me, reply.Shard, reply.Data, reply.ConfigNum, kv.config.Num)
	DPrintf("%v-%v shardsOwn:%v, kv.config:%v", kv.gid, kv.me, kv.shardsOwn, kv.config)
	if kv.nextConfig.Num == kv.config.Num+1 && len(kv.shardsIn) == 0 {
		DPrintf("%v-%v update config from %v-%v, %+v, kv.db:%v", kv.gid, kv.me, kv.config.Num, kv.nextConfig.Num, kv.nextConfig, kv.db)
		kv.config = kv.nextConfig
		// TODO here to delete data
		DPrintf("%v-%v shardsOwn:%v", kv.gid, kv.me, kv.shardsOwn)
	} else {

	}
	kv.unlock("installShard")
}

func (kv *ShardKV) takeSnapshot(index int) {
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
		e.Encode(kv.latestCommand)
		e.Encode(kv.shardsOut)
		e.Encode(kv.shardsIn)
		e.Encode(kv.shardsOwn)
		e.Encode(kv.config)
		e.Encode(kv.nextConfig)
		data := w.Bytes()
		kv.rf.Snapshot(index, data)
		DPrintf("%v-%v, finish snapshot, kv.config: %v, kv.db:%v", kv.gid, kv.me, kv.config, kv.db)
		//raft.DPrintf("[server %v], finish snapshot raftstatesize:%v", kv.me, kv.rf.GetRaftStateSize())
	}
}


// pull data may fail for the server which need to fetch data from hasn't update its shardsOut, so may install config many times
func (kv *ShardKV) installConfig(config shardctrler.Config) {
	kv.lock("installConfig")
	currentConfig := kv.config
	if config.Num == kv.config.Num+1 {
		// ensure one config just update shardsOut and shardsIn once
		if config.Num != kv.nextConfig.Num {
			kv.updateShardsInAndOut(kv.config, config)



			DPrintf("%v-%v find new config, shardsIn:%+v, shardsOut[%v]:%+v", kv.gid, kv.me,  kv.shardsIn, config.Num, kv.shardsOut[config.Num])
			DPrintf("%v-%v, config:%+v, nextConfig:%+v", kv.gid, kv.me, kv.config, config)
			kv.nextConfig = config
			DPrintf("%v-%v, update nextConfig:%+v", kv.gid, kv.me, kv.nextConfig)
		}
	} else {
		kv.unlock("installConfig")
		return
	}

	// update config when there is no need to pull data
	if kv.config.Num+1 == kv.nextConfig.Num && len(kv.shardsIn) == 0 {
		// when there is no need to pull data, after update config must update shardsOwn at same time
		for shard, gid := range kv.nextConfig.Shards {
			if gid == kv.gid {
				kv.shardsOwn[shard] = true
			} else {
				kv.shardsOwn[shard] = false
			}
		}
		DPrintf("%v-%v update config from %v-%v, %+v, kv.db:%v", kv.gid, kv.me, kv.config.Num, kv.nextConfig.Num, kv.nextConfig, kv.db)
		// TODO here to delete data

		kv.config = kv.nextConfig
		DPrintf("%v-%v shardsOwn:%v", kv.gid, kv.me, kv.shardsOwn)


	}
	// to pull data from shardsIn
	if _, isLeader := kv.rf.GetState(); isLeader {
		for shard, gid := range kv.shardsIn {
			go kv.pullData(shard, gid, currentConfig)
		}
	}
	kv.unlock("installConfig")
}

func (kv *ShardKV) pullData(shard int, gid int, config shardctrler.Config) {
	args := MigrateDataArgs {
		Shard:     shard,
		ConfigNum: config.Num+1,
	}
	if servers, ok := config.Groups[gid]; ok {
		for i:=0; i<len(servers); i++ {
			srv := kv.make_end(servers[i])
			var reply MigrateDataReply
			//DPrintf("%v-%v, ready to Migrate, args:%+v", kv.gid, kv.me, args)
			ok := srv.Call("ShardKV.MigrateData", &args, &reply)
			if ok && reply.Success {
				DPrintf("%v-%v ready to sync MDreply:%+v", kv.gid, kv.me, reply)
				kv.rf.Start(reply)
			}
		}
	}
}

func (kv *ShardKV) updateShardsInAndOut(old, new shardctrler.Config) {
	// DPrintf("%v-%v, oldConfig:%v", kv.gid, kv.me, old)
	kv.updateShardsOut(old, new)
	kv.updateShardsIn(old, new)
}

func (kv *ShardKV) updateShardsOut(old, new shardctrler.Config) {
	if old.Num == 0 {
		return
	}
	for shard, gid := range new.Shards {
		// out
		if old.Shards[shard] == kv.gid && gid != kv.gid {
			if len(kv.shardsOut[new.Num]) == 0 {
				kv.shardsOut[new.Num] = make(map[int]map[string]string)
			}
			if len(kv.shardsOut[new.Num][shard]) == 0 {
				kv.shardsOut[new.Num][shard] = make(map[string]string)
			}
			kv.shardsOut[new.Num][shard] = kv.getShardData(shard)
		}
	}
}

func (kv *ShardKV) updateShardsIn (old, new shardctrler.Config) {
	if old.Num == 0 {
		return
	}
	for shard, gid := range new.Shards {
		if gid == kv.gid && old.Shards[shard] != kv.gid {
			kv.shardsIn[shard] = old.Shards[shard]
		}
	}
}

func (kv *ShardKV) getShardData(shard int) map[string]string {
	data := make(map[string]string)
	for k, v := range kv.db {
		if key2shard(k) == shard {
			data[k] = v
		}
	}
	return data
}


func (kv *ShardKV) installSnapshot(snapshot []byte) {
	kv.lock("")
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil ||
		d.Decode(&kv.latestCommand) != nil ||
		 d.Decode(&kv.shardsOut) != nil ||
		  d.Decode(&kv.shardsIn) != nil ||
		   d.Decode(&kv.shardsOwn) != nil ||
		    d.Decode(&kv.config) != nil ||
			 d.Decode(&kv.nextConfig) != nil{
		DPrintf("%v-%v fails to recover from snapshot", kv.gid, kv.me)
	}
	kv.unlock("")
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- struct{}{}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateDataReply{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan struct{}, 1)
	kv.notifyChMap = make(map[int]chan Op)
	kv.latestCommand = make(map[int64]int)
	kv.db = make(map[string]string)
	kv.clerk = shardctrler.MakeClerk(ctrlers)
	kv.shardsOwn = make(map[int]bool)
	kv.shardsIn = make(map[int]int)
	kv.shardsOut = make(map[int]map[int]map[string]string)
	kv.config = shardctrler.Config {
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}
	kv.nextConfig = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}

	kv.installSnapshot(persister.ReadSnapshot())
	DPrintf("%v-%v, shardsOwn:%v", kv.gid, kv.me, kv.shardsOwn)
	go kv.run()
	go kv.fetchConfig()
	// go kv.updateConfig()

	return kv
}



func (kv *ShardKV) MigrateData(args *MigrateDataArgs, reply *MigrateDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Success = false
		reply.Err = ErrWrongLeader
		return
	}

	kv.lock("MigrateData")
	defer kv.unlock("MigrateData")

	if args.ConfigNum > kv.config.Num {
		reply.Success = false
		return
	}
	// 确保大家都是最新的config
	data := make(map[string]string)
	latestCommand := make(map[int64]int)
	for key, value := range kv.shardsOut[args.ConfigNum][args.Shard] {
		data[key] = value
	}
	for k, v := range kv.latestCommand {
		latestCommand[k] = v
	}
	reply.Data = data
	reply.LatestCommand = latestCommand
	reply.Shard = args.Shard
	// record the leader's config num while fetch data
	reply.ConfigNum = args.ConfigNum
	reply.Success = true
}

//func (kv *ShardKV) updateConfig() {
//	for {
//		select {
//		case <-kv.killCh:
//			return
//		default:
//			kv.lock("waitPullData")
//			if kv.nextConfig.Num == kv.config.Num+1 {
//				if len(kv.shardsIn[kv.nextConfig.Num]) == 0 {
//					DPrintf("%v-%v update config from %v-%v, %+v, kv.db:%v", kv.gid, kv.me, kv.config.Num, kv.nextConfig.Num, kv.nextConfig, kv.db)
//					kv.config = kv.nextConfig
//				} else {
//					// DPrintf("%v-%v, shardsIn[%v]:%+v", kv.gid, kv.me, kv.nextConfig.Num, kv.shardsIn[kv.nextConfig.Num])
//				}
//			} else {
//				// DPrintf("%v-%v, config:%+v, nextConfig:%+v", kv.gid, kv.me, kv.config, kv.nextConfig)
//			}
//			kv.unlock("waitPullData")
//			time.Sleep(50 * time.Millisecond)
//		}
//	}
//}

func (kv *ShardKV) fetchConfig() {
	for {
		select {
		case <-kv.killCh:
			return
		default:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.lock("fetchConfigNum")
				currentConfigNum := kv.config.Num
				kv.unlock("fetchConfigNum")
				nextConfig := kv.clerk.Query(currentConfigNum+1)

				// to ensure install config in order
				kv.lock("fetchConfig")
				if nextConfig.Num == kv.config.Num+1 {
					DPrintf("%v-%v ready to sync new config(%+v)", kv.gid, kv.me, nextConfig)
					// not responsible for shardsOut
					if kv.config.Num != 0 {
						for shard, gid := range nextConfig.Shards {
							if kv.config.Shards[shard] == kv.gid && gid != kv.gid {
								kv.shardsOwn[shard] = false
							}
						}
					}
					// if it's the first config, update shardsOwn
					if nextConfig.Num == 1 {
						for shard, gid := range nextConfig.Shards {
							if gid==kv.gid {
								kv.shardsOwn[shard] = true
							}
						}
					}
					DPrintf("%v-%v, shardsOwn:%v", kv.gid, kv.me, kv.shardsOwn)
					kv.unlock("fetchConfig")
					kv.rf.Start(nextConfig)
				} else {
					kv.unlock("fetchConfig")
				}
			}
			time.Sleep(FetchConfigInterval * time.Millisecond)
		}
	}
}

// while the server not hold the shard or the server is migrating its data, should not respond the client
func (kv *ShardKV) shouldResponse(key string, configNum int) (bool, Err) {
	if kv.holdKey(key) {
		if len(kv.shardsIn) == 0 {
			return true, OK
		} else {
			return false, ErrMigrating
		}
	}
	return false, ErrWrongGroup
}

func (kv *ShardKV) holdKey(key string) bool {
	shard := key2shard(key)

	return kv.shardsOwn[shard]
}

func (kv *ShardKV) getNotifyCh(index int) chan Op {
	kv.lock("")
	defer kv.unlock("")

	if notifyCh, ok := kv.notifyChMap[index]; ok {

		return notifyCh
	}
	notifyCh := make(chan Op, 1)
	kv.notifyChMap[index] = notifyCh
	return notifyCh
}

func (kv *ShardKV) isDuplicate(clientId int64, seq int) bool {
	if latestSeq, ok := kv.latestCommand[clientId]; ok {
		if latestSeq >= seq {
			return true
		}
	}
	return false
}

func (kv *ShardKV) lock(s string) {
	//if s!="" {
	//	DPrintf("%v-%v, try kv.lock %v", kv.gid, kv.me, s)
	//}
	kv.mu.Lock()
	//if s!="" {
	//	DPrintf("%v-%v, kv.lock %v", kv.gid, kv.me, s)
	//}
}

func (kv *ShardKV) unlock(s string) {
	//if s!="" {
	//	DPrintf("%v-%v, kv.unlock %v", kv.gid, kv.me, s)
	//}
	kv.mu.Unlock()
}