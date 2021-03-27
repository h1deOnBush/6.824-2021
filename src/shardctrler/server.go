package shardctrler


import (
	"6.824/raft"
	"log"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const TIMEOUT = 500

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	killCh  chan struct{}
	// Your data here.

	configs []Config // indexed by config num
	// for client duplication detect
	latestCommand map[int64]int
	notifyChMap map[int]chan Op
}


type Op struct {
	// Your data here.
	// join/leave/move/query
	Name string
	Args interface{}
	ClientId int64
	Seq int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	originalOp := Op{
		Name:     "Join",
		Args:     *args,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	reply.WrongLeader = sc.handle(originalOp)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	originalOp := Op{
		Name:     "Leave",
		Args:     *args,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	reply.WrongLeader = sc.handle(originalOp)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	originalOp := Op{
		Name:     "Move",
		Args:     *args,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	reply.WrongLeader = sc.handle(originalOp)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	originalOp := Op{
		Name:     "Query",
		Args:     *args,
		Seq :     -1,
	}
	reply.WrongLeader = sc.handle(originalOp)
	if !reply.WrongLeader {
		sc.mu.Lock()
		defer sc.mu.Unlock()

		if args.Num >=0 &&  args.Num<=sc.getLatestConfig().Num {
			reply.Config = sc.configs[args.Num]
		} else {
			reply.Config = sc.getLatestConfig()
		}
		DPrintf("query success, config: %+v", reply.Config)
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.killCh <- struct{}{}
}



// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) updateConfig(operation string, args interface{}) {
	newConfig := sc.createNewConfig()
	switch operation {
	case "Join":
		joinArgs := args.(JoinArgs)
		for gid, servers := range joinArgs.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = append(newConfig.Groups[gid], servers...)
			sc.rebalance(&newConfig, "Join", gid)
		}
		DPrintf("join success, config: %+v", newConfig)
	case "Leave":
		leaveArgs := args.(LeaveArgs)
		for _, gid := range leaveArgs.GIDs {
			delete(newConfig.Groups, gid)
			sc.rebalance(&newConfig, "Leave", gid)
		}
		DPrintf("leave success, config: %+v", newConfig)
	case "Move":
		moveArgs := args.(MoveArgs)
		if _, ok := newConfig.Groups[moveArgs.GID]; ok {
			newConfig.Shards[moveArgs.Shard] = moveArgs.GID
		}
		DPrintf("move success, config: %+v", newConfig)
	default:
		log.Fatal("unsupported operation", operation)
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) run() {
	for {
		select {
		case <-sc.killCh:
			return
		case applyMsg := <-sc.applyCh:
			if !applyMsg.CommandValid {
				continue
			}
			op := applyMsg.Command.(Op)
			sc.mu.Lock()
			if op.Seq<0 || !sc.isDuplicate(op.ClientId, op.Seq) {
				if op.Seq>=0 {
					sc.updateConfig(op.Name, op.Args)
					sc.latestCommand[op.ClientId] = op.Seq
				}
				if notifyCh, ok := sc.notifyChMap[applyMsg.CommandIndex]; ok {
					notifyCh <- op
				}
			}
			sc.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.latestCommand = make(map[int64]int)
	sc.killCh = make(chan struct{})
	sc.notifyChMap = make(map[int]chan Op)
	// Your code here.
	go sc.run()
	return sc
}

func (sc *ShardCtrler) rebalance(cfg *Config, op string, gid int) {
	gidShards := sc.getRealDistribution(cfg)
	switch op {
	case "Join":
		if len(cfg.Groups) == 1 {
			for i:=0; i<NShards; i++ {
				cfg.Shards[i] = gid
			}
			return
		}
		avg := NShards / len(cfg.Groups)
		for i:=0; i<avg; i++ {
			maxGid := sc.getMaxShardGid(gidShards)
			cfg.Shards[gidShards[maxGid][0]] = gid
			gidShards[maxGid] = gidShards[maxGid][1:]
			DPrintf("maxGid:%v, cfg.Shards: %+v, gidShards: %v", maxGid, cfg.Shards, gidShards)
		}
	case "Leave":
		shards := gidShards[gid]
		delete(gidShards, gid)
		if len(cfg.Groups) == 0 {
			cfg.Shards = [NShards]int{}
			return
		}
		for _, shard := range shards {
			minGid := sc.getMinShardGid(gidShards)
			cfg.Shards[shard] = minGid
			gidShards[minGid] = append(gidShards[minGid], shard)
		}
	}
}

func (sc *ShardCtrler) getRealDistribution(cfg *Config) map[int][]int {
	gidShards := make(map[int][]int)

	// important, for situation shards < groups
	for gid, _ := range cfg.Groups {
		gidShards[gid] = []int{}
	}

	for shard, gid := range cfg.Shards {
		gidShards[gid] = append(gidShards[gid], shard)
	}

	return gidShards
}

func (sc *ShardCtrler) getMaxShardGid(gidShards map[int][]int) int {
	maxCount := 0
	maxGid := 0
	for gid, shards := range gidShards {
		if len(shards) > maxCount {
			maxCount = len(shards)
			maxGid = gid
		}
	}
	return maxGid
}

func (sc *ShardCtrler) getMinShardGid(gidShards map[int][]int) int {
	minCount := NShards
	minGid := 0
	for gid, shards := range gidShards {
		if len(shards) < minCount {
			minCount = len(shards)
			minGid = gid
		}
	}
	return minGid
}

func (sc *ShardCtrler) getLatestConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) createNewConfig() Config {
	lastConfig := sc.getLatestConfig()
	newGroups := make(map[int][]string)
	for gid, servers := range lastConfig.Groups {
		newGroups[gid] = append([]string{}, servers...)
	}
	newConfig := Config{
		Num: lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: newGroups,
	}
	return newConfig
}

func (sc *ShardCtrler) isDuplicate(clientId int64, seq int) bool {
	if latestSeq, ok := sc.latestCommand[clientId]; ok {
		if latestSeq >= seq {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) handle(originalOp Op) bool {
	index, _, isLeader := sc.rf.Start(originalOp)
	if !isLeader {
		return true
	}
	DPrintf("sc %v, receive client op: %+v", sc.me,originalOp)
	notifyCh := sc.getNotifyCh(index)
	select {
	case op := <-notifyCh:
		close(notifyCh)
		sc.mu.Lock()
		delete(sc.notifyChMap, index)
		sc.mu.Unlock()
		if op.ClientId != originalOp.ClientId || op.Seq != originalOp.Seq {
			return true
		}
		return false
	case <-time.After(time.Duration(TIMEOUT)*time.Millisecond):
		if sc.isDuplicate(originalOp.ClientId, originalOp.Seq) {
			return false
		}
		return true
	}
}

func (sc *ShardCtrler) getNotifyCh(index int) chan Op {
	sc.mu.Lock()
	if notifyCh, ok := sc.notifyChMap[index]; ok {
		sc.mu.Unlock()
		return notifyCh
	}
	notifyCh := make(chan Op, 1)
	sc.notifyChMap[index] = notifyCh
	sc.mu.Unlock()
	return notifyCh
}