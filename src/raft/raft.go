package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"




// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == 0 {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor, lastIncludedTerm, lastIncludedIndex int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("readPersist error")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.voteFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm  = lastIncludedTerm
		rf.mu.Unlock()
	}
}



func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	if rf.currentTerm > args.Term {
		DPrintf("[server %v, role %v, term %v] leader [%v] term is smaller\n", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.changeState(2)
	}
	// if leader term == currentTerm,then restart timer, for condition that leader need to sync nextIndex[] with follower
	rf.restartTimer()

	// in unreliable network condition
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex+1
		reply.ConflictTerm = -1
		return
	}

	// leader's prevLogIndex too large, larger than the last index in follower, need to decrease
	if args.PrevLogIndex>rf.getLastIdx() {
		DPrintf("[server %v, role %v, term %v], prevLogIndex(%v) too large, larger than the last index in follower, need to decrease\n", rf.me, rf.state, rf.currentTerm, args.PrevLogIndex)
		reply.Success = false
		reply.ConflictIndex = rf.getLastIdx()+1
		reply.ConflictTerm = -1
		return
	}


	// entry's term in prevLogIndex is different from prevLogTerm
	var prevLogTerm int
	if rf.getRealIdx(args.PrevLogIndex) == 0 {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		DPrintf("[server %v], snapshotIdx(%v), snapshotTerm(%v)", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
		prevLogTerm = rf.logs[rf.getRealIdx(args.PrevLogIndex)].Term
	}
	if prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		conflictTerm := prevLogTerm
		reply.ConflictTerm = conflictTerm
		conflictIndex := args.PrevLogIndex
		for rf.logs[rf.getRealIdx(conflictIndex)-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		DPrintf("[server %v, role %v, term %v], entry in preLogIndex(%v) with different term(%v)\n", rf.me, rf.state, rf.currentTerm, args.PrevLogIndex, conflictTerm)
		return
	}
	// if an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it
	unmatchedIdx := -1
	for idx, entry := range args.Entries {
		if len(rf.logs) < rf.getRealIdx(entry.Index)+1 ||
			rf.logs[rf.getRealIdx(entry.Index)].Term != entry.Term {
			// unmatched log found
			unmatchedIdx = idx
			break
		}
	}

	if unmatchedIdx != -1 {
		// there are unmatched entries,truncate unmatched Follower entries, and apply Leader entries
		rf.logs = rf.logs[:(rf.getRealIdx(args.PrevLogIndex) + 1 + unmatchedIdx)]
		rf.logs = append(rf.logs, args.Entries[unmatchedIdx:]...)
		DPrintf("[server %v, role %v, term %v] Append Entry successfully from [%v], (%v)-(%v)", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.PrevLogIndex + 1 + unmatchedIdx, args.PrevLogIndex+len(args.Entries))
	} else {
		if len(args.Entries) == 0 {
			DPrintf("[server %v, role %v, term %v] receive successful heartbeat from [%v]", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		} else {
			DPrintf("[server %v, role %v, term %v] Append Entry successfully from [%v], already have (%v)-(%v)", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getLastIdx() {
			rf.setCommitIndexAndApply(rf.getLastIdx())
		} else {
			rf.setCommitIndexAndApply(args.LeaderCommit)
		}
	}
	rf.changeState(2)
	reply.Success = true
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[server %v, role %v, term %v] reject vote for [%v], candidate term is small", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(2)
		rf.voteFor = -1
	}
	if rf.voteFor!=args.CandidateId && rf.voteFor!=-1 {
		reply.VoteGranted = false
		DPrintf("[server %v, role %v, term %v] reject vote for [%v], already voted for [%v]\n", rf.me, rf.state, rf.currentTerm, args.CandidateId, rf.voteFor)
		return
	}
	// to judge whether candidate's log is more up-to-date than follower's
	lastLogIndex := rf.getLastIdx()
	var lastLogTerm int
	if lastLogIndex == rf.lastIncludedIndex {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.logs[rf.getRealIdx(lastLogIndex)].Term
	}
	if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		DPrintf("[server %v, role %v, term %v] reject vote for [%v], not up to date, lastLogTerm(%v), args.lastLogTerm(%v)\n", rf.me, rf.state, rf.currentTerm, args.CandidateId, lastLogTerm, args.LastLogTerm)
		return
	} else if lastLogTerm == args.LastLogTerm {
		if lastLogIndex > args.LastLogIndex {
			reply.VoteGranted = false
			DPrintf("[server %v, role %v, term %v] reject vote for [%v], not up to date, lastLogIndex(%v), args.lastLogIndex(%v)\n", rf.me, rf.state, rf.currentTerm, args.CandidateId, lastLogIndex, args.LastLogIndex)
			return
		}
	}
	// reset electionTimer
	rf.restartTimer()
	rf.voteFor = args.CandidateId
	DPrintf("[server %v, role 2, term %v], vote for [%v]\n", rf.me, rf.currentTerm, args.CandidateId)
	reply.VoteGranted = true
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == 0
	if isLeader {
		index = rf.getLastIdx()+1
		DPrintf("[server %v, role %v, term %v] receive command(%v) from client, index:(%v)", rf.me, rf.state, rf.currentTerm, command, index)

		rf.logs = append(rf.logs, Entry{Command: command, Term: term, Index: index})
		rf.persist()
		// why this step
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// initialization for leader election
	rf.currentTerm = 1
	rf.applyCh = applyCh
	rf.state = 2
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initial with sentinel node
	rf.logs = []Entry{{Term: 0, Index: 0}}
	rand.Seed(time.Now().UnixNano())
	rf.restartTimer()
	rf.voteFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// create a goroutine to start leader election
	go rf.ticker()
	go rf.heartbeats()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) ticker()  {
	for !rf.killed() {
		rf.mu.Lock()
		if time.Now().Sub(rf.tick) >  rf.electionTimeout {
			switch rf.state {
			case 1:
				rf.startElection()
			case 2:
				rf.changeState(1)
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}


func (rf *Raft) startElection() {
	rf.restartTimer()
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.voteCount = 1
	rf.persist()
	lastLogIndex := rf.getLastIdx()
	var lastLogTerm int
	if lastLogIndex == rf.lastIncludedIndex {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.logs[rf.getRealIdx(lastLogIndex)].Term
	}
	currentTerm := rf.currentTerm
	args := RequestVoteArgs {
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.collectVotes(args)
}

func (rf *Raft) restartTimer() {
	rf.tick = time.Now()
	rf.electionTimeout = time.Duration(electionTimeout + rand.Intn(extra)) * time.Millisecond
}

func (rf *Raft) changeState(state uint32) {
	if state == rf.state {
		return
	}
	rf.state = state
	switch state {
	case 0:
		for i:=0; i<len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastIdx()+1
			rf.matchIndex[i] = 0
		}
		rf.sendHeartbeat()
	case 1:
		rf.startElection()
	case 2:
		rf.restartTimer()
		rf.voteFor = -1
	}
}

func (rf *Raft) heartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == 0 {
			rf.sendHeartbeat()
			rf.mu.Unlock()
			time.Sleep(heartbeatTimeout * time.Millisecond)
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				if rf.state != 0 {
					rf.mu.Unlock()
					return
				}
				currentTerm := rf.currentTerm
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs {
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}
					DPrintf("[server %v, role %v, term %v] ready to send installSnapshot to [%v]", rf.me, rf.state, rf.currentTerm, i)
					rf.mu.Unlock()
					var reply InstallSnapshotReply
					rf.syncSnapshot(i, &args, &reply)
					return
				}
				prevLogIndex := rf.nextIndex[i]-1
				var prevLogTerm int
				if prevLogIndex <= rf.lastIncludedIndex {
					prevLogTerm = rf.lastIncludedTerm
				} else {
					prevLogTerm = rf.logs[rf.getRealIdx(prevLogIndex)].Term
				}
				//DPrintf("prevLogIndex(%v), prevLogTerm(%v)", prevLogIndex, prevLogTerm)
				state := rf.state
				entries := make([]Entry, len(rf.logs[(rf.getRealIdx(prevLogIndex)+1):]))
				copy(entries, rf.logs[(rf.getRealIdx(prevLogIndex)+1):])
				args := AppendEntriesArgs {
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				var reply AppendEntriesReply
				if len(entries) == 0 {
					DPrintf("[server %v, role %v, term %v] send heartbeat to [%v]\n", rf.me, state, currentTerm, i)
				} else {
					DPrintf("[server %v, role %v, term %v] Append entry to [%v] from %v to %v\n", rf.me, state, currentTerm, i, prevLogIndex+1, prevLogIndex+len(entries))
				}
				ok := rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				if rf.currentTerm != args.Term || rf.state!=0 {
					rf.mu.Unlock()
					return
				}
				if ok {
					if reply.Success {
						DPrintf("[server %v, role %v, term %v] receive successful append entry response from [%v]\n", rf.me, rf.state, rf.currentTerm, i)
						rf.nextIndex[i] = args.PrevLogIndex+len(args.Entries)+1
						rf.matchIndex[i] = rf.nextIndex[i]-1
						// update committed
						for N := rf.getLastIdx(); N > rf.max(rf.commitIndex, rf.lastIncludedIndex); N-- {
							count := 0
							for _, matchIndex := range rf.matchIndex {
								if matchIndex >= N &&  rf.logs[rf.getRealIdx(N)].Term == rf.currentTerm {
									count += 1
								}
							}

							if count > len(rf.peers)/2 {
								// most of nodes agreed on rf.log[i]
								rf.setCommitIndexAndApply(N)
								break
							}
						}
					} else {
						if reply.Term > rf.currentTerm {
							DPrintf("[server %v, role %v, term %v] leader term is obsolete, change to follower", rf.me, rf.state, rf.currentTerm)
							rf.currentTerm = reply.Term
							rf.persist()
							rf.changeState(2)
						} else {
							// can't find a entry that has same index and term with args, decrease prevLogIndex
							DPrintf("[server %v, role %v, term %v] inconsistent entry send to [%v], now to decrease preLogIndex(%v), conflict index: (%v), conflictTerm (%v)", rf.me, rf.state, rf.currentTerm, i, args.PrevLogIndex, reply.ConflictIndex, reply.ConflictTerm)
							rf.nextIndex[i] = reply.ConflictIndex
							if reply.ConflictTerm != -1 {
								for j := rf.getRealIdx(args.PrevLogIndex); j >= 1; j-- {
									if rf.logs[j-1].Term == reply.ConflictTerm {
										// in next trial, check if log entries in ConflictTerm matches
										rf.nextIndex[i] = j+rf.lastIncludedIndex
										break
									}
								}
							}
						}
					}
				} else {
					//DPrintf("[server %v, role %v, term %v] not receive response from [%v] for append entry, delay or drop may happen\n", rf.me, state, currentTerm, i)
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}


func (rf *Raft) syncSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		if rf.currentTerm != args.Term || rf.state != 0 {
			DPrintf("[server %v, role %v, term %v] sync snapshot to [%v] failed, term or role has already changed", rf.me, rf.state, rf.currentTerm, server)
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.persist()
			rf.changeState(2)
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex+1
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.mu.Unlock()
	} else {
		//DPrintf("[server %v, role %v, term %v], sendInstallSnapshot to [%v] not receive response\n", rf.me, rf.state, rf.currentTerm, server)
	}
}


func (rf *Raft) collectVotes(args RequestVoteArgs) {
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				DPrintf("[server %v, role 1, term %v], send request vote to [%v]", rf.me, args.Term, i)
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					if args.Term != rf.currentTerm || rf.state != 1{
						DPrintf("[server %v, role %v, term %v], term has changed to (%v), or state has changed, obsolete response\n", rf.me, rf.state, args.Term, rf.currentTerm)
						rf.mu.Unlock()
						return
					}
					if reply.VoteGranted {
						rf.voteCount++
						if rf.voteCount > len(rf.peers)/2 {
							DPrintf("[server %v, role 1, term %v], get majority votes\n", rf.me, args.Term)
							rf.changeState(0)
						}
						DPrintf("[server %v, role 1, term %v], receive votes from [%v]", rf.me, args.Term, i)

					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.persist()
							rf.changeState(2)
							rf.mu.Unlock()
							return
						}
					}
					rf.mu.Unlock()
				} else {
					//DPrintf("[server %v, role %v, term %v] not receive vote response from [%v], drop or delay may happens", rf.me, state, currentTerm, i)
				}
			}(i)
		}
	}
}

func (rf *Raft) setCommitIndexAndApply(commitIndex int)  {
	rf.commitIndex = commitIndex
	// apply to state machine, with a new goroutine
	if rf.commitIndex > rf.lastApplied {
		var entriesToApply []Entry
		if rf.lastIncludedIndex > rf.lastApplied {
			DPrintf("[server %v, role %v, term %v], apply log from %v to %v\n", rf.me, rf.state, rf.currentTerm, rf.lastIncludedIndex+1, rf.commitIndex)
			entriesToApply = append(entriesToApply, rf.logs[(rf.getRealIdx(rf.lastIncludedIndex)+1):(rf.getRealIdx(rf.commitIndex)+1)]...)
		} else {
			DPrintf("[server %v, role %v, term %v], apply log from %v to %v\n", rf.me, rf.state, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
			entriesToApply = append(entriesToApply, rf.logs[(rf.getRealIdx(rf.lastApplied)+1):(rf.getRealIdx(rf.commitIndex)+1)]...)
		}


		go func(startIdx int, entries []Entry) {
			for _, entry := range entries {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.applyCh <- msg
				// update lastApplied after send it to channel
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).service receives a snapshot from rafe peer,
	// service receive snapshot from raft peer
	if lastIncludedIndex == rf.lastIncludedIndex && lastIncludedTerm == rf.lastIncludedTerm {
		//rf.trimLogFromIdx(lastIncludedIndex)
		//rf.lastIncludedIndex = lastIncludedIndex
		//rf.lastIncludedTerm = lastIncludedTerm
		//rf.persistStateAndSnapshot(snapshot)
		return true
	}
	return false
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("service told service [%v] to generate snapshot, snapshotIdx(%v)\n", rf.me, index)
	if index <= rf.lastIncludedIndex {
		DPrintf("[server %v, role %v, term %v], snapshot is obsolete, index(%v) is smaller than current snapshotIndex(%v)\n", rf.me, rf.state, rf.currentTerm, index, rf.lastIncludedIndex)
		return
	}
	rf.lastIncludedTerm = rf.logs[rf.getRealIdx(index)].Term
	rf.trimLogFromIdx(index)
	rf.lastIncludedIndex = index
	rf.persistStateAndSnapshot(snapshot)
	DPrintf("[server %v, role %v, term %v] generate snapshot successfully, current snapshotIdx(%v), current first index(%v), snapshotTerm(%v)\n", rf.me, rf.state, rf.currentTerm, rf.lastIncludedIndex, rf.getFirstIdx(), rf.lastIncludedTerm)
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//
//	follower receive snapshot from leader, use the applyCh to send the snapshot(included in ApplyMsg) to the service
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.changeState(2)
		return
	}
	rf.restartTimer()
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		// update snapshot and persist
		rf.trimLogFromIdx(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.persistStateAndSnapshot(args.Data)

		rf.commitIndex = rf.max(rf.commitIndex, rf.lastIncludedIndex)
		rf.lastApplied = rf.max(rf.lastApplied, rf.lastIncludedIndex)
		DPrintf("[server %v, role %v, term %v] install snapshot successfully, snapshotIdx(%v), snapshotTerm(%v)", rf.me, rf.state, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)
		// notify the upper service to switch to snapshot
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	} else {
		// snapshot is obsolete
		DPrintf("[server %v, role %v, term %v], install snapshot failed, current snapshotIdx(%v), args.snapshotIdx(%v)\n", rf.me, rf.state, rf.currentTerm, rf.lastIncludedIndex, args.LastIncludedIndex)
	}
}


//
// leader tell follower to sync snapshot to prevent follower from lag far behind leader
//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


//
// truncate log from the given index
//
func (rf *Raft) trimLogFromIdx(index int) {
	idx := rf.getRealIdx(index)
	if idx >= len(rf.logs) {
		// discard all log entries
		DPrintf("[server %v, role %v, term %v] discard all its log\n", rf.me, rf.state, rf.currentTerm)
		rf.logs = []Entry{{Term: 0, Index: 0}}
	} else {
		DPrintf("[server %v, role %v, term %v] truncate log from (%v)\n", rf.me, rf.state, rf.currentTerm, index)
		rf.logs = rf.logs[idx+1:]
		rf.logs = append([]Entry{{Term: 0, Index: 0}}, rf.logs...)
	}
}

// according to log index get the real index in logs
func (rf *Raft) getRealIdx(index int) int {
	return index-rf.lastIncludedIndex
}

// get current last entry index
func (rf *Raft) getLastIdx() int {
	if len(rf.logs) == 1 {
		return rf.lastIncludedIndex
	} else {
		return rf.logs[len(rf.logs)-1].Index
	}
}

func (rf *Raft) getFirstIdx() int {
	if len(rf.logs) == 1 {
		return rf.lastIncludedIndex
	} else {
		return rf.logs[1].Index
	}
}

func (rf *Raft) max(a,b int) int {
	if a>b {
		return a
	}
	return b
}
