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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//---------------------------------data structure------------------------------

//
// A Go object implementing a single Raft peer.
//
// peer's roleType
const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

//heartbeat state
const (
	FREE = iota
	DONE
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	chanMu    sync.Mutex          // lock to protect goroutine shared access to the applyChan

	// Your data here (2A, 2B, 2C).
	//2A
	RoleType    int
	VotedFor    int
	CurrentTerm int
	IsHeartbeat int
	VoteCount   int
	TimerNumber int
	RandomTime  time.Duration
	//2B
	CommitIndex int
	LastApplied int
	NextIndex   []int
	MatchIndex  []int
	Log         []Entry
	applyChan   chan ApplyMsg
	//2D
	LastIncludedIndex int
	LastIncludedTerm  int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type HeartbeatArgs struct {
	//code 2A
	Term     int
	LeaderId int
	//code 2B
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
	EntryIndex   int
}

type HeartbeatReply struct {
	//code 2A
	Term    int
	Success bool
	Ok      bool
	//code 2B
	FollowerId int
	EntryIndex int
	//code 2C
	ConflictIndex int
	ConflictTerm  int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term         int
	CandidateId  int
	LastLogIndex int
	//2B
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Ok          bool
}

type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Success bool
	Term    int // follower's term
}

//---------------------------------persist------------------------------

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	//note:order of Decode must be same with Encode
	var VotedFor int
	var CurrentTerm int
	var Log []Entry
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&VotedFor) != nil {
		fmt.Println("decode error")
	} else {
		rf.VotedFor = VotedFor
	}
	if d.Decode(&CurrentTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.CurrentTerm = CurrentTerm
	}
	if d.Decode(&Log) != nil {
		fmt.Println("decode error")
	} else {
		rf.Log = Log
	}
	if d.Decode(&LastIncludedIndex) != nil {
		fmt.Println("decode error")
	} else {
		rf.LastIncludedIndex = LastIncludedIndex
	}
	if d.Decode(&LastIncludedTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.LastIncludedTerm = LastIncludedTerm
	}
}

//---------------------------------running------------------------------

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.RoleType == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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
	if rf.killed() {
		return index, term, false
	}
	if rf.RoleType != LEADER {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log = append(rf.Log, Entry{Term: rf.CurrentTerm, Command: command})
	index = rf.GetLastLogIndex()
	term = rf.CurrentTerm
	isLeader = true
	rf.persist()
	//fmt.Println("leader update his log to ", len(rf.Log))

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

func (rf *Raft) Killed() bool {
	return rf.killed()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//if server doesn't receive heartbeat or a candidate fail,start a new candidate
	}
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
	//code 2A
	rf.CurrentTerm = 0
	rf.RoleType = FOLLOWER
	rf.VotedFor = -1
	rf.IsHeartbeat = FREE
	rf.TimerNumber = 0
	rf.RandomTime = time.Millisecond * time.Duration(100*(rand.Intn(10)+10))
	rf.VoteCount = 0
	//code 2B
	rf.LastApplied = 0
	rf.CommitIndex = 0
	rf.Log = make([]Entry, 0)
	rf.applyChan = applyCh
	//code 2D
	rf.LastIncludedTerm = 0
	rf.LastIncludedIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.LastIncludedIndex > 0 {
		rf.LastApplied = rf.LastIncludedIndex
	}
	// start ticker goroutine to start elections
	go rf.UpdateLastAppliedTimer()
	go rf.SelectionTimer()
	go rf.LeaderHeartbeatTimer()

	return rf
}

//---------------------------------snapshot------------------------------

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.CommitIndex || index <= rf.LastIncludedIndex {
		return
	}
	//fmt.Println(1)
	rf.LastIncludedTerm = rf.GetLogTerm(index)
	newLog := make([]Entry, 0)
	for i := index + 1; i <= rf.GetLastLogIndex(); i++ {
		newLog = append(newLog, rf.GetLog(i))
	}
	rf.Log = newLog
	if rf.LastIncludedIndex < index {
		rf.LastIncludedIndex = index
	}
	if rf.LastApplied < index {
		rf.LastApplied = index
	}
	if rf.CommitIndex < index {
		rf.CommitIndex = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.Success = true
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.RoleType = FOLLOWER
		rf.CurrentTerm = args.Term
		//don't forget persist()
		rf.persist()
	}
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	newLog := make([]Entry, 0)
	for i := args.LastIncludedIndex + 1; i <= rf.GetLastLogIndex(); i++ {
		newLog = append(newLog, rf.GetLog(i))
	}
	rf.Log = newLog
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.IsHeartbeat = DONE
	if rf.LastApplied < args.LastIncludedIndex {
		rf.LastApplied = args.LastIncludedIndex
	}
	if rf.CommitIndex < args.LastIncludedIndex {
		rf.CommitIndex = args.LastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	applyMsg := ApplyMsg{SnapshotValid: true, CommandValid: false, Snapshot: args.Data,
		SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
	rf.mu.Unlock()
	rf.chanMu.Lock()
	rf.applyChan <- applyMsg
	rf.chanMu.Unlock()
}

func (rf *Raft) LeaderSendSnapshot(server int) {
	rf.mu.Lock()
	if rf.RoleType != LEADER || rf.LastIncludedIndex < rf.NextIndex[server] {
		go rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{Term: rf.CurrentTerm, LeaderId: rf.me, LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm: rf.LastIncludedTerm, Data: rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.RoleType != LEADER || rf.CurrentTerm != args.Term {
			return
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.RoleType = FOLLOWER
			rf.VotedFor = -1
			rf.persist()
			return
		}
		if reply.Success {
			rf.NextIndex[server] = args.LastIncludedIndex + 1
			rf.MatchIndex[server] = args.LastIncludedIndex
		}
	}
}

//---------------------------------selection------------------------------

func (rf *Raft) SelectionTimer() {
	for !rf.killed() {
		time.Sleep(rf.RandomTime)
		rf.mu.Lock()
		if rf.RoleType != LEADER && rf.IsHeartbeat != DONE {
			//vote myself
			rf.RoleType = CANDIDATE
			rf.CurrentTerm++
			rf.VotedFor = -1
			rf.VoteCount = 0
			rf.VoteCount++
			rf.VotedFor = rf.me
			rf.persist()
			rf.IsHeartbeat = FREE
			//fmt.Println(rf.me, "start a selection,term:", rf.CurrentTerm)
			args := &RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me}
			args.LastLogIndex = rf.GetLastLogIndex()
			args.LastLogTerm = rf.GetLastLogTerm()
			rf.mu.Unlock()
			go rf.Selection(args)
		} else {
			rf.IsHeartbeat = FREE
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Selection(args *RequestVoteArgs) {
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.CandidateSendVote(index, args)
	}
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.RoleType = FOLLOWER
		rf.VotedFor = -1
	}
	if args.Term < rf.CurrentTerm {
		return
	} else {
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			lastLogTerm := rf.GetLastLogTerm()
			if lastLogTerm > args.LastLogTerm {
				//fmt.Println(111)
				//!!!:Even if args.Term == rf.CurrentTerm,there is a case that args.LastLogTerm may still < lastLogTerm
				return
			} else if lastLogTerm < args.LastLogTerm {
				reply.VoteGranted = true
				rf.VotedFor = args.CandidateId
			} else {
				if rf.GetLastLogIndex() > args.LastLogIndex {
					return
				} else {
					reply.VoteGranted = true
					rf.VotedFor = args.CandidateId
				}
			}
		}
	}
	rf.persist()
}

func (rf *Raft) CandidateSendVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.RoleType != CANDIDATE || args.Term != rf.CurrentTerm {
		return
	}
	if ok {
		if reply.VoteGranted == true && args.Term == rf.CurrentTerm {
			rf.VoteCount++
			if rf.VoteCount > len(rf.peers)/2 {
				rf.RoleType = LEADER
				//rf.IsHeartbeat = DONE
				rf.NextIndex = make([]int, len(rf.peers))
				for index, _ := range rf.NextIndex {
					rf.NextIndex[index] = rf.GetLastLogIndex() + 1
					//fmt.Println(5, ":", rf.NextIndex[index])
				}
				rf.MatchIndex = make([]int, len(rf.peers))
				rf.MatchIndex[rf.me] = rf.GetLastLogIndex()
			}
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.RoleType = FOLLOWER
				rf.VotedFor = -1
				rf.CurrentTerm = reply.Term
				//rf.IsHeartbeat = DONE
				rf.persist()
				return
			}
		}
	}
}

//---------------------------------appendEntries------------------------------

func (rf *Raft) LeaderHeartbeatTimer() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.RoleType != LEADER {
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			for index, _ := range rf.peers {
				if index == rf.me {
					rf.MatchIndex[index] = rf.GetLastLogIndex()
					continue
				}
				//send snapshot
				if rf.NextIndex[index] <= rf.LastIncludedIndex {
					go rf.LeaderSendSnapshot(index)
				} else {
					//send logs
					go rf.LeaderSendLogs(index)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) UpdateLastAppliedTimer() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		applyMsgs := make([]ApplyMsg, 0)
		rf.mu.Lock()
		if rf.LastIncludedIndex > 0 && rf.LastApplied < rf.LastIncludedIndex {
			rf.LastApplied = rf.LastIncludedIndex
		}
		if rf.LastApplied < rf.CommitIndex {
			for rf.CommitIndex > rf.LastApplied {
				rf.LastApplied++
				applyMsgs = append(applyMsgs, ApplyMsg{SnapshotValid: false, CommandValid: true, Command: rf.GetLog(rf.LastApplied).Command,
					CommandIndex: rf.LastApplied})
			}
			rf.mu.Unlock()
			rf.chanMu.Lock()
			for _, a := range applyMsgs {
				rf.applyChan <- a
			}
			rf.chanMu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

// LogAppend Rpc handler
func (rf *Raft) LogAppend(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerId = rf.me
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.RoleType = FOLLOWER
		rf.VotedFor = -1
		rf.persist()
	}
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	if args.PreLogIndex < rf.LastIncludedIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.GetLastLogIndex()
		return
	}
	if rf.GetLastLogIndex() < args.PreLogIndex {
		reply.Success = false
		reply.Term = args.Term
		reply.ConflictIndex = rf.GetLastLogIndex()
		reply.ConflictTerm = -1
		return
	}
	//fmt.Println(2)
	if args.PreLogIndex > 0 && rf.GetLogTerm(args.PreLogIndex) != args.PreLogTerm {
		reply.Success = false
		reply.Term = args.Term
		reply.ConflictTerm = rf.GetLogTerm(args.PreLogIndex)
		bound := -1
		if rf.LastIncludedIndex > 0 {
			bound = rf.LastIncludedIndex
		} else {
			bound = 1
		}
		for index := bound; index <= args.PreLogIndex; index++ {
			if rf.GetLogTerm(index) == reply.ConflictTerm {
				reply.ConflictIndex = index
				break
			}
		}
		return
	}
	reply.Term = args.Term
	reply.EntryIndex = args.EntryIndex
	reply.FollowerId = rf.me
	rf.IsHeartbeat = DONE
	if args.PreLogIndex < rf.GetLastLogIndex() {
		if rf.LastIncludedIndex > 0 {
			// 4 3 2
			// 2 1 0
			//pl:4,li:1,[:3],[:pl-li]
			rf.Log = rf.Log[:args.PreLogIndex-rf.LastIncludedIndex]
		} else {
			//4 3 2 1
			//3 2 1 0
			//pl:3,[:3]
			rf.Log = rf.Log[:args.PreLogIndex]
		}
	}
	for _, p := range args.Entries {
		rf.Log = append(rf.Log, p)
	}
	rf.persist()
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit <= args.EntryIndex {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = args.EntryIndex
		}
		//fmt.Println(rf.me, "update his commitIndex to ", rf.CommitIndex)
	}
	reply.Success = true
}

// LeaderSendLogs Rpc sender
func (rf *Raft) LeaderSendLogs(server int) {
	rf.mu.Lock()
	if rf.RoleType != LEADER || rf.NextIndex[server] <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	args := &HeartbeatArgs{Term: rf.CurrentTerm, LeaderId: rf.me,
		LeaderCommit: rf.CommitIndex, EntryIndex: rf.GetLastLogIndex()}
	args.Entries = make([]Entry, 0)
	args.PreLogIndex = rf.NextIndex[server] - 1
	if args.PreLogIndex > 0 {
		args.PreLogTerm = rf.GetLogTerm(args.PreLogIndex)
	}
	if rf.LastIncludedIndex == 0 {
		for i := rf.NextIndex[server] - 1; i < len(rf.Log); i++ {
			log := Entry{Command: rf.Log[i].Command, Term: rf.Log[i].Term}
			args.Entries = append(args.Entries, log)
		}
	} else {
		for i := rf.NextIndex[server] - rf.LastIncludedIndex - 1; i < len(rf.Log); i++ {
			log := Entry{Command: rf.Log[i].Command, Term: rf.Log[i].Term}
			args.Entries = append(args.Entries, log)
		}
	}
	rf.mu.Unlock()
	reply := &HeartbeatReply{}
	ok := rf.SendLogAppend(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.RoleType != LEADER || rf.CurrentTerm != args.Term {
		return
	}
	if ok {
		if reply.Success == false {
			//fmt.Println("Follower", reply.FollowerId, " reject hb/log from", rf.me)
			if reply.Term > rf.CurrentTerm {
				//fmt.Println(rf.me, "become a follower")
				rf.RoleType = FOLLOWER
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.persist()
			} else {
				if reply.ConflictTerm != -1 {
					conflictIndex := -1
					bound := -1
					if rf.LastIncludedIndex == 0 {
						bound = 1
					} else {
						bound = rf.LastIncludedIndex
					}
					for i := args.PreLogIndex; i >= bound; i-- {
						//fmt.Println(5)
						if rf.GetLogTerm(i) == reply.ConflictTerm {
							conflictIndex = i
							break
						}
					}
					if conflictIndex == -1 {
						rf.NextIndex[reply.FollowerId] = reply.ConflictIndex
						//fmt.Println(1, ":", rf.NextIndex[reply.FollowerId])
					} else {
						rf.NextIndex[reply.FollowerId] = conflictIndex + 1
						//fmt.Println(2, ":", rf.NextIndex[reply.FollowerId])
					}
				} else {
					rf.NextIndex[reply.FollowerId] = reply.ConflictIndex + 1
					//fmt.Println(3, ":", rf.NextIndex[reply.FollowerId])
				}
			}
		} else {
			//fmt.Println(rf.me, "succeed to send log/hb to follower", reply.FollowerId)
			//fmt.Println(4, ":", rf.NextIndex[reply.FollowerId])
			rf.NextIndex[reply.FollowerId] = args.PreLogIndex + len(args.Entries) + 1
			rf.MatchIndex[reply.FollowerId] = rf.NextIndex[reply.FollowerId] - 1
			rf.UpdateCommitIndex()
		}
	}
}

func (rf *Raft) UpdateCommitIndex() {
	//fine N,most servers commitIndex > N,update N
	if rf.CommitIndex < rf.GetLastLogIndex() {
		bound := -1
		if rf.LastIncludedIndex > 0 {
			bound = rf.LastIncludedIndex
		} else {
			bound = rf.CommitIndex + 1
		}
		for i := rf.GetLastLogIndex(); i >= bound; i-- {
			sum := 0
			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					sum++
					continue
				}
				if rf.MatchIndex[j] >= i {
					sum++
				}
			}
			//fmt.Println(3)

			if sum > len(rf.peers)/2 && rf.GetLogTerm(i) == rf.CurrentTerm {
				rf.CommitIndex = i
				break
			}
		}
	}
}
