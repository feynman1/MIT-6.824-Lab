package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) GetLastLogIndex() int {
	if rf.LastIncludedIndex == 0 {
		return len(rf.Log)
	}
	//4 3 2
	//2 1 0
	//li:1,len:3,4=3+1,
	return len(rf.Log) + rf.LastIncludedIndex
}

func (rf *Raft) GetLastLogTerm() int {
	if len(rf.Log) == 0 {
		if rf.LastIncludedIndex == 0 {
			return 0
		} else {
			return rf.LastIncludedTerm
		}
	}
	return rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) GetLog(index int) Entry {
	if index < 1 {
		log.Fatal("getLog index lower than 1")
	}
	if rf.LastIncludedIndex == 0 {
		return rf.Log[index-1]
	}
	if rf.LastIncludedIndex > (index - 1) {
		log.Fatal("getLog index overflow")
	}
	//4 3 2
	//2 1 0
	//li:1,index:3,3-1-1,
	return rf.Log[index-rf.LastIncludedIndex-1]
}

func (rf *Raft) GetLogTerm(index int) int {
	if index < 1 {
		log.Fatal("getLogTerm index lower than 1")
	}
	if rf.LastIncludedIndex == 0 {
		if len(rf.Log) == 0 {
			return 0
		}
		return rf.Log[index-1].Term
	}
	if rf.LastIncludedIndex > index {
		log.Fatal("LII:", rf.LastIncludedIndex, " index-1:", index-1, " getLogTerm index overflow")
	} else if rf.LastIncludedIndex == index {
		return rf.LastIncludedTerm
	}
	//4 3 2
	//2 1 0
	//i:3,li:1,[1],[i-li-1]
	return rf.Log[index-rf.LastIncludedIndex-1].Term
}

func (rf *Raft) SendInstallSnapshot(index int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[index].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	reply.Ok = ok
	return ok
}

func (rf *Raft) SendLogAppend(index int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[index].Call("Raft.LogAppend", args, reply)
	reply.Ok = ok
	if !ok {
		reply.FollowerId = index
	}
	return ok
}
