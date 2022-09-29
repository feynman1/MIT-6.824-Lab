package shardctrler

import (
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

//-------------------------------Data Structure-------------------------------
const (
	JOIN        = "Join"
	MOVE        = "Move"
	LEAVE       = "Leave"
	QUERY       = "Query"
	INVALIEDGID = 0
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	seqMap      map[int64]int   //map[clientId] seqId
	waitChanMap map[int]chan Op //map[lastIndex] Chan
	configs     []Config        // indexed by config num
}

type Op struct {
	// Your data here.
	OpType      string
	ClientId    int64
	SeqId       int
	QueryNum    int
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
}

//-------------------------------Running-------------------------------

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChanMap = make(map[int]chan Op)

	go sc.ApplyMsgHandlerLoop()
	return sc
}

func (sc *ShardCtrler) ApplyMsgHandlerLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if !sc.IsDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case MOVE:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.MoveMsgHandler(op.MoveGid, op.MoveShard))
					case JOIN:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinMsgHandler(op.JoinServers))
					case LEAVE:
						sc.configs = append(sc.configs, *sc.LeaveMsgHandler(op.LeaveGids))
						sc.seqMap[op.ClientId] = op.SeqId
					}
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}
				sc.GetWaitCh(index) <- op
			}
		}
	}
}

func (sc *ShardCtrler) JoinMsgHandler(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for gid, newservers := range servers {
		newGroups[gid] = newservers
	}
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}
	if len(GroupMap) == 0 {
		return &Config{
			Groups: newGroups,
			Num:    len(sc.configs),
			Shards: [10]int{},
		}
	}
	return &Config{
		Groups: newGroups,
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, lastConfig.Shards),
	}
}

func (sc *ShardCtrler) MoveMsgHandler(gid int, shard int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs)}
	newGroups := make(map[int][]string)
	newShard := [10]int{}
	for gid_, serverList := range lastConfig.Groups {
		newGroups[gid_] = serverList
	}
	for shard_, gids := range lastConfig.Shards {
		newShard[shard_] = gids
	}
	newShard[shard] = gid
	newConfig.Groups = newGroups
	newConfig.Shards = newShard
	return &newConfig
}

func (sc *ShardCtrler) LeaveMsgHandler(gids []int) *Config {
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for _, gid := range gids {
		delete(newGroups, gid)
	}
	GroupMap := make(map[int]int)
	newShard := lastConfig.Shards
	for gid := range newGroups {
		if !leaveMap[gid] {
			GroupMap[gid] = 0
		}
	}
	for shard, gid := range newShard {
		if gid != 0 {
			if leaveMap[gid] {
				newShard[shard] = INVALIEDGID
			} else {
				GroupMap[gid]++
			}
		}
	}
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroups,
	}
}

//-------------------------------Service-------------------------------

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//start a command to rf
	command := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: JOIN, JoinServers: args.Servers}
	lastIndex, _, _ := sc.rf.Start(command)
	ch := sc.GetWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, lastIndex)
		sc.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		return
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//start a command to rf
	command := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: LEAVE, LeaveGids: args.GIDs}
	lastIndex, _, _ := sc.rf.Start(command)
	ch := sc.GetWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, lastIndex)
		sc.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		return
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//start a command to rf
	command := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: MOVE, MoveGid: args.GID, MoveShard: args.Shard}
	lastIndex, _, _ := sc.rf.Start(command)
	ch := sc.GetWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, lastIndex)
		sc.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		return
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//start a command to rf
	command := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: QUERY, QueryNum: args.Num}
	lastIndex, _, _ := sc.rf.Start(command)
	ch := sc.GetWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, lastIndex)
		sc.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			reply.Err = ErrWrongLeader
			return
		}
		sc.mu.Lock()
		reply.Err = OK
		sc.seqMap[replyOp.ClientId] = replyOp.SeqId
		if replyOp.QueryNum == -1 || replyOp.QueryNum >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[replyOp.QueryNum]
		}
		sc.mu.Unlock()
		return
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

//-------------------------------Utils-------------------------------

func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(GroupMap)

	//find overload gid and free its overload
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}
		if GroupMap[sortGids[i]] > target {
			changeNum := GroupMap[sortGids[i]] - target
			for shard, gid := range lastShards {
				if gid == sortGids[i] {
					lastShards[shard] = INVALIEDGID
					changeNum--
				}
				if changeNum == 0 {
					break
				}
			}
			GroupMap[sortGids[i]] = target
		}
	}
	//reload gid
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}
		if GroupMap[sortGids[i]] < target {
			changeNum := target - GroupMap[sortGids[i]]
			for shard, gid := range lastShards {
				if gid == INVALIEDGID {
					lastShards[shard] = sortGids[i]
					changeNum--
				}
				if changeNum == 0 {
					break
				}
			}
			GroupMap[sortGids[i]] = target
		}
	}
	return lastShards
}

func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)
	gidSlice := make([]int, 0, length)
	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GroupMap[gidSlice[j]] < GroupMap[gidSlice[j-1]] || (GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

func moreAllocations(length int, remainder int, i int) bool {
	if i < length-remainder {
		return true
	} else {
		return false
	}

}

func (kv *ShardCtrler) IsDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//code
	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardCtrler) GetWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChanMap[index]
	if !exist {
		kv.waitChanMap[index] = make(chan Op, 1)
		ch = kv.waitChanMap[index]
	}
	return ch
}
