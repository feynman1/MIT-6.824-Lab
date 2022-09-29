package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"strconv"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

//-----------------------------Data Structure-----------------------------
type Shard struct {
	ConfigNum int
	KvMap     map[string]string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType         string
	ClientId       int64
	LeaderId       int
	SeqId          int
	Key            string
	Value          string
	Index          int
	Config         shardctrler.Config
	MigrationReply MigrateReply
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
	seqMap            map[int64]int //map[clientId] seqId
	shardsPersist     []Shard
	waitChanMap       map[int]chan Op //map[lastIndex] Chan
	lastIncludedIndex int
	mck               *shardctrler.Clerk
	cfg               shardctrler.Config
	//lab 4b
	outShards   map[int]map[int]Shard //map[configNum]shard
	inShard     map[int]int           //map[shard]configNum
	GarbageList map[int]map[int]bool  //map[configNum]map[shard]bool
}

type MigrateArgs struct {
	ConfigNum int
	Shard     int
}

type MigrateReply struct {
	Err       Err
	Shard     Shard
	SeqMap    map[int64]int //map[clientId] seqId
	ConfigNum int
	ShardNum  int
}

//-----------------------------Service-----------------------------

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.rf.Killed() || !isLeader {
		//fmt.Println("g1", " gid:", kv.gid, "shard:", shard)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else if ok := kv.cfg.Shards[shard]; ok != kv.gid {
		//fmt.Println("g2")
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.shardsPersist[shard].KvMap == nil {
		reply.Err = ErrWrongShardNotArrive
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//start a command to rf
	command := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: GET, Key: args.Key}
	lastIndex, _, _ := kv.rf.Start(command)
	ch := kv.GetWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChanMap, lastIndex)
		kv.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//Case:ErrWrongGroup
		if replyOp.OpType == ErrWrongGroup {
			//fmt.Println("g3")
			reply.Err = ErrWrongGroup
			return
		}
		//Case:ErrWrongLeader
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			//fmt.Println("g4")
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		value, exist := kv.shardsPersist[shard].KvMap[args.Key]
		kv.mu.Unlock()
		if !exist {
			//fmt.Println("g5")
			reply.Err = ErrNoKey
			return
		}
		//fmt.Println("g6")
		reply.Err = OK
		reply.Value = value
		return
	case <-timer.C:
		//fmt.Println("g7")
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.rf.Killed() || !isLeader {
		//fmt.Println("g1", " gid:", kv.gid, "shard:", shard)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else if ok := kv.cfg.Shards[shard]; ok != kv.gid {
		//fmt.Println("a2")
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.shardsPersist[shard].KvMap == nil {
		//fmt.Println("a3")
		reply.Err = ErrWrongShardNotArrive
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//start a command to rf
	command := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: args.Op, Key: args.Key, Value: args.Value}
	lastIndex, _, _ := kv.rf.Start(command)
	ch := kv.GetWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChanMap, lastIndex)
		kv.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//Case:ErrWrongGroup
		if replyOp.OpType == ErrWrongGroup {
			//fmt.Println("a4")
			reply.Err = ErrWrongGroup
			return
		}
		//Case:ErrWrongLeader
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			//fmt.Println("a5")
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		return
	case <-timer.C:
		//fmt.Println("a6")
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) SendMigrationReply(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.ShardNum, reply.ConfigNum = OK, args.Shard, args.ConfigNum
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		//fmt.Println("no leader gid:", kv.gid)
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Println("leader gid:", kv.gid)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum >= kv.cfg.Num {
		reply.Err = ErrWrongConfigNum
		return
	}
	reply.Shard, reply.SeqMap = kv.copyDBAndSeqMap(args.Shard, args.ConfigNum)
	return
}

func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.ShardNum, reply.ConfigNum = OK, args.Shard, args.ConfigNum
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else if _, ok := kv.outShards[args.ConfigNum]; !ok {
		reply.Err = ErrWrongConfigNum
		kv.mu.Unlock()
		return
	} else if _, ok := kv.outShards[args.ConfigNum][args.Shard]; !ok {
		reply.Err = ErrWrongConfigNum
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//start a command to rf
	command := Op{ClientId: nrand(), SeqId: args.Shard, OpType: "GC", Key: strconv.Itoa(args.ConfigNum), Value: ""}
	lastIndex, _, _ := kv.rf.Start(command)
	ch := kv.GetWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChanMap, lastIndex)
		kv.mu.Unlock()
	}()
	//wait for reply from rf
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//Case:ErrWrongGroup
		if replyOp.OpType == ErrWrongGroup {
			reply.Err = ErrWrongGroup
			return
		} else if replyOp.OpType == ErrWrongLeader {
			reply.Err = ErrWrongLeader
			return
		} //Case:ErrWrongLeader
		reply.Err = OK
		return
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

//-----------------------------Utils-----------------------------

//Server<->Raft utils

func (kv *ShardKV) IsDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//code
	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) GetWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChanMap[index]
	if !exist {
		kv.waitChanMap[index] = make(chan Op, 1)
		ch = kv.waitChanMap[index]
	}
	return ch
}

func (kv *ShardKV) PersistSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.seqMap)
	e.Encode(kv.inShard)
	e.Encode(kv.cfg)
	e.Encode(kv.shardsPersist)
	e.Encode(kv.outShards)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) DecodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	w := bytes.NewBuffer(data)
	d := labgob.NewDecoder(w)
	var sm map[int64]int
	var inMap map[int]int
	var config shardctrler.Config
	var sP []Shard
	var oS map[int]map[int]Shard
	if d.Decode(&sm) == nil {
		kv.seqMap = sm
	}
	if d.Decode(&inMap) == nil {
		kv.inShard = inMap
	}
	if d.Decode(&config) == nil {
		kv.cfg = config
	}
	if d.Decode(&sP) == nil {
		kv.shardsPersist = sP
	}
	if d.Decode(&oS) == nil {
		kv.outShards = oS
	}
}

func (kv *ShardKV) updateDBWithMigrateData(migration MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//CommandType:Migration
	if migration.ConfigNum == (kv.cfg.Num - 1) {
		delete(kv.inShard, migration.ShardNum)
		//case?
		if kv.shardsPersist[migration.ShardNum].KvMap == nil {
			kv.shardsPersist[migration.ShardNum] = migration.Shard
			for k, v := range migration.SeqMap {
				if _, ok := kv.seqMap[k]; !ok {
					kv.seqMap[k] = v
				} else if kv.seqMap[k] < v {
					kv.seqMap[k] = v
				}
			}
			//fmt.Println("gid:", kv.gid, " sP:", kv.shardsPersist)
			if _, ok := kv.GarbageList[migration.ConfigNum]; !ok {
				kv.GarbageList[migration.ConfigNum] = make(map[int]bool)
			}
			kv.GarbageList[migration.ConfigNum][migration.ShardNum] = true
		}
	}
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
}

func (kv *ShardKV) Killed() bool {
	return kv.rf.Killed()
}

func (kv *ShardKV) checkCfgChanged(configNum int) (shardctrler.Config, bool) {
	query := kv.mck.Query(configNum)
	if query.Num == configNum {
		return query, true
	}
	return shardctrler.Config{}, false
}

func (kv *ShardKV) UpdateOutAndInShard(config shardctrler.Config) {
	//fmt.Println("s6")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num <= kv.cfg.Num {
		return
	}
	oldCfg := kv.cfg
	kv.cfg = config
	for shard, gid := range config.Shards {
		if kv.gid != gid {
			if oldCfg.Shards[shard] == kv.gid {
				if kv.outShards[oldCfg.Num] == nil {
					kv.outShards[oldCfg.Num] = make(map[int]Shard)
				}
				kv.outShards[oldCfg.Num][shard] = kv.shardsPersist[shard]
			}
			continue
		}
		if oldCfg.Shards[shard] == 0 && oldCfg.Num == 0 {
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = kv.cfg.Num
		}
		if oldCfg.Num > 0 && oldCfg.Shards[shard] != kv.gid {
			kv.inShard[shard] = oldCfg.Num
		}
	}
	for k, _ := range kv.outShards[oldCfg.Num] {
		if kv.shardsPersist[k].KvMap != nil {
			kv.shardsPersist[k].KvMap = nil
		}
	}
	//fmt.Println("gid:", kv.gid, "configNum:", kv.cfg.Num, "SP:", kv.shardsPersist)
}

func (kv *ShardKV) copyDBAndSeqMap(shard int, configNum int) (Shard, map[int64]int) {
	s := Shard{ConfigNum: configNum, KvMap: make(map[string]string)}
	seqMap := make(map[int64]int)
	if kv.outShards[configNum][shard].KvMap == nil {
		//fmt.Println("gid:", kv.gid, "kvmap is nil")
	}
	for k, v := range kv.outShards[configNum][shard].KvMap {
		s.KvMap[k] = v
	}
	for k, v := range kv.seqMap {
		seqMap[k] = v
		// delete(kv.seqMap,k) : wait gc
	}
	return s, seqMap
}

func (kv *ShardKV) GCTool(configNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.outShards[configNum]; ok {
		if _, ok := kv.outShards[configNum][shard]; ok {
			delete(kv.outShards[configNum], shard)
		}
	}
}

//-----------------------------Running-----------------------------

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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.cfg = shardctrler.Config{}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.seqMap = make(map[int64]int)
	kv.waitChanMap = make(map[int]chan Op)
	kv.inShard = make(map[int]int)
	kv.GarbageList = make(map[int]map[int]bool)
	kv.shardsPersist = make([]Shard, NShards)
	kv.outShards = make(map[int]map[int]Shard)
	kv.lastIncludedIndex = -1
	//load snapshot
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}
	//start communication
	go kv.ApplyMsgHandlerLoop()
	go kv.PullCtrlerConfig()
	go kv.PullServerShard()
	go kv.SendGC()
	return kv
}

// ApplyMsgHandlerLoop Raft->server
func (kv *ShardKV) ApplyMsgHandlerLoop() {
	for !kv.rf.Killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if msg.CommandIndex <= kv.lastIncludedIndex {
					return
				}
				op := msg.Command.(Op)
				//CommandType:config
				if op.OpType == NEWCONFIG {
					kv.UpdateOutAndInShard(op.Config)
				} else if op.OpType == MIGRATION {
					kv.updateDBWithMigrateData(op.MigrationReply)
				} else {
					//CommandType:Op
					index := msg.CommandIndex
					if op.OpType == "GC" {
						cfgNum, _ := strconv.Atoi(op.Key)
						kv.GCTool(op.SeqId, cfgNum)
					} else {
						shard := key2shard(op.Key)
						if ok := kv.cfg.Shards[shard]; ok != kv.gid {
							op.OpType = ErrWrongGroup
						} else {
							if !kv.IsDuplicate(op.ClientId, op.SeqId) {
								kv.mu.Lock()
								switch op.OpType {
								case PUT:
									kv.shardsPersist[shard].KvMap[op.Key] = op.Value
								case APPEND:
									kv.shardsPersist[shard].KvMap[op.Key] += op.Value
								}
								kv.seqMap[op.ClientId] = op.SeqId
								kv.mu.Unlock()
							}
						}
					}
					//Encode snapshot
					if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
						snapshot := kv.PersistSnapshot()
						kv.rf.Snapshot(msg.CommandIndex, snapshot)
					}
					kv.GetWaitCh(index) <- op
				}
			} else if msg.SnapshotValid {
				//decode snapshot
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.DecodeSnapshot(msg.Snapshot)
					kv.lastIncludedIndex = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

// PullCtrlerConfig server->ctrler->server->raft
func (kv *ShardKV) PullCtrlerConfig() {
	for !kv.Killed() {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		//if last config hasn't applied,return.
		if !isLeader || len(kv.inShard) > 0 {
			//fmt.Println("s9 ", len(kv.inShard))
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		//kv.mu.Unlock()
		//fmt.Println("s7")
		//pull config if it changed
		if config, ok := kv.checkCfgChanged(kv.cfg.Num + 1); ok {
			op := Op{OpType: NEWCONFIG, Config: config}
			kv.rf.Start(op)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//PullServerShard Server->Server
func (kv *ShardKV) PullServerShard() {
	for !kv.Killed() {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.inShard) == 0 {
			kv.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for shard, configNum := range kv.inShard {
			go func(shard int, config shardctrler.Config) {
				args := MigrateArgs{Shard: shard, ConfigNum: config.Num}
				gid := config.Shards[shard]
				servers := config.Groups[gid]
				//fmt.Println("start migration to ", gid)
				for _, server := range servers {
					end := kv.make_end(server)
					var reply = MigrateReply{}
					ok := end.Call("ShardKV.SendMigrationReply", &args, &reply)
					//fmt.Println("ok:", ok, "reply err:", reply.Err)
					if ok && reply.Err == OK {
						//fmt.Println("Migration configNum:", reply.Shard.ConfigNum, "kv configNum:", kv.cfg.Num)
						op := Op{OpType: MIGRATION, MigrationReply: reply}
						kv.rf.Start(op)
						break
					}
				}
			}(shard, kv.mck.Query(configNum)) // kv.cfg might change,must use Query to get cfg
		}
		kv.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *ShardKV) SendGC() {
	for !kv.rf.Killed() {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.GarbageList) == 0 {
			kv.mu.Unlock()
			continue
		}
		for configNum, list := range kv.GarbageList {
			for shard, ok := range list {
				if !ok {
					continue
				}
				go func(oldConfig shardctrler.Config, shard int) {
					args := &MigrateArgs{Shard: shard, ConfigNum: configNum}
					reply := &MigrateReply{}
					gid := oldConfig.Shards[shard]
					for _, server := range oldConfig.Groups[gid] {
						end := kv.make_end(server)
						if ok := end.Call("ShardKV.GarbageCollection", args, reply); ok && reply.Err == OK {
							delete(kv.GarbageList[configNum], shard)
							if len(kv.GarbageList[configNum]) == 0 {
								delete(kv.GarbageList, configNum)
							}
						}
					}
				}(kv.mck.Query(configNum), shard)
			}
		}
		kv.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}
