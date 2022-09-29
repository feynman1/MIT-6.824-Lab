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

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//---------------------Data Structure---------------------
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	ClientId int64
	LeaderId int
	SeqId    int
	Key      string
	Value    string
	Index    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap            map[int64]int     //map[clientId] seqId
	kvPersist         map[string]string //k-v map
	waitChanMap       map[int]chan Op   //map[lastIndex] Chan
	lastIncludedIndex int
}

//--------------------------operation--------------------------

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
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
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		if args.SeqId != replyOp.SeqId || args.ClientId != replyOp.ClientId {
			reply.Err = ErrWrongLeader
			return
		}
		value, exist := kv.kvPersist[args.Key]
		if !exist {
			reply.Err = ErrNoKey
			return
		}
		reply.Err = OK
		reply.Value = value
		return
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
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

func (kv *KVServer) IsDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//code
	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) GetWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChanMap[index]
	if !exist {
		kv.waitChanMap[index] = make(chan Op, 1)
		ch = kv.waitChanMap[index]
	}
	return ch
}

func (kv *KVServer) PersistSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

func (kv *KVServer) DecodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	w := bytes.NewBuffer(data)
	d := labgob.NewDecoder(w)
	var kp map[string]string
	var sm map[int64]int
	if d.Decode(&kp) == nil {
		kv.kvPersist = kp
	}
	if d.Decode(&sm) == nil {
		kv.seqMap = sm
	}
}

//--------------------------running--------------------------

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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvPersist = make(map[string]string)
	kv.seqMap = make(map[int64]int)
	kv.waitChanMap = make(map[int]chan Op)
	kv.lastIncludedIndex = -1
	//load snapshot
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	go kv.ApplyMsgHandlerLoop()
	return kv
}

func (kv *KVServer) ApplyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if msg.CommandIndex <= kv.lastIncludedIndex {
					return
				}
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if !kv.IsDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case PUT:
						kv.kvPersist[op.Key] = op.Value
					case APPEND:
						kv.kvPersist[op.Key] += op.Value
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}
				//Encode snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapshot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				kv.GetWaitCh(index) <- op
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
