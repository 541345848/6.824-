package kvsrv

import (
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	Mutex sync.Mutex
	EqualMap map[int64]string
	Map map[string]string
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()
	/*if _, exists := kv.EqualMap[args.Pre]; exists {
		//log.Printf("delete %v",args.Pre)
		delete(kv.EqualMap, args.Pre)
	}
	if value, exists := kv.EqualMap[args.Uuid]; exists {
		reply.Value = value
		return 
	}*/
	if value, ok := kv.Map[args.Key]; ok {
		reply.Value = value
		
	}else{
		reply.Value = ""
	}
	//kv.EqualMap[args.Uuid] = reply.Value
	//log.Printf("uudi %v , kv.EqualMap:%v",args.Uuid, len(kv.EqualMap[args.Uuid]))
	//log.Printf("uudi %v , kv.Map:%v",args.Uuid, len(kv.Map))
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()
	if _, exists := kv.EqualMap[args.Pre]; exists {
		//log.Printf("delete %v",args.Pre)
		delete(kv.EqualMap, args.Pre)
	}
	
	if _, exists := kv.EqualMap[args.Uuid]; exists {
		return 
	}
	kv.Map[args.Key]  = args.Value
	kv.EqualMap[args.Uuid] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Mutex.Lock()
	defer kv.Mutex.Unlock()
	if _, exists := kv.EqualMap[args.Pre]; exists {
		//log.Printf("delete %v",args.Pre)
		delete(kv.EqualMap, args.Pre)
	}
	old := kv.Map[args.Key]
	if value, exists := kv.EqualMap[args.Uuid]; exists {
		reply.Value = value
		return 
	}
	
	kv.Map[args.Key] = kv.Map[args.Key]+args.Value
	reply.Value = old
	kv.EqualMap[args.Uuid] = reply.Value
}

func (kv *KVServer) Print() {
	for {
		time.Sleep(1*time.Second)
		kv.Mutex.Lock()
		log.Printf("%v",len(kv.EqualMap))
		kv.Mutex.Unlock()
	}
}
func StartKVServer() *KVServer {
	kv := new(KVServer)
	*kv = KVServer{
		Mutex : sync.Mutex{},
		Map : map[string]string{},
		EqualMap : make(map[int64]string),
	}
	// You may need initialization code here.
	//go kv.Print()
	return kv
}
