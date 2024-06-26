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
	//	"bytes"

	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type Entry struct {
	Term int
	Cmd  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int

	applyCh    chan ApplyMsg
	state      int
	nextIndex  []int
	matchIndex []int

	followerChannel  chan int
	candidateChannel chan int
	leaderChannel    chan int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term       int
	Success    bool
	LogSuccess bool
	NextIndex  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//log.Printf("follow %v", args)
	if args.PrevLogIndex == 0 || len(args.Entries) == 0 {
		reply.LogSuccess = true
		rf.commitIndex = args.LeaderCommit
		rf.log = append(rf.log[:max(args.PrevLogIndex, 0)], args.Entries...)
		reply.NextIndex = len(rf.log)
	} else {
		for index := len(rf.log) - 1; index >= 0; index-- {
			if index+1 == args.PrevLogIndex && args.PrevLogTerm == rf.log[index].Term {
				reply.LogSuccess = true
				rf.commitIndex = args.LeaderCommit
				rf.log = append(rf.log[:max(args.PrevLogIndex, 0)], args.Entries...)
				reply.NextIndex = len(rf.log)
			}
		}
	}

	//log.Printf("follower %v, %v", rf.me, rf.log)
	if args.Term > rf.currentTerm || args.Term == rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.followerChannel <- FOLLOWER
		reply.Success = true
		return
	}
	reply.Success = false
	return

	//log.Printf("term %v %v append from %v\n", rf.currentTerm, rf.me, args.LeaderId)
}

func (rf *Raft) sendAppendEntries() {
	results := make(chan bool, len(rf.peers))
	l_results := make(chan bool, len(rf.peers))
	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	log := rf.log
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		prevLogTerm := 0
		if len(log) != 0 && rf.nextIndex[server] > 0 {
			prevLogTerm = log[max(rf.nextIndex[server]-1, 0)].Term
		}
		//fmt.Printf("%v %v %v\n", log, rf.nextIndex[server]-1, log[max(0, 0):])
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server],
			PrevLogTerm:  prevLogTerm,
			Entries:      log[max(rf.nextIndex[server], 0):],
			LeaderCommit: leaderCommit,
		}
		//fmt.Printf("leader %v send %v,  %v\n", rf.nextIndex, server, args)

		var reply AppendEntriesReply
		if server != rf.me {
			go func(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
				ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
				if ok {
					results <- reply.Success
					l_results <- reply.LogSuccess
					rf.mu.Lock()
					if reply.LogSuccess {
						rf.nextIndex[server] = reply.NextIndex
						rf.matchIndex[server] = args.LeaderCommit
					} else {
						rf.nextIndex[server] = max(rf.nextIndex[server]-1, 0)
					}
					rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.followerChannel <- FOLLOWER
					}
				} else {
					results <- false
				}
			}(&args, &reply, server)
		}
	}

	count_t := 1
	count_f := len(rf.peers) - 1
	for i := 0; i < len(rf.peers)-1; i++ {
		result := <-results // 接收一个返回值
		if result {
			count_t = count_t + 1
			if count_t > len(rf.peers)/2 {
				rf.state = LEADER
				//log.Printf("term %v, %v be leader", args.Term, args.LeaderId)
				rf.leaderChannel <- LEADER
				break
			}
		} else {
			count_f = count_f - 1
			if count_f <= len(rf.peers)/2 {
				//fmt.Printf("count %v len(rf.peers)/2 %v \n", count, len(rf.peers)/2)
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.followerChannel <- FOLLOWER
				break
			}
		}
	}
	count_l := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		result := <-l_results
		if result {
			count_l = count_l + 1
			if count_l > len(rf.peers)/2 {
				rf.commitIndex = len(log)
			}
		}
	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//log.Printf("term %v server %v want to vote for server %v,but alread vote for %v\n", rf.currentTerm, rf.me, args.CandidateId, rf.votedFor)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if args.LastLogIndex >= rf.lastApplied && args.LastLogTerm >= lastLogTerm && ((rf.currentTerm == args.Term && rf.votedFor == -1) || args.Term > rf.currentTerm) {
		rf.followerChannel <- FOLLOWER
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote() {
	results := make(chan bool, len(rf.peers))
	rf.mu.Lock()
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[rf.lastApplied-1].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastApplied,
		LastLogTerm:  lastLogTerm,
	}
	rf.votedFor = rf.me
	rf.mu.Unlock()
	//log.Printf("term %v, %v start vote grand", rf.currentTerm, rf.me)
	for server := 0; server < len(rf.peers); server++ {
		var reply RequestVoteReply
		if server != rf.me {
			go func(args *RequestVoteArgs, reply *RequestVoteReply, server int) {
				ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
				if ok {
					rf.mu.Lock()
					rf.currentTerm = int(math.Max(float64(reply.Term), float64(rf.currentTerm)))
					rf.mu.Unlock()
					results <- reply.VoteGranted
				} else {
					results <- false
				}
			}(&args, &reply, server)
		}
	}
	count_t := 1
	count_f := len(rf.peers) - 1
	for i := 0; i < len(rf.peers)-1; i++ {
		result := <-results // 接收一个返回值
		if result {
			count_t = count_t + 1
			if count_t > len(rf.peers)/2 {
				rf.state = LEADER
				rf.leaderChannel <- LEADER
				go rf.sendAppendEntries()
				break
			}
		} else {
			count_f = count_f - 1
			if count_f <= len(rf.peers)/2 {
				//fmt.Printf("count %v len(rf.peers)/2 %v \n", count, len(rf.peers)/2)
				rf.state = FOLLOWER
				rf.followerChannel <- FOLLOWER
				break
			}
		}
		if result {

		}
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isLeader = true
	}
	if !isLeader {
		return index, term, isLeader
	}
	term = rf.currentTerm
	rf.log = append(rf.log, Entry{Term: term, Cmd: command})
	index = len(rf.log)
	//log.Printf("%v %v %v", index, term, isLeader)
	// Your code here (3B).
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) print() {
	for {
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			log.Printf("term %v, %v leader %v", rf.currentTerm, rf.me, rf.log)
		} else {
			log.Printf("term %v, %v follower %v", rf.currentTerm, rf.me, rf.log)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	for {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied + 1,
				Command:      rf.log[rf.lastApplied].Cmd,
			}
			rf.applyCh <- msg
			//log.Printf("%v %v %v", rf.commitIndex, rf.lastApplied, msg)

			rf.lastApplied = rf.lastApplied + 1
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) ticker() {
	//rf.killed() == false
	for {
		select {
		case <-rf.followerChannel:
			rf.mu.Lock()
			rf.state = FOLLOWER
			rf.mu.Unlock()
		case <-rf.candidateChannel:
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm = rf.currentTerm + 1
			rf.mu.Unlock()
			go rf.sendRequestVote()
		case <-rf.leaderChannel:
			rf.mu.Lock()
			rf.state = LEADER
			rf.mu.Unlock()
			//log.Printf("i got leader %v", rf.me)
			time.Sleep(time.Millisecond * 50)
			go rf.sendAppendEntries()
		case <-time.After(time.Duration(150+(rand.Int63()%300)) * time.Millisecond):
			rf.state = CANDIDATE
			rf.candidateChannel <- CANDIDATE
		}
		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	//persistent state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Entry{}

	//volatile state on server
	rf.commitIndex = 0
	rf.lastApplied = 0

	//volatile state on leaders
	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.state = FOLLOWER
	// Your initialization code here (3A, 3B, 3C).
	rf.followerChannel = make(chan int, len(rf.peers))
	rf.candidateChannel = make(chan int, len(rf.peers))
	rf.leaderChannel = make(chan int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.print()
	go rf.apply()
	return rf
}
