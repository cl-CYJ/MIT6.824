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
	"bytes"
	// "fmt"
	"log"
	"math/rand"

	// "net"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

type Nodestate string

const (
	Follower  Nodestate = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        Nodestate
	heartBeat    *time.Timer
	electionTime *time.Timer

	// Persistent state on all servers (通用的持久化的状态):
	currentTerm int
	votedFor    int
	log         Log

	// Volatile state on all servers (通用的易失性的状态):
	commitIndex int
	lastApplied int

	// Volatile state on leaders (leader上的易失性的状态)
	nextIndex  []int
	matchIndex []int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// 持久化figure2中指定的需要持久化的状态
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
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 读取持久化的状态并恢复
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
	var currentTerm int
	var votedFor int
	var logs Log
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("Failed to decode persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// 请求投票的RPC结构示例，严格按照论文实现
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// 请求投票的应答RPC结构，按论文实现
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// 处理请求投票的请求
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	myLastLog := rf.log.lastLog()
	myLastLogIndex := myLastLog.Index
	myLastLogTerm := myLastLog.Term
	// fmt.Println("此次投票由", args.CandidateId, "发起，需要", rf.me, "投票")
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// fmt.Println(rf.me, "不能投票给",args.CandidateId, " 她已经投给了", rf.votedFor)
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > myLastLogTerm ||
			(args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.persist()
		rf.resetElectionTime()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
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
// 向id为server的follower发起投票请求
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
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index = rf.log.lastLog().Index + 1
	// fmt.Println("rf ", rf.me, "rf.state", rf.state, "lastlog.Index", rf.log.lastLog().Index, " index ", index)
	term = rf.currentTerm

	log := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.log.append(log)
	rf.persist()
	rf.appendEntries(false)
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
	rand.Seed(time.Now().Unix())
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = time.NewTimer(50 * time.Millisecond)
	rf.electionTime = time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = makeEmptyLog()
	rf.log.append(Entry{-1, 0, 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// return rf
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 异步进行计时
	go rf.ticker()

	// 异步把日志应用到状态机
	go rf.applier()

	return rf
}

// 异步进行计时
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartBeat.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.appendEntries(true)
				// fmt.Println("leader", rf.me, "重置了心跳计时")
			}
			rf.heartBeat.Reset(50 * time.Millisecond)
			rf.mu.Unlock()
		case <-rf.electionTime.C:
			// fmt.Println(rf.me, "的选举时间到期了 term =", rf.currentTerm)
			// fmt.Println(time.Now(), len(rf.peers))
			rf.mu.Lock()
			rf.state = Candidate
			rf.leaderElection()
			rf.mu.Unlock()
		}
	}
}

// 重置raft的选举定时器，使用150到300ms的随机值，防止选举瓜分
func (rf *Raft) resetElectionTime() {
	rf.electionTime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
}

/*
Candidate选举请求函数
1. 自增当前任期
2. 给自己投票
3. 重置选举超时定时器
3. 发送请求投票的RPC给其他服务器,异步对结果进行处理
*/
func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTime()
	rf.persist()
	votesNum := 1
	lastLog := rf.log.lastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	var changeToLeader sync.Once
	for id, _ := range rf.peers {
		if id != rf.me {
			// fmt.Println(id, " ", rf.me)
			go func(id int) {
				// fmt.Println(rf.me, "发起对", id, "的请求投票")
				reply := RequestVoteReply{}
				if rf.sendRequestVote(id, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state == Candidate && rf.currentTerm == args.Term {
						if reply.VoteGranted {
							// fmt.Println(id, "投了一票，现在共", votesNum, "票， 共 ", len(rf.peers))
							votesNum += 1
							if votesNum > len(rf.peers)/2 {
								changeToLeader.Do(func() {
									// fmt.Println("选举出了leader ", rf.me, " term =", rf.currentTerm )
									rf.state = Leader
									for idx, _ := range rf.peers {
										rf.nextIndex[idx] = lastLog.Index + 1
										rf.matchIndex[idx] = -1
									}
									rf.appendEntries(true)
								})
							}
						} else if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
						}
					}
				}

			}(id)
		}
	}
}

// 复制日志模块
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) appendEntries(heartbeat bool) {
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTime()
			continue
		} else {
			rf.appendEntry(peer, heartbeat)
		}
	}
}

func (rf *Raft) appendEntry(peer int, heartbeat bool) {
	lastLog := rf.log.lastLog()
	if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= 0 {
			nextIndex = 1
		}
		if lastLog.Index+1 < nextIndex {
			nextIndex = lastLog.Index
		}
		preLog := rf.log.at(nextIndex - 1)
		// fmt.Println("index " ,preLog.Index)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLog.Index,
			PreLogTerm:   preLog.Term,
			Entries:      make([]Entry, lastLog.Index-nextIndex+1),
			LeaderCommit: rf.commitIndex,
		}
		copy(args.Entries, rf.log.slice(nextIndex))
		go rf.leaderSendEntries(peer, &args)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderSendEntries(peer int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return
		}
		if args.Term == rf.currentTerm {
			if reply.Success {
				match := args.PreLogIndex + len(args.Entries)
				next := match + 1
				rf.nextIndex[peer] = max(rf.nextIndex[peer], next)
				rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
				// fmt.Printf("state :%v, [%v]: %v append success next %v match %v\n", rf.state, rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
			} else if reply.Conflict {
				if reply.XTerm == -1 {
					rf.nextIndex[peer] = reply.XLen
				} else {
					lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
					if lastLogInXTerm > 0 {
						rf.nextIndex[peer] = lastLogInXTerm + 1
					} else {
						rf.nextIndex[peer] = reply.XIndex
					}
				}
			}
			rf.leaderCommmit()
		}
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	// 使用二分查找加速
	l := 0
	r := rf.log.lastLog().Index
	for  l < r {
		mid := (l + r + 1) >> 1
		if rf.log.at(mid).Term <= x {
			l = mid
		} else {
			r = mid - 1
		}
	}
	if rf.log.at(l).Term == x {
		return l
	} else {
		return -1
	}
	// for i := rf.log.lastLog().Index; i > 0; i -- {
	// 	term := rf.log.at(i).Term
	// 	if term == x {
	// 		return i
	// 	} else if term < x {
	// 		break
	// 	}
	// }
	// return -1
}

func (rf *Raft) leaderCommmit() {
	if rf.state != Leader {
		return
	}
	// fmt.Println("执行了一次leaderCommmit")
	for i := rf.commitIndex + 1; i <= rf.log.lastLog().Index; i++ {
		if rf.log.at(i).Term != rf.currentTerm {
			// fmt.Println("i的任期为 ", rf.log.at(i).Term, " 现在的任期为", rf.currentTerm)
			continue
		}
		counter := 1
		for peer := 0; peer < len(rf.peers); peer ++ {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= i {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = i
				// fmt.Printf("[%v] leader尝试提交 index %v\n", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
		// fmt.Printf("leader %v日志长度为 %v , 尝试提交日志 index %v，共有%v台，达到了%v台 当前commit为%v\n", rf.me, rf.log.len(), i, len(rf.peers), counter, rf.commitIndex)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// fmt.Println("复制日志失败", rf.me, " 的term更大", " leader =", args.LeaderId)
		return
	}

	rf.resetElectionTime()
	// if rf.state == Leader {
	// 	rf.state = Follower
	// 	// fmt.Println(rf.me, "变成了follower")
	// }

	if rf.log.lastLog().Index < args.PreLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		return
	}
	if rf.log.at(args.PreLogIndex).Term != args.PreLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PreLogIndex).Term
		// 使用二分查找加速
		l := 0
		r := rf.log.at(args.PreLogIndex).Index
		for  l < r {
			mid := (l + r) >> 1
			if rf.log.at(mid).Term >= xTerm {
				r = mid
			} else {
				l = mid + 1
			}
		}
		// for xIndex := args.PreLogIndex; xIndex > 0; xIndex-- {
		// 	if rf.log.at(xIndex-1).Term != xTerm {
		// 		reply.XIndex = xIndex
		// 		break
		// 	}
		// }
		reply.XIndex = l
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		return
	}
	// } else if rf.log.at(argxs.PreLogIndex).Index == args.PreLogIndex && rf.log.at(args.PreLogIndex).Term == args.PreLogTerm {
	// 	rf.log.truncate(args.PreLogIndex + 1)
	// 	rf.log.append(args.Entries...)
	// 	rf.persist()
	// 	reply.Success = true;
	// } else {
	// 	return
	// }

	// if rf.log.at(args.PreLogIndex).Index == args.PreLogIndex && rf.log.at(args.PreLogIndex).Term != args.PreLogTerm {
	// 	fmt.Println(rf.log.at(args.PreLogIndex).Term, args.PreLogTerm)
	// 	fmt.Println("没有匹配上")
	// 	return
	// }
	// 不应该直接截断，有可能收到的是过时的RPC包，先检查这些entries是否已经包含在日志中
	for idx, entry := range args.Entries {
		// fmt.Println("preLogIndex匹配成功")
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log.truncate(entry.Index)
			rf.persist()
		}
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			rf.persist()
			break
		}
	}
	reply.Success = true
	if rf.commitIndex < args.LeaderCommit {
		// rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.commitIndex = args.LeaderCommit
		rf.apply()
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.log.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			// fmt.Println(rf.me, "的lastLog.Index", rf.log.lastLog().Index)
			// fmt.Println(rf.me, " ", rf.state, " ", applyMsg.CommandValid, " ", applyMsg.Command," ", applyMsg.CommandIndex)
			rf.applyCh <- applyMsg
		} else {
			rf.applyCond.Wait()
			// DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}
