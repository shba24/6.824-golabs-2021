package raft

import (
	"6.824/labgob"
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

/*
Overall Raft library APIs
-------------------------

This is an outline of the API that raft must expose to
the service (or tester). see comments below for
each of these functions for more details.

rf = Make(...)
  create a new Raft server.
rf.Start(command interface{}) (index, term, isleader)
  start agreement on a new log entry
rf.GetState() (term, isLeader)
  ask a Raft for its current term, and whether it thinks it is leader
ApplyMsg
  each time a new entry is committed to the log, each Raft peer
  should send an ApplyMsg to the service (or tester)
  in the same server so that service can execute that command
  through its state machine.
*/

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

/*
ApplyMsg
As each Raft peer becomes aware that successive log entries are
committed, the peer should send an ApplyMsg to the service (or
tester) on the same server, via the [applyCh] passed to Make(). set
CommandValid to true to indicate that the ApplyMsg contains a newly
committed log entry.

In part 2D you'll want to send other kinds of messages (e.g.,
snapshots) on the applyCh, but set CommandValid to false for these
other uses.
*/
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

type State int

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

const (
	IsDebuggingEnabled = false
	ElectionTimeout    = time.Millisecond * 500
	HeartbeatTimeout   = time.Millisecond * 200
)

type LogEntry struct {
	Cmd  string
	Term int
}

/*
Raft
A Go object implementing a single Raft peer
*/
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	applyCh         chan ApplyMsg       // channel to signal service caller
	dead            int32               // set by Kill()
	state           State               // Current state of the raft node
	electionTicker  *time.Ticker        // Ticker for election timeout
	heartBeatTicker *time.Ticker        // Ticker for heartbeat
	stateCond       *sync.Cond          // Condition for any state change, with [mu] as internal mutex

	// Volatile data structure
	commitIndex int // Index of the highest log entry known to be committed, initialized 0
	lastApplied int // Index of the highest log entry applied to state machine, initialized 0

	// Only setup on the leader and reinitialized everytime a node is elected as leader
	nextIndex []int // Index of the next log entry to be sent to each server
	// initialized to last log index + 1
	matchIndex []int // Index of the highest log entry known to be replicated on each server
	// initialized to 0

	// Non-Volatile data structure
	currentTerm int // latest term server has seen, needs persisting as next term after
	// restart depends on it, initialized with 0
	votedFor int        // candidate which received vote in the current term, initialized with -1
	logs     []LogEntry // log entries each containing command for the state machine and term when entry
	// was received by leader (first index is 0)
}

func (rf *Raft) Lock(lockname string) {
	if IsDebuggingEnabled {
		log.Printf("[%d] Locking %+v", rf.me, lockname)
	}
	rf.mu.Lock()
	if IsDebuggingEnabled {
		log.Printf("[%d] Locked %+v", rf.me, lockname)
	}
}

func (rf *Raft) Unlock(lockname string) {
	if IsDebuggingEnabled {
		log.Printf("[%d] Unlocking %+v", rf.me, lockname)
	}
	rf.mu.Unlock()
	if IsDebuggingEnabled {
		log.Printf("[%d] Unlocked %+v", rf.me, lockname)
	}
}

func (rf *Raft) changeStateWithoutLock(newState State) {
	if rf.state == newState {
		return
	}
	rf.state = newState
	log.Printf("[%d] Updated state to: [%d]", rf.me, rf.state)
	rf.stateCond.Signal()
}

func (rf *Raft) AppointLeader() {
	log.Printf("[%d] Appointing leader", rf.me)
	rf.changeStateWithoutLock(LEADER)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	rf.resetElectionTimer()
}

func (rf *Raft) AppointFollower() {
	rf.changeStateWithoutLock(FOLLOWER)
	rf.nextIndex = nil
	rf.matchIndex = nil
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.logs[len(rf.logs)-1].Term
	index := len(rf.logs) - 1
	return term, index
}

func (rf *Raft) randomElectionDuration() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTicker.Stop()
	rf.electionTicker.Reset(rf.randomElectionDuration())
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartBeatTicker.Stop()
	rf.heartBeatTicker.Reset(HeartbeatTimeout)
}

/*
GetState
Return currentTerm and whether this server
believes it is the leader.
*/
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.Lock("GetState::1")
	defer rf.Unlock("GetState::1")
	term = rf.currentTerm
	isLeader = false
	log.Printf("[%d] State: [%d] Term: [%d]", rf.me, rf.state, rf.currentTerm)
	if rf.state == LEADER {
		isLeader = true
	}
	return term, isLeader
}

/*
persist
Save Raft's persistent state to stable storage,
where it can later be retrieved after a crash and restart.
Lock on the data to be hold by caller
*/
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

/*
readPersist
Restore previously persisted state.
*/
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalln("Failed to decode from persistent storage.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

/*
CondInstallSnapshot
A service wants to switch to snapshot.  Only do so if Raft hasn't had
more recent info since it communicate the snapshot on applyCh.
*/
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

/*
Snapshot
The service says it has created a snapshot that has
all info up to and including index. this means the
service no longer needs the log through (and including)
that index. Raft should now trim its log as much as possible.*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

/*
RequestVoteArgs
Field names must start with capital letters!
Sent by the candidates.
*/
type RequestVoteArgs struct {
	Term        int // Candidate's term id
	CandidateId int // Candidate's Id
	LastLogTerm int // Term of candidate’s last log entry, used to select a valid leader
	LastLogIdx  int // Index of candidate’s last log entry, used to select a valid leader
}

/*
AppendEntriesArgs
Field names must start with capital letters!
Invoked by leader to replicate log entries. Also used as heartbeat
*/
type AppendEntriesArgs struct {
	Term         int        // Leader's term id
	LeaderId     int        // Followers can use this to redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones, used to check consistency of the logs
	PrevLogTerm  int        // Term of the PrevLogIndex, used to check consistency of the logs
	Entries      []LogEntry // Log entries to store, empty for heartbeat and may send more for efficiency
	LeaderCommit int        // Leader's commit index, used to tell where to insert these entries
}

/*
RequestVoteReply
field names must start with capital letters!
*/
type RequestVoteReply struct {
	VoteGranted bool // true if candidate received vote, else false
	Term        int  // returns the receiver's term whether vote granted or not
}

/*
AppendEntriesReply
Field names must start with capital letters!
*/
type AppendEntriesReply struct {
	Term    int  // Leader's term id
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

/*
RequestVote

RPC for requesting the vote by the caller
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock("RequestVote::1")
	defer rf.Unlock("RequestVote::1")
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		reply.Term = args.Term
	} else {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(lastLogTerm < args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIdx >= lastLogIndex)) {
			reply.VoteGranted = true
			reply.Term = args.Term
		} else {
			reply.VoteGranted = false
			reply.Term = args.Term
		}
	}
	if reply.VoteGranted {
		log.Printf("[%d] Granting vote to [%d] for Term[%d]", rf.me, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.AppointFollower()
		rf.resetElectionTimer()
	}
	// Persist in the file system
	rf.persist()
}

/*
AppendEntries

RPC for requesting to append the log entries by the leader
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock("AppendEntries::1")
	defer rf.Unlock("AppendEntries::1")
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if (len(rf.logs) < lastLogIndex+1) ||
		rf.logs[lastLogIndex].Term != lastLogTerm {
		rf.resetElectionTimer()
		reply.Success = false
		reply.Term = args.Term
	} else {
		rf.resetElectionTimer()
		for idx, log := range args.Entries {
			if len(rf.logs) <= lastLogIndex+idx+2 {
				rf.logs[lastLogIndex+idx+1] = log
			} else {
				rf.logs = append(rf.logs, log)
			}
		}
		rf.logs = rf.logs[:lastLogIndex+len(args.Entries)+1]
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.LeaderId
			rf.AppointFollower()
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(len(rf.logs)-1, args.LeaderCommit)
		}
	}
	// Persist in the file system
	rf.persist()
}

/*
sendRequestVote

Code to send a RequestVote RPC to a server.
server is the index of the target server in rf.peers[].
expects RPC arguments in args.
fills in *reply with RPC reply, so caller should
pass &reply.
the types of the args and reply passed to Call() must be
the same as the types of the arguments declared in the
handler function (including whether they are pointers).

The labrpc package simulates a lossy network, in which servers
may be unreachable, and in which requests and replies may be lost.
Call() sends a request and waits for a reply. If a reply arrives
within a timeout interval, Call() returns true; otherwise
Call() returns false. Thus Call() may not return for a while.
A false return can be caused by a dead server, a live server that
can't be reached, a lost request, or a lost reply.

Call() is guaranteed to return (perhaps after a delay) *except* if the
handler function on the server side does not return.  Thus there
is no need to implement your own timeouts around Call().

look at the comments in ../labrpc/labrpc.go for more details.

if you're having trouble getting RPC to work, check that you've
capitalized all field names in structs passed over RPC, and
that the caller passes the address of the reply struct with &, not
the struct itself.
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/*
Start

The service using Raft (e.g. a k/v server) wants to start
agreement on the next command to be appended to Raft's log. if this
server isn't the leader, returns false. Otherwise, start the
agreement and return immediately. There is no guarantee that this
command will ever be committed to the Raft log, since the leader
may fail or lose an election. Even if the Raft instance has been killed,
this function should return gracefully.

The first return value is the index that the command will appear at
if it's ever committed. The second return value is the current
term. The third return value is true if this server believes it is
the leader.
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

/*
Kill

The tester doesn't halt goroutines created by Raft after each test,
but it does call the Kill() method. your code can use killed() to
check whether Kill() has been called. the use of atomic avoids the
need for a lock.

The issue is that long-running goroutines use memory and may chew
up CPU time, perhaps causing later tests to fail and generating
confusing debug output. any goroutine with a long-running loop
should call killed() to check whether it should stop.
*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) CallRequestVote(idx int, req *RequestVoteArgs, voteCh chan bool) {
	for !rf.killed() {
		rf.Lock("CallRequestVote::1")
		if rf.state != CANDIDATE || req.Term != rf.currentTerm {
			rf.Unlock("CallRequestVote::1")
			return
		}
		rf.Unlock("CallRequestVote::1")
		apiTimeoutTicker := time.NewTicker(100 * time.Millisecond)
		var resp RequestVoteReply
		endCh := make(chan bool, 1)
		go func() {
			ok := rf.sendRequestVote(idx, req, &resp)
			if !ok {
				time.Sleep(10 * time.Millisecond)
			}
			endCh <- ok
		}()

		select {
		case ok := <-endCh:
			if !ok {
				continue
			} else {
				rf.Lock("CallRequestVote::2")
				voteCh <- resp.VoteGranted
				if resp.Term > rf.currentTerm {
					log.Printf("[%d] Received greater term, going to submission", rf.me)
					rf.currentTerm = resp.Term
					rf.AppointFollower()
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.Unlock("CallRequestVote::2")
				return
			}
		case <-apiTimeoutTicker.C:
			continue
		}
	}
}

/*
TriggerElection
Caller is expected to hold the lock for the raft shared state
*/
func (rf *Raft) TriggerElection(req *RequestVoteArgs) {
	inFavour := 1
	totalVotes := 1
	voteCh := make(chan bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.CallRequestVote(i, req, voteCh)
	}
	fullTimeoutTicker := time.NewTicker(100 * time.Millisecond)
	for !rf.killed() {
		select {
		case vote:= <-voteCh:
			totalVotes++
			if vote {
				inFavour++
			}
			if totalVotes == len(rf.peers) || inFavour > len(rf.peers)/2 || totalVotes-inFavour >= len(rf.peers)/2 {
				break
			}
			continue
		case <- fullTimeoutTicker.C:
			return
		}
		break
	}

	rf.Lock("TriggerElection::1")
	// If in the middle of election, this node became the follower
	// there is no point in continuing with this election as somebody
	// else with greater term has already been identified and we have already given up
	if rf.state != CANDIDATE || req.Term != rf.currentTerm {
		return
	}
	rf.Unlock("TriggerElection::1")

	if inFavour > len(rf.peers)/2 {
		log.Printf("[%d] Received majority votes: %d out of %d", rf.me, inFavour, totalVotes)
		rf.Lock("TriggerElection::2")
		if rf.state == CANDIDATE && req.Term == rf.currentTerm {
			rf.AppointLeader()
			rf.resetHeartbeatTimer()
		}
		rf.Unlock("TriggerElection::2")
		return
	}
}

/*
ElectionTicker
The ticker go routine starts a new election if this peer hasn't received
heartbeats recently.
*/
func (rf *Raft) ElectionTicker() {
	for rf.killed() == false {
		<-rf.electionTicker.C
		rf.Lock("ElectionTicker::1")
		if rf.state == LEADER {
			rf.stateCond.Wait()
			rf.Unlock("ElectionTicker::1")
		} else {
			rf.resetElectionTimer()
			// election time after timeout
			rf.changeStateWithoutLock(CANDIDATE)
			rf.votedFor = rf.me
			rf.currentTerm += 1
			lastLogTerm, lastLogIdx := rf.lastLogTermIndex()
			// Persist in the file system
			rf.persist()
			req := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
				LastLogIdx:  lastLogIdx,
				LastLogTerm: lastLogTerm,
			}
			rf.Unlock("ElectionTicker::2")
			log.Printf("[%d] Starting campaign", rf.me)
			go rf.TriggerElection(req)
		}
	}
}

func (rf *Raft) SendHeartBeat(idx int, req *AppendEntriesArgs) {
	for rf.killed() == false {
		rf.Lock("SendHeartBeat::1")
		if rf.state != LEADER {
			rf.Unlock("SendHeartBeat::1")
			return
		}
		rf.Unlock("SendHeartBeat::1")

		apiTimeoutTicker := time.NewTicker(100 * time.Millisecond)
		endCh := make(chan bool, 1)
		var resp AppendEntriesReply
		/*
			This code segment can get stuck because of RPC call taking too much time to
			return from the server side. We need to have a timeout here to move the overall heartbeat thread.
		*/
		go func() {
			endCh <- rf.sendAppendEntries(idx, req, &resp)
		}()

		select {
		case <-apiTimeoutTicker.C:
			continue
		case ok := <-endCh:
			if ok {
				break
			} else {
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		rf.Lock("SendHeartBeat::1")
		// TODO: We don't care about AppendEntries. Success now as we just need heatbeat
		if resp.Term > rf.currentTerm {
			log.Printf("[%d] Received greater term, going to submission", rf.me)
			rf.currentTerm = resp.Term
			rf.persist()
			rf.resetElectionTimer()
			rf.AppointFollower()
		}
		rf.Unlock("SendHeartBeat::1")
		// TODO: Add usage of resp for lab B,C,D
	}
}

/*
HeartBeatTicker

Sends the heartbeat at regular interval
*/
func (rf *Raft) HeartBeatTicker() {
	for rf.killed() == false {
		<-rf.heartBeatTicker.C
		rf.Lock("HeartBeatTicker::1")
		if rf.state == LEADER {
			rf.resetHeartbeatTimer()
			lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
			req := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogTerm:  lastLogTerm,
				PrevLogIndex: lastLogIndex,
				// TODO: Keeping entries nil for later purpose when we want to send logs, right now only heartbeat
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			rf.Unlock("HeartBeatTicker::1")
			go func() {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.SendHeartBeat(i, req)
				}
			}()
		} else {
			rf.stateCond.Wait()
			rf.Unlock("Unlocked state condition")
		}
	}
}

/*
Make :the service or tester wants to create a Raft server. the ports
of all the Raft servers (including this one) are in peers[]. this
server's port is peers[me]. all the servers' peers[] arrays
have the same order. persister() is a place for this server to
save its persistent state, and also initially holds the most
recent saved state, if any. applyCh is a channel on which the
tester or service expects Raft to send ApplyMsg messages.
Make() must return quickly, so it should start goroutines
for any long-running work.
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	if !IsDebuggingEnabled {
		log.SetOutput(ioutil.Discard)
	}
	// Set random seed number
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.stateCond = sync.NewCond(&rf.mu)
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.electionTicker = time.NewTicker(rf.randomElectionDuration())
	rf.heartBeatTicker = time.NewTicker(HeartbeatTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ElectionTicker()

	go rf.HeartBeatTicker()

	return rf
}
