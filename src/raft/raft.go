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

func max(a, b int) int {
	if a > b {
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
	ElectionTimeout    = time.Millisecond * 300
	/*
		Lab 2B test TestRPCBytes2B has a problem with properly testing the
		number of bytes. If we send the heartbeat too frequently, then test might
		see more bytes than expected which is stupidly hard coded in the tests.
	*/
	HeartbeatTimeout = time.Millisecond * 200
)

type LogEntry struct {
	Cmd  interface{}
	Term int
}

type AppendLogMsg struct {
	index int
	cmd   interface{}
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
	appendLogCh     chan bool           // channel to tell asynchronously about commit index update
	leaderCond      *sync.Cond          // Condition for state change to LEADER, with [mu] as internal mutex
	followerCond    *sync.Cond          // Condition for state change to FOLLOWER, with [mu] as internal mutex

	// Snapshot data structures: Non-Volatile
	snapshot		[]byte				// Content of the snapshot
	installSnapshot		bool			// flag for apply log goroutine to look at to send snapshot to the service
	lastSnapshotTerm	int				// Last log term in the snapshot
	lastSnapshotIndex	int				// Last log index (included) in the snapshot, log entries will contain remaining log entries

	// Volatile data structure
	commitIndex int // Index of the highest log entry known to be committed in local logs, initialized 0
	lastApplied int // Index of the highest log entry applied to state machine or notified to service about via applyCh, initialized 0

	// Only setup on the leader and reinitialized everytime a node is elected as leader
	nextIndex []int // Index of the next log entry to be sent to each server
	// initialized to last log index + 1
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server
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
	if newState == LEADER {
		rf.leaderCond.Broadcast()
	} else if newState == FOLLOWER {
		rf.followerCond.Broadcast()
	}
}

func (rf *Raft) AppointLeader() {
	log.Printf("[%d] Appointing leader", rf.me)
	rf.changeStateWithoutLock(LEADER)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + rf.lastSnapshotIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) AppointFollower() {
	rf.changeStateWithoutLock(FOLLOWER)
	rf.nextIndex = nil
	rf.matchIndex = nil
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	index := len(rf.logs) + rf.lastSnapshotIndex
	term := rf.lastSnapshotTerm
	if len(rf.logs) != 0 {
		term = rf.logs[len(rf.logs)-1].Term
	}
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
	isLeader := false
	rf.Lock("GetState::1")
	defer rf.Unlock("GetState::1")
	term = rf.currentTerm
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
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.installSnapshot)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
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
	var currentTerm, votedFor, lastSnapshotIndex, lastSnapshotTerm int
	var installSnapshot bool
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&installSnapshot) != nil {
		log.Fatalln("Failed to decode from persistent storage.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.installSnapshot = installSnapshot
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

/*
RequestVoteArgs
Field names must start with capital letters!
Sent by the candidates.
*/
type RequestVoteArgs struct {
	Term        int // Candidate's term id
	CandidateId int // Candidate's Id
	LastLogTerm int // Term of candidate???s last log entry, used to select a valid leader
	LastLogIdx  int // Index of candidate???s last log entry, used to select a valid leader
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
	NextIdx int  // Next not matching index
}

/*
InstallSnapshotArgs
Fields names must start with capital letters!
*/
type InstallSnapshotArgs struct {
	Term				int			// Leader's term id
	LeaderId			int			// Followers can use this to redirect clients
	LastIncludedIndex 	int			// Index of log entry in the snapshot, used to check consistency of the snapshot
	LastIncludedTerm  	int			// Term of the LastIncludedIndex, used to check consistency of the snapshot
	Snapshot      		[]byte		// snapshot raw data
	LeaderCommit 		int			// Leader's commit index, used to tell where to insert these entries
}

/*
InstallSnapshotReply
Fields names must start with capital letters!
*/
type InstallSnapshotReply struct {
	Term    int  // Leader's term id
}

/*
InstallSnapshot

Raft leaders must sometimes tell lagging Raft peers to update
their state by installing a snapshot. So sometimes leader will
call this RPC to ask its followers to install a particular
snapshot on its raft node.

When a follower receives and handles an InstallSnapshot RPC,
it must hand the included snapshot to the service using Raft.
The InstallSnapshot handler can use the applyCh to send the
snapshot to the service, by putting the snapshot in ApplyMsg.
*/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock("InstallSnapshot::1")
	defer rf.Unlock("InstallSnapshot::1")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.AppointFollower()
		rf.resetElectionTimer()
	}

	// Ignore old snapshots
	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		reply.Term = args.Term
	} else {
		_, lastLogIndex := rf.lastLogTermIndex()
		if lastLogIndex <= args.LastIncludedIndex {
			rf.logs = []LogEntry{}
			if rf.matchIndex != nil {
				rf.matchIndex[rf.me] = args.LastIncludedIndex
			}
		} else {
			rf.logs = rf.logs[args.LastIncludedIndex+1:]
		}
		rf.lastSnapshotIndex = args.LastIncludedIndex
		rf.lastSnapshotTerm = args.LastIncludedTerm
		rf.snapshot = args.Snapshot
		rf.installSnapshot = true
		rf.appendLogCh <- true
	}
	// Persist
	rf.persist()
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
		if lastLogTerm < args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIdx >= lastLogIndex) {
			reply.VoteGranted = true
			reply.Term = args.Term
		} else {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.AppointFollower()
			rf.resetElectionTimer()
		}
	} else if args.Term == rf.currentTerm {
		log.Printf("[%d] votedFor : %d, args.CandidateId: %d, lastLogTerm: %d, "+
			"args.LastLogTerm: %d, lastLogIndex: %d, args.LastLogIdx: %d", rf.me, rf.votedFor, args.CandidateId,
			lastLogTerm, args.LastLogTerm, lastLogIndex, args.LastLogIdx)
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
	_, lastLogIndex := rf.lastLogTermIndex()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term
	if rf.currentTerm < args.Term {
		rf.votedFor = args.LeaderId
	}
	rf.currentTerm = args.Term
	rf.AppointFollower()
	rf.resetElectionTimer()
	if lastLogIndex < args.PrevLogIndex {
		// We need more old entries
		reply.NextIdx = lastLogIndex + 1
		reply.Success = false
	} else if args.PrevLogIndex<rf.lastSnapshotIndex {
		// Requesting for append logs in snapshot range
		// It's not possible in general case. Not sure if it's possible at all.
		// Here we will just ask for logs entries just next to the end of snapshot
		reply.NextIdx = rf.lastSnapshotIndex+1
		reply.Success = false
	} else if args.PrevLogIndex==rf.lastSnapshotIndex {
		if args.PrevLogTerm != rf.lastSnapshotTerm {
			// Again, should not happen, but just in case
			reply.NextIdx = 1
			reply.Success = false
		} else {
			rf.logs = args.Entries
			reply.Success = true
			reply.NextIdx = len(rf.logs) + rf.lastSnapshotIndex + 1
			if args.LeaderCommit > rf.commitIndex {
				_, lastLogIdx := rf.lastLogTermIndex()
				rf.commitIndex = min(lastLogIdx, args.LeaderCommit)
				rf.appendLogCh <- true
			}
		}
	} else if rf.logs[args.PrevLogIndex-rf.lastSnapshotIndex-1].Term != args.PrevLogTerm {
		// We need more old entries
		nextLogIndex := rf.lastSnapshotIndex+1
		termNeeded := rf.logs[args.PrevLogIndex-rf.lastSnapshotIndex-1].Term
		for i := args.PrevLogIndex; i > rf.lastSnapshotIndex && i > rf.commitIndex; i-- {
			if rf.logs[i-rf.lastSnapshotIndex-1].Term != termNeeded {
				break
			}
			nextLogIndex = i
		}
		reply.NextIdx = nextLogIndex
		reply.Success = false
	} else {
		rf.logs = append(rf.logs[:args.PrevLogIndex-rf.lastSnapshotIndex], args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			_, lastLogIdx := rf.lastLogTermIndex()
			rf.commitIndex = min(lastLogIdx, args.LeaderCommit)
			rf.appendLogCh <- true
		}
		reply.Success = true
		reply.NextIdx = len(rf.logs) + rf.lastSnapshotIndex + 1
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok:= rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

/*
Snapshot

The service using Raft (e.g. a k/v server) wants Raft to cooperate
with them to save space. This interface will be used by the service
for that particular use case where from time to time service will
persistently store a "snapshot" of its current state, and call this
RPC and Raft will discard log entries that precede the snapshot and
store the identity or content of snapshot for it to be replicated
instead of the explicit log entries this snapshot comprises.

A service calls Snapshot() to communicate the snapshot of its state
to Raft. The snapshot includes all info up to and including index.
This means the corresponding Raft peer no longer needs the log through
(and including) index. Your Raft implementation should trim its log
as much as possible.
*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.Lock("Snapshot::1")
	defer rf.Unlock("Snapshot::1")

	// No point in consuming an old snapshot
	if index <= rf.lastSnapshotIndex {
		return
	}

	// We can't go over the current log limits as well
	if index > len(rf.logs)+rf.lastSnapshotIndex {
		panic("Gone overboard with the snapshot.")
	}

	_, lastLogIndex := rf.lastLogTermIndex()
	newLastSnapshotTerm := rf.logs[index-rf.lastSnapshotIndex-1].Term
	newLastSnapshotIndex := index
	if lastLogIndex > index {
		rf.logs = rf.logs[index-rf.lastSnapshotIndex:]
	} else {
		// As no remaining log entries
		rf.logs = []LogEntry{}
	}
	rf.snapshot = snapshot
	rf.lastSnapshotTerm = newLastSnapshotTerm
	rf.lastSnapshotIndex = newLastSnapshotIndex
	if rf.nextIndex!=nil {
		rf.nextIndex[rf.me] = len(rf.logs) + rf.lastSnapshotIndex + 1
	}
	// Persist
	rf.persist()
}

/*
CondInstallSnapshot

A service wants to switch to snapshot. Only do so if Raft hasn't had
more recent info since it communicate the snapshot on applyCh.

The service reads from applyCh, and invokes CondInstallSnapshot with
the snapshot to tell Raft that the service is switching to the
passed-in snapshot state, and that Raft should update its log at
the same time.

CondInstallSnapshot should refuse to install a snapshot if it is
an old snapshot (i.e., if Raft has processed entries after the
snapshot's lastIncludedTerm/lastIncludedIndex). This is because
Raft may handle other RPCs and send messages on the applyCh after
it handled the InstallSnapshot RPC, and before CondInstallSnapshot
was invoked by the service. It is not OK for Raft to go back to an
older snapshot, so older snapshots must be refused.

This is just to install the snapshot for the same state in which
Raft received the RPC call for Snapshot.

If the snapshot is recent, then Raft should trim its log, persist
the new state, return true, and the service should switch to the
snapshot before processing the next message on the applyCh.
*/
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.Lock("CondInstallSnapshot::1")
	defer rf.Unlock("CondInstallSnapshot::1")
	if rf.lastSnapshotTerm > lastIncludedTerm || rf.lastSnapshotIndex > lastIncludedIndex {
		return false
	}
	return true
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
	rf.Lock("Start::1")
	defer rf.Unlock("Start::1")
	if rf.state != LEADER {
		return index, term, false
	}
	term = rf.currentTerm
	index = len(rf.logs) + rf.lastSnapshotIndex + 1
	newLog := LogEntry{
		Cmd:  command,
		Term: term,
	}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me] = index
	rf.persist()
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
	globalTimeoutTicker := time.NewTicker(300 * time.Millisecond)
	apiTimeoutTicker := time.NewTicker(100 * time.Millisecond)
	defer globalTimeoutTicker.Stop()
	defer apiTimeoutTicker.Stop()
	for !rf.killed() {
		rf.Lock("CallRequestVote::1")
		if rf.state != CANDIDATE || req.Term != rf.currentTerm {
			rf.Unlock("CallRequestVote::1")
			return
		}
		rf.Unlock("CallRequestVote::1")
		var resp RequestVoteReply
		endCh := make(chan bool, 1)
		apiTimeoutTicker.Stop()
		apiTimeoutTicker.Reset(100 * time.Millisecond)
		go func() {
			ok := rf.sendRequestVote(idx, req, &resp)
			if !ok {
				time.Sleep(10 * time.Millisecond)
			}
			endCh <- ok
		}()

		select {
		case <-globalTimeoutTicker.C:
			return
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
	fullTimeoutTicker := time.NewTicker(300 * time.Millisecond)
	defer fullTimeoutTicker.Stop()
	for !rf.killed() {
		select {
		case vote := <-voteCh:
			totalVotes++
			if vote {
				inFavour++
			}
			if totalVotes == len(rf.peers) || inFavour > len(rf.peers)/2 || totalVotes-inFavour >= len(rf.peers)/2 {
				break
			}
			continue
		case <-fullTimeoutTicker.C:
			return
		}
		break
	}

	if inFavour > len(rf.peers)/2 {
		log.Printf("[%d] Received majority votes: %d out of %d", rf.me, inFavour, totalVotes)
		rf.Lock("TriggerElection::2")
		// If in the middle of election, this node became the follower
		// there is no point in continuing with this election as somebody
		// else with greater term has already been identified and we have already given up
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
			rf.followerCond.Wait()
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

/*
getPrevLogEntries

Gets the prevLogTerm, prevLogIndex, entries which needs to be sent to followers
in AppendEntries RPC call
*/
func (rf *Raft) getPrevLogEntries(idx int) (prevLogTerm int, prevLogIndex int, entries []LogEntry) {
	log.Printf("[%d] idx: %d, nextIdx: %d, rf.lastSnapshotIndex: %d, len(rf.logs): %d", rf.me, idx, rf.nextIndex[idx], rf.lastSnapshotIndex, len(rf.logs))
	prevLogIndex = rf.nextIndex[idx] - 1
	prevLogTerm = rf.lastSnapshotTerm
	if prevLogIndex > rf.lastSnapshotIndex {
		prevLogTerm = rf.logs[prevLogIndex-rf.lastSnapshotIndex-1].Term
	}
	lastLogTerm, lastLogIdx := rf.lastLogTermIndex()
	if prevLogIndex >= lastLogIdx {
		prevLogIndex = lastLogIdx
		prevLogTerm = lastLogTerm
		return prevLogTerm, prevLogIndex, nil
	}
	for i := prevLogIndex + 1; i < len(rf.logs) + rf.lastSnapshotIndex + 1; i++ {
		entries = append(entries, rf.logs[i-rf.lastSnapshotIndex-1])
	}
	return prevLogTerm, prevLogIndex, entries
}

/*
sendAppendEntriesToPeer

Sends AppendEntries RPC call to the peers
Returns true if RPC call completed
Returns false if RPC call failed or should be retried
*/
func (rf *Raft) sendAppendEntriesToPeer(idx int, globalTimeoutTicker *time.Ticker) bool {
	apiTimeoutTicker := time.NewTicker(100 * time.Millisecond)
	defer apiTimeoutTicker.Stop()
	rf.Lock("SendHeartBeat::1")
	if rf.state != LEADER {
		rf.Unlock("SendHeartBeat::1")
		return true
	}
	endCh := make(chan bool, 1)
	var resp AppendEntriesReply
	prevLogTerm, prevLogIndex, entries := rf.getPrevLogEntries(idx)
	req := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.Unlock("SendHeartBeat::1")
	/*
		This code segment can get stuck because of RPC call taking too much time to
		return from the server side. We need to have a timeout here to move the overall heartbeat thread.
	*/
	go func() {
		ok := rf.sendAppendEntries(idx, &req, &resp)
		endCh <- ok
	}()

	select {
	case <-globalTimeoutTicker.C:
		return true
	case <-apiTimeoutTicker.C:
		return false
	case ok := <-endCh:
		if ok {
			break
		} else {
			time.Sleep(10 * time.Millisecond)
			return false
		}
	}

	rf.Lock("SendHeartBeat::1")
	if resp.Term > rf.currentTerm {
		log.Printf("[%d] Received greater term from: %d", rf.me, idx)
		rf.currentTerm = resp.Term
		rf.persist()
		rf.resetElectionTimer()
		rf.AppointFollower()
		rf.Unlock("SendHeartBeat::1")
		return true
	}

	if rf.state != LEADER {
		rf.Unlock("SendHeartBeat::1")
		return true
	}

	// We don't want to do any extra work here if we don't need to do it synchronously
	// as that will delay the processing of heartbeat
	// We will just send the appropriate signal to some background thread which will
	// do it asynchronously for this raft node
	if resp.Success {
		/*
			Next Index for the Leader will always be moving forward
			and will never go backwards. Any response showing NextIdx
			less than the current nextIndex is old response which can be
			neglected.
		*/
		if rf.nextIndex[idx] < resp.NextIdx {
			rf.nextIndex[idx] = resp.NextIdx
			rf.matchIndex[idx] = resp.NextIdx - 1
		}
		rf.appendLogCh <- true
		rf.Unlock("SendHeartBeat::1")
		return true
	} else {
		rf.nextIndex[idx] = max(resp.NextIdx, 1)
	}

	rf.Unlock("SendHeartBeat::1")
	return false
}

/*
sendInstallSnapshotToPeer

Sends InstallSnapshot to the peer
Returns true if RPC call completed
Returns false if RPC call failed or should be retried
*/
func (rf *Raft) sendInstallSnapshotToPeer(idx int, globalTimeoutTicker *time.Ticker) bool {
	apiTimeoutTicker := time.NewTicker(100 * time.Millisecond)
	defer apiTimeoutTicker.Stop()
	rf.Lock("sendInstallSnapshotToPeer::1")
	if rf.state != LEADER {
		rf.Unlock("sendInstallSnapshotToPeer::1")
		return true
	}
	endCh := make(chan bool, 1)
	var resp InstallSnapshotReply
	req := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Snapshot:          rf.snapshot,
		LeaderCommit:      rf.commitIndex,
	}
	rf.Unlock("sendInstallSnapshotToPeer::1")
	/*
		This code segment can get stuck because of RPC call taking too much time to
		return from the server side. We need to have a timeout here to move the overall heartbeat thread.
	*/
	go func() {
		ok := rf.sendInstallSnapshot(idx, &req, &resp)
		endCh <- ok
	}()

	select {
	case <-globalTimeoutTicker.C:
		return true
	case <-apiTimeoutTicker.C:
		return false
	case ok := <-endCh:
		if ok {
			break
		} else {
			time.Sleep(10 * time.Millisecond)
			return false
		}
	}

	rf.Lock("sendInstallSnapshotToPeer::1")
	if resp.Term > rf.currentTerm {
		log.Printf("[%d] Received greater term from: %d", rf.me, idx)
		rf.currentTerm = resp.Term
		rf.persist()
		rf.resetElectionTimer()
		rf.AppointFollower()
		rf.Unlock("sendInstallSnapshotToPeer::1")
		return true
	}

	if rf.state != LEADER {
		rf.Unlock("sendInstallSnapshotToPeer::1")
		return true
	}

	// We don't want to do any extra work here if we don't need to do it synchronously
	// as that will delay the processing of heartbeat
	// We will just send the appropriate signal to some background thread which will
	// do it asynchronously for this raft node
	/*
		Next Index for the Leader will always be moving forward
		and will never go backwards.
	*/
	if rf.nextIndex[idx] < req.LastIncludedIndex + 1 {
		rf.nextIndex[idx] = req.LastIncludedIndex + 1
		rf.matchIndex[idx] = req.LastIncludedIndex
	}
	rf.appendLogCh <- true
	rf.Unlock("sendInstallSnapshotToPeer::1")
	return true
}

/*
SendHeartBeat

Send the Heartbeat message to all the follower/peer at idx
*/
func (rf *Raft) SendHeartBeat(idx int) {
	globalTimeoutTicker := time.NewTicker(200 * time.Millisecond)
	apiTimeoutTicker := time.NewTicker(100 * time.Millisecond)
	defer globalTimeoutTicker.Stop()
	defer apiTimeoutTicker.Stop()
	for rf.killed() == false {
		// Check whether it needs to send the snapshot or log entries
		rf.Lock("SendHeartBeat::1")
		if rf.state != LEADER {
			rf.Unlock("SendHeartBeat::1")
			return
		} else if rf.nextIndex[idx] <= rf.lastSnapshotIndex {
			rf.Unlock("SendHeartBeat::1")
			if rf.sendInstallSnapshotToPeer(idx, globalTimeoutTicker) {
				return
			}
		} else {
			rf.Unlock("SendHeartBeat::1")
			if rf.sendAppendEntriesToPeer(idx, globalTimeoutTicker) {
				return
			}
		}
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
			rf.Unlock("HeartBeatTicker::1")
			go func() {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.SendHeartBeat(i)
				}
			}()
			rf.resetHeartbeatTimer()
		} else {
			rf.leaderCond.Wait()
			rf.Unlock("Unlocked state condition")
		}
	}
}

/*
appendLogs

Listening to any message sent to appendLogCh in Raft object
As this is a background thread and no blocking api calls
there is no timeout needed for this function, other than
if the whole raft node is shutdown in which case rf.killed()
will help with exit. Also, if there are concurrent threads
it may interfere with each other's operations
*/
func (rf *Raft) appendLogs() {
	appendLogTicker := time.NewTicker(200 * time.Millisecond)
	for !rf.killed() {
		select {
		case <-rf.appendLogCh:
			rf.Lock("appendLogs::1")
			var msgs []ApplyMsg
			oldCommitIdx := rf.commitIndex
			// See if we need to send the snapshot to the service
			if rf.installSnapshot {
				rf.installSnapshot = false
				msgs = append(msgs, ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastSnapshotTerm,
					SnapshotIndex: rf.lastSnapshotIndex,
				})
				rf.commitIndex = max(rf.commitIndex, rf.lastSnapshotIndex)
			}
			// Check if commit index is increased
			for commitIdx := rf.commitIndex + 1; commitIdx < len(rf.logs)+rf.lastSnapshotIndex+1; commitIdx++ {
				cnt := 0
				for _, nodeCommitIdx := range rf.matchIndex {
					if nodeCommitIdx >= commitIdx {
						cnt++
						if cnt > len(rf.peers)/2 {
							if rf.currentTerm == rf.logs[commitIdx-rf.lastSnapshotIndex-1].Term {
								rf.commitIndex = commitIdx
							}
							break
						}
					}
				}
				if cnt <= len(rf.peers)/2 {
					break
				}
			}
			if oldCommitIdx != rf.commitIndex {
				log.Printf("[%d] Updated Commit Index: %d", rf.me, rf.commitIndex)
			}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if i <= rf.lastSnapshotIndex {
					continue
				}
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-rf.lastSnapshotIndex-1].Cmd,
					CommandIndex: i,
				})
			}
			rf.Unlock("appendLogs::1")

			for _, msg := range msgs {
				rf.applyCh <- msg
				rf.Lock("appendLogs::2")
				if msg.SnapshotValid {
					rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
				} else {
					rf.lastApplied = max(rf.lastApplied, msg.CommandIndex)
				}
				rf.Unlock("appendLogs::2")
			}
		case <-appendLogTicker.C:
			rf.appendLogCh <- true
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
	rf.appendLogCh = make(chan bool, 100)
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.followerCond = sync.NewCond(&rf.mu)
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastSnapshotIndex = -1
	rf.lastSnapshotTerm = -1
	rf.snapshot = []byte{}
	rf.logs = make([]LogEntry, 1)
	rf.electionTicker = time.NewTicker(rf.randomElectionDuration())
	rf.heartBeatTicker = time.NewTicker(HeartbeatTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// trigger goroutine to append logs
	go rf.appendLogs()

	// trigger goroutine to start election
	go rf.ElectionTicker()

	// trigger goroutine to send heartbeat
	go rf.HeartBeatTicker()

	return rf
}
