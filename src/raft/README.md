# Raft Consensus Algorithm

###### LABs

**FAULT TOLERANT KEY-VALUE STORAGE SYSTEM**

* Raft
* Raft + Key/Value Store
* Sharded Key/Value replicated machines + Raft + Key/Value Store

###### LAB 2A



###### Leader Election

* Each server is in one of the state: LEADER, FOLLOWER, AND CANDIDATE.
* If client request sent to the follower, follower will forward it to the leader.
* Each node starts as FOLLOWER and waits for heartbeat (AppendEntries RPC with no logs) otherwise timeout and starts election.
* Each communication among the nodes will contain the current term id.
* If incoming term number is higher than current term number, current node will update its term number to the larger one and reverts to FOLLOWER state.
* If incoming term number is lower, then reject the request straight away.
* Election
  * Increment local term number
  * Transitions to CANDIDATE state.
  * Issue requestVote RPCs to all other nodes in the cluster all in parallel.
  * Wait for one of the three things happen
    * Wins the election by receiving majority votes for the same term and transitions to LEADER state
    * Gets a higher or equal term number from other node (told using AppendEntries heartbeat) and marks that node as leader and transitions to FOLLOWER.
    * Election Timeout happens or received a term number lower than current, node restarts the election. Remains in CANDIDATE state.
  * Each server votes for given candidate at most once in FCFS manner for a given term.
    
* LEADER takes care of sending heartbeat messages to the rest of the followers in the cluster.
* RAFT RPCs are idempotent.
    
* APIs/RPC
    * RequestVote
      * Initiated by candidates during election
      * Retried if not received response.
    * AppendEntries
      * Called by Leader to replicate logs
      * Acts as a heartbeat as well
      * Retried if not received response.
      * Repeated request will not be entertained.
* State at each node
    * current state
    * random timeout for election
    * List of nodes in the cluster
    * term number
    * current vote candidate
    * current leader

###### Log Replication

* With new command, LEADER appends the command in the log locally and issues RPC calls in parallel to other nodes in 
  the cluster.
* When the entry has been updated on the majority of nodes (for rest it will keep on sending the request in parallel 
  until successful), leader will apply the command the to its local state machine and return the result to the client.
* AppendLog has prevLogIndex and prevLogTerm in the request, and if the receiving node is in-consistent with that
  request, then it will return the result as false, asking leader to re-try it.
* After log has been committed on the local logs of the follower, it is run through the state machine of the followers.
* When leader first comes to power, it initializes all nextIndex values to the index just after the last in its log.
* After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC. Eventually nextIndex will reach a point where the leader and follower logs match.

**TEST RUN**

<pre>
Test (2A): initial election ...
... Passed --   3.6  3 5683 1334361    0
Test (2A): election after network failure ...
... Passed --   5.9  3 3518  779201    0
Test (2A): multiple elections ...
... Passed --   6.5  7 9341 2031823    0
PASS
ok  	6.824/raft	16.831s
</pre>
