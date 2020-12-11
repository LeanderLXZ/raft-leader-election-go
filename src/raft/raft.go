package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.GetState() (term, isLeader)
//   Check a node's current term, and whether it thinks it is leader
// rf.sendRequestVote(...)
//   Cause a node to start an election by sending a vote RPC to other nodes
// rf.RequestVote(...)
//   this function will be called via RPC on the other Raft nodes during the election

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// ApplyMsg is used to try to add a new entry to the log
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

// Raft is a Go object implementing a single Raft peer.
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

	// candidate, follower, or leader
	state         string
	leaderID      int
	activeTime    time.Time
	heartbeatTime time.Time

	// Persistent state on all servers
	currentTerm int
	votedFor    int
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == "leader"
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// RequestVoteArgs RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate's term
	Term int
	// candidate requesting vote
	CandidateID int
}

//
// RequestVoteReply structure returned by RPC.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	//true means candidate received vote
	VoteGranted bool
}

// VoteResponses RPC arguments structure.
type VoteResponses struct {
	id    int
	reply *RequestVoteReply
}

func checkTerm(rf *Raft, term int) {
	if rf.currentTerm < term {
		// update state
		rf.state = "follower"
		// update leaderID
		rf.leaderID = -1
		// update vote
		rf.votedFor = -1
		// update term
		rf.currentTerm = term
	}
}

// RequestVote is the RPC handler which will be executed by a node
// when it gets a request for a vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set to false until the leader is selected
	reply.VoteGranted = false
	// Reply the term of current node
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		return
	}

	// 2. Covert to follower if term > currentTerm
	checkTerm(rf, args.Term)

	// Voting
	// If votedFor is null or candidateId, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// Vote for candidate
		rf.votedFor = args.CandidateID
		// update granted status
		reply.VoteGranted = true
		// update activate time
		rf.activeTime = time.Now()
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Election
func (rf *Raft) elect() {
	// if rf is not killed
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// If time out, convert to candidate
			timeNow := time.Now()
			// Make sure the election timeouts in different peers don’t always
			// fire at the same time, or else all peers will vote only for
			// themselves and no one will become the leader.
			timeout := time.Duration(200+rand.Int31n(300)) * time.Millisecond
			if rf.state == "follower" && timeNow.Sub(rf.activeTime) >= timeout {
				rf.state = "candidate"
			}

			// request votes
			if rf.state == "candidate" && timeNow.Sub(rf.activeTime) >= timeout {

				// Vote for self and update states
				rf.votedFor = rf.me
				rf.currentTerm++
				rf.activeTime = timeNow

				// Send vote request to peers
				requestArgs := RequestVoteArgs{}
				requestArgs.CandidateID = rf.me
				requestArgs.Term = rf.currentTerm

				rf.mu.Unlock()
				nPeers := len(rf.peers)
				voteCh := make(chan *VoteResponses, nPeers)
				// Send requests
				for i := 0; i < nPeers; i++ {
					go func(peerID int, args *RequestVoteArgs) {
						// the node is self
						if peerID == rf.me {
							return
						}
						requestReply := RequestVoteReply{}
						success := rf.sendRequestVote(peerID, args, &requestReply)
						if success {
							voteCh <- &VoteResponses{id: peerID, reply: &requestReply}
						} else {
							voteCh <- &VoteResponses{id: peerID, reply: nil}
						}
					}(i, &requestArgs)
				}

				// Check the channel until voting is finished
				nVoteMe := 1
				nFinished := 1
				largestTerm := -1
				for {
					select {
					case vote := <-voteCh:
						nFinished++
						voteReply := vote.reply
						if voteReply != nil {
							// The peer vote for me
							if voteReply.VoteGranted {
								nVoteMe++
							}
							// Update largest term
							if voteReply.Term > largestTerm {
								largestTerm = voteReply.Term
							}
						}
						// Finish the voting when all nodes has voted
						if nFinished == len(rf.peers) || nVoteMe > nPeers/2 {
							goto JUMPPOINT
						}
					}
				}
			JUMPPOINT:
				rf.mu.Lock()
				// Check the state
				if rf.state != "candidate" {
					return
				}
				// Check the term
				if rf.currentTerm < largestTerm {
					checkTerm(rf, largestTerm)
					return
				}
				// Win the voting
				if nVoteMe > nPeers/2 {
					rf.state = "leader"
					rf.leaderID = rf.me
					rf.heartbeatTime = time.Unix(0, 0)
					return
				}
			}
		}()
	}
}

// AppendEntriesArgs RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	LeaderCommit int
}

// AppendEntriesReply RPC arguments structure.
type AppendEntriesReply struct {
	Term int
}

// HeartbeatResponses RPC arguments structure.
type HeartbeatResponses struct {
	id    int
	reply *AppendEntriesReply
}

// AppendEntries will be sent from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply the term of current node
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		return
	}

	// 2. Covert to follower if term > currentTerm
	checkTerm(rf, args.Term)

	// Append new leader
	rf.leaderID = args.LeaderID

	// Update active time
	rf.activeTime = time.Now()
}

// Send heartbeat to other nodes
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Heartbeat
func (rf *Raft) heartbeat() {
	// if rf is not killed
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check if I am the leader
			if rf.state != "leader" {
				return
			}

			// Send heart beat for every 100ms
			timeNow := time.Now()
			if timeNow.Sub(rf.heartbeatTime) < 100*time.Millisecond {
				return
			}
			rf.heartbeatTime = time.Now()

			// Send heartbeat
			nPeers := len(rf.peers)
			for i := 0; i < nPeers; i++ {
				// If the peer is me
				if i == rf.me {
					continue
				}
				heartbeatArgs := AppendEntriesArgs{}
				heartbeatArgs.LeaderID = rf.me
				heartbeatArgs.Term = rf.currentTerm
				// send heartbeat goroutine
				go func(peerID int, args *AppendEntriesArgs) {
					heartbeatReply := AppendEntriesReply{}
					success := rf.sendAppendEntries(peerID, args, &heartbeatReply)
					if success {
						// Check term of the reply
						rf.mu.Lock()
						defer rf.mu.Unlock()
						checkTerm(rf, heartbeatReply.Term)
					}
				}(i, &heartbeatArgs)
			}
		}()
	}
}

// Start the consensus process to add a new entry to the log.
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

	return index, term, isLeader
}

// Kill your Raft code. Called by tester.
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

// Make a new Raft node and perform its initialization.
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
	rf.state = "follower"
	rf.leaderID = -1
	rf.votedFor = -1
	rf.activeTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// go routine for election
	go rf.elect()

	// go routine for heartbeat
	go rf.heartbeat()

	return rf
}
