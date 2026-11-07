package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Transport defines the communication interface between Raft peers.
type Transport interface {
	SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Node represents a single Raft consensus participant.
type Node struct {
	mu        sync.Mutex
	id        string
	peers     []string
	transport Transport

	// Persistent state on all servers
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	role        Role

	// Volatile state on leaders
	nextIndex  map[string]int
	matchIndex map[string]int

	// Timers and coordination
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	lastHeartbeat     time.Time
	stopCh            chan struct{}

	// State machine storage
	stateMachine map[string]string
}

// NewNode constructs and starts a Raft node instance.
func NewNode(id string, peers []string, transport Transport) *Node {
	n := &Node{
		id:                id,
		peers:             peers,
		transport:         transport,
		currentTerm:       0,
		votedFor:          "",
		log:               []LogEntry{{Index: 0, Term: 0, Command: "NOOP"}}, // 1-based log indexing sentinel
		role:              Follower,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make(map[string]int),
		matchIndex:        make(map[string]int),
		heartbeatInterval: 50 * time.Millisecond,
		stopCh:            make(chan struct{}),
		stateMachine:      make(map[string]string),
	}

	n.resetElectionTimeout()
	n.lastHeartbeat = time.Now()
	go n.runLoop()
	return n
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) resetElectionTimeout() {
	// Randomized timeout between 150ms and 300ms per Raft §5.2
	r := rand.Intn(150)
	n.electionTimeout = time.Duration(150+r) * time.Millisecond
}

func (n *Node) runLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			if role == Leader {
				n.sendHeartbeats()
			} else {
				n.checkElectionTimeout()
			}
		}
	}
}

func (n *Node) checkElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if time.Since(n.lastHeartbeat) > n.electionTimeout {
		log.Printf("[RAFT %s] Election timeout reached (%v). Starting election for Term %d.\n",
			n.id, n.electionTimeout, n.currentTerm+1)
		n.startElection()
	}
}

func (n *Node) startElection() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimeout()
	n.lastHeartbeat = time.Now()

	lastLog := n.log[len(n.log)-1]
	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	votes := 1 // Vote for self
	totalPeers := len(n.peers) + 1
	quorum := (totalPeers / 2) + 1

	for _, peer := range n.peers {
		go func(target string) {
			reply, err := n.transport.SendRequestVote(target, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if n.role == Candidate && reply.VoteGranted && reply.Term == n.currentTerm {
				votes++
				if votes >= quorum {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	log.Printf("[RAFT %s] Acquired quorum votes. Becoming LEADER for Term %d!\n", n.id, n.currentTerm)
	n.role = Leader
	for _, p := range n.peers {
		n.nextIndex[p] = len(n.log)
		n.matchIndex[p] = 0
	}
	n.sendHeartbeats()
}

func (n *Node) becomeFollower(term int) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.lastHeartbeat = time.Now()
	n.resetElectionTimeout()
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return
	}

	for _, peer := range n.peers {
		prevIndex := n.nextIndex[peer] - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		prevTerm := n.log[prevIndex].Term

		var entries []LogEntry
		if len(n.log) > n.nextIndex[peer] {
			entries = n.log[n.nextIndex[peer]:]
		}

		args := AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}

		go func(target string, req AppendEntriesArgs) {
			reply, err := n.transport.SendAppendEntries(target, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if n.role == Leader && reply.Term == n.currentTerm {
				if reply.Success {
					n.nextIndex[target] = req.PrevLogIndex + len(req.Entries) + 1
					n.matchIndex[target] = req.PrevLogIndex + len(req.Entries)
					n.checkAndUpdateCommitIndex()
				} else {
					// Follower rejected; back up nextIndex and retry
					if n.nextIndex[target] > 1 {
						n.nextIndex[target]--
					}
				}
			}
		}(peer, args)
	}
}

func (n *Node) checkAndUpdateCommitIndex() {
	// Leader updates commitIndex if majority matchIndex >= N
	totalPeers := len(n.peers) + 1
	quorum := (totalPeers / 2) + 1

	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		matches := 1 // Leader has entry
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				matches++
			}
		}
		if matches >= quorum {
			n.commitIndex = idx
			n.applyCommittedEntries()
			break
		}
	}
}

func (n *Node) applyCommittedEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		// In a real KV store, apply command to stateMachine
		log.Printf("[RAFT %s] Applied entry %s to State Machine.\n", n.id, entry)
	}
}

// RequestVote RPC Handler (§5.2).
func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if args.Term < n.currentTerm {
		return reply
	}

	// Rule: Vote if candidate has up-to-date log and haven't voted yet
	canVote := (n.votedFor == "" || n.votedFor == args.CandidateID)
	lastLog := n.log[len(n.log)-1]
	logUpToDate := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	if canVote && logUpToDate {
		reply.VoteGranted = true
		n.votedFor = args.CandidateID
		n.lastHeartbeat = time.Now() // Reset timer on granting vote
	}

	return reply
}

// AppendEntries RPC Handler (§5.3).
func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	if args.Term < n.currentTerm {
		return reply
	}

	// Valid leader recognized
	n.lastHeartbeat = time.Now()
	if n.role != Follower {
		n.role = Follower
	}

	// Verify log consistency at prevLogIndex
	if args.PrevLogIndex >= len(n.log) || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return reply
	}

	// Overwrite any conflicting entries
	insertIndex := args.PrevLogIndex + 1
	for _, entry := range args.Entries {
		if insertIndex < len(n.log) {
			if n.log[insertIndex].Term != entry.Term {
				n.log = n.log[:insertIndex]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
		insertIndex++
	}

	// Update commitIndex if leader has committed entries
	if args.LeaderCommit > n.commitIndex {
		minCommit := args.LeaderCommit
		if len(n.log)-1 < minCommit {
			minCommit = len(n.log) - 1
		}
		n.commitIndex = minCommit
		n.applyCommittedEntries()
	}

	reply.Success = true
	return reply
}

// SubmitCommand allows clients to submit mutations to the Leader.
func (n *Node) SubmitCommand(cmd string) (int, int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return -1, -1, false
	}

	index := len(n.log)
	term := n.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	return index, term, true
}
