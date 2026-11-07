package raft

import (
	"fmt"
)

// Role represents a Raft node state.
type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// LogEntry represents an entry in the replicated log.
type LogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command string `json:"command"` // e.g. "SET foo bar"
}

// RequestVoteArgs invoked by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	Term         int    `json:"term"`          // candidate's term
	CandidateID  string `json:"candidate_id"`  // candidate requesting vote
	LastLogIndex int    `json:"last_log_index"`// index of candidate's last log entry
	LastLogTerm  int    `json:"last_log_term"` // term of candidate's last log entry
}

// RequestVoteReply returned by peers (§5.2).
type RequestVoteReply struct {
	Term        int  `json:"term"`         // currentTerm, for candidate to update itself
	VoteGranted bool `json:"vote_granted"` // true means candidate received vote
}

// AppendEntriesArgs invoked by leader for log replication and heartbeats (§5.3).
type AppendEntriesArgs struct {
	Term         int        `json:"term"`           // leader's term
	LeaderID     string     `json:"leader_id"`      // so follower can redirect clients
	PrevLogIndex int        `json:"prev_log_index"` // index of log entry immediately preceding new ones
	PrevLogTerm  int        `json:"prev_log_term"`  // term of prevLogIndex entry
	Entries      []LogEntry `json:"entries"`        // log entries to store (empty for heartbeat)
	LeaderCommit int        `json:"leader_commit"`  // leader's commitIndex
}

// AppendEntriesReply returned by peers (§5.3).
type AppendEntriesReply struct {
	Term    int  `json:"term"`    // currentTerm, for leader to update itself
	Success bool `json:"success"` // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (r Role) String() string {
	return string(r)
}

func (e LogEntry) String() string {
	return fmt.Sprintf("[Idx:%d Term:%d Cmd:%s]", e.Index, e.Term, e.Command)
}
