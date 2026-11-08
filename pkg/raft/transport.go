package raft

import "sync"

// InMemoryNetwork routes RPCs between in-process Raft nodes.
// Used by the cluster demo and integration tests.
type InMemoryNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

// NewInMemoryNetwork creates a network transport for local clusters.
func NewInMemoryNetwork() *InMemoryNetwork {
	return &InMemoryNetwork{nodes: make(map[string]*Node)}
}

// Register adds a node to the local network so peers can reach it.
func (m *InMemoryNetwork) Register(node *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.id] = node
}

// SendRequestVote delivers a RequestVote RPC to the target node.
func (m *InMemoryNetwork) SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error) {
	m.mu.RLock()
	targetNode, exists := m.nodes[target]
	m.mu.RUnlock()

	if !exists {
		return RequestVoteReply{}, nil
	}
	return targetNode.RequestVote(args), nil
}

// SendAppendEntries delivers an AppendEntries RPC to the target node.
func (m *InMemoryNetwork) SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	m.mu.RLock()
	targetNode, exists := m.nodes[target]
	m.mu.RUnlock()

	if !exists {
		return AppendEntriesReply{}, nil
	}
	return targetNode.AppendEntries(args), nil
}
