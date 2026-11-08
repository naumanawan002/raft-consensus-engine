package raft

import (
	"testing"
	"time"
)

func TestRaft_LeaderElection_And_LogReplication(t *testing.T) {
	net := NewInMemoryNetwork()
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	nodes := make([]*Node, 3)
	for i, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := NewNode(id, peers, net)
		net.Register(node)
		nodes[i] = node
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// 1. Wait for Leader Election (within 600ms)
	time.Sleep(600 * time.Millisecond)

	var leader *Node
	leaderCount := 0
	for _, n := range nodes {
		if n.Role() == Leader {
			leader = n
			leaderCount++
		}
	}

	if leaderCount != 1 || leader == nil {
		t.Fatalf("expected exactly 1 leader in cluster, got %d", leaderCount)
	}

	t.Logf("Node %s elected as cluster LEADER for Term %d", leader.ID(), leader.Term())

	// 2. Submit Log Command to Leader
	idx, term, ok := leader.SubmitCommand("SET currency USD")
	if !ok || idx <= 0 {
		t.Fatalf("failed to submit command to leader: idx=%d, term=%d", idx, term)
	}

	// 3. Wait for AppendEntries Quorum Replication
	time.Sleep(150 * time.Millisecond)

	leader.mu.Lock()
	commitIdx := leader.commitIndex
	leader.mu.Unlock()

	if commitIdx < idx {
		t.Errorf("expected commitIndex >= %d, got %d", idx, commitIdx)
	}

	// 4. Test Leader Failover: Kill active leader
	t.Logf("Simulating leader crash: Stopping %s...", leader.ID())
	leader.Stop()

	// Wait for remaining 2 followers to detect leader absence and elect a new leader
	time.Sleep(700 * time.Millisecond)

	newLeaderCount := 0
	var newLeader *Node
	for _, n := range nodes {
		if n.id != leader.id && n.Role() == Leader {
			newLeader = n
			newLeaderCount++
		}
	}

	if newLeaderCount != 1 || newLeader == nil {
		t.Fatalf("expected failover to elect exactly 1 new leader, got %d", newLeaderCount)
	}

	t.Logf("Failover SUCCESS: Node %s elected as NEW LEADER for Term %d", newLeader.ID(), newLeader.Term())
}
