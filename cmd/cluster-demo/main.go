package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nauman/raft-consensus-engine/pkg/raft"
)

func main() {
	port := flag.String("port", ":8095", "HTTP status dashboard port")
	flag.Parse()

	log.Println("==========================================================")
	log.Println("  Raft Distributed Consensus Engine (from scratch in Go)")
	log.Println("  Based on Ongaro & Ousterhout Stanford Raft Paper")
	log.Println("==========================================================")

	net := raft.NewInMemoryNetwork()
	nodeIDs := []string{"node-alpha", "node-bravo", "node-charlie"}

	nodes := make([]*raft.Node, 3)
	for i, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := raft.NewNode(id, peers, net)
		net.Register(node)
		nodes[i] = node
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// HTTP Status Dashboard
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		status := make([]map[string]any, 3)
		for i, n := range nodes {
			status[i] = map[string]any{
				"id":   n.ID(),
				"role": n.Role().String(),
				"term": n.Term(),
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cluster": "3-node-raft",
			"nodes":   status,
		})
	})

	http.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		cmd := r.URL.Query().Get("cmd")
		if cmd == "" {
			cmd = "SET test_key 123"
		}

		// Find current leader
		var leader *raft.Node
		for _, n := range nodes {
			if n.Role() == raft.Leader {
				leader = n
				break
			}
		}

		if leader == nil {
			http.Error(w, "Election in progress: no leader currently elected", http.StatusServiceUnavailable)
			return
		}

		idx, term, ok := leader.SubmitCommand(cmd)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "submitted",
			"leader_id": leader.ID(),
			"term":      term,
			"log_index": idx,
			"success":   ok,
		})
	})

	srv := &http.Server{Addr: *port}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[INFO] Raft Cluster Dashboard listening on %s\n", *port)
		log.Println("[INFO]   GET /status        - View node roles and terms")
		log.Println("[INFO]   GET /command?cmd=X - Submit command to elected leader")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[INFO] Shutting down Raft cluster...")
	_ = srv.Close()
}
