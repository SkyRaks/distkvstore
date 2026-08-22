// Command distkvstore runs one node of an in-memory key/value store over HTTP.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

// parsePeers turns "a:1, b:2" into []string{"a:1", "b:2"}, tolerating blanks.
func parsePeers(s string) []string {
	peers := make([]string, 0)
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			peers = append(peers, p)
		}
	}
	return peers
}

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	id := flag.String("id", "", "unique node id, e.g. node1 (required)")
	peers := flag.String("peers", "", "comma-separated addresses of the other nodes")
	stateDir := flag.String("state-dir", "data", "directory to persist currentTerm/votedFor in, as <state-dir>/<id>.state.json")
	slow := flag.Duration("slow", 0, "delay before answering /ping, e.g. 3s (simulates a hung node)")
	electionTimeoutMin := flag.Duration("election-timeout-min", 3*time.Second, "minimum election timeout")
	electionTimeoutMax := flag.Duration("election-timeout-max", 6*time.Second, "maximum election timeout")
	heartbeatInterval := flag.Duration("heartbeat-interval", 1*time.Second, "how often the leader sends heartbeats")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required (e.g. -id node1)")
	}

	if err := os.MkdirAll(*stateDir, 0o700); err != nil {
		log.Fatalf("create -state-dir %s: %v", *stateDir, err)
	}

	n := &node{
		id:                 *id,
		addr:               *addr,
		peers:              parsePeers(*peers),
		slow:               *slow,
		client:             &http.Client{Timeout: 1 * time.Second},
		store:              newStore(),
		stateDir:           *stateDir,
		log:                []logEntry{{}}, // index 0 is the sentinel
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
		electionTimeoutMin: *electionTimeoutMin,
		electionTimeoutMax: *electionTimeoutMax,
		heartbeatInterval:  *heartbeatInterval,
		resetCh:            make(chan struct{}, 1),
		replicateCh:        make(chan struct{}, 1),
	}
	// Load before anything else touches currentTerm/votedFor: no HTTP server
	// and no goroutines are running yet, so this needs no lock.
	n.loadPersistedState()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /get", n.handleGet)
	mux.HandleFunc("/put", n.handlePut)
	mux.HandleFunc("GET /ping", n.handlePing)
	mux.HandleFunc("GET /ping-peer", n.handlePingPeer)
	mux.HandleFunc("/request-vote", n.handleRequestVote)
	mux.HandleFunc("/append-entries", n.handleAppendEntries)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go n.runElectionTimer(ctx)

	go func() {
		log.Printf("[%s] listening on %s, peers=%v", n.id, n.addr, n.peers)
		if n.slow > 0 {
			log.Printf("[%s] /ping artificially delayed by %s", n.id, n.slow)
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[%s] shutting down...", n.id)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown error: %v", err)
	}
	log.Printf("[%s] stopped", n.id)
}
