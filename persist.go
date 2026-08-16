package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

// persistedState is the subset of a node's Raft state that must survive a
// crash: Figure 2 of the paper marks currentTerm and votedFor as persistent,
// written to stable storage before responding to any RPC. Everything else on
// node (role, leaderID, ...) is safe to lose on restart -- a node that comes
// back up as a follower with no known leader will simply relearn both from
// the next heartbeat or election.
type persistedState struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

func (n *node) statePath() string {
	return filepath.Join(n.stateDir, n.id+".state.json")
}

// setTermAndVote updates currentTerm/votedFor and persists the change before
// returning. Every place that mutates either field goes through this instead
// of assigning directly, so "changed in memory" and "flushed to disk" can
// never drift apart -- an RPC response is only sent after this returns.
//
// Callers already hold n.mu (write-locked); this does no locking of its own.
func (n *node) setTermAndVote(term int, votedFor string) {
	n.currentTerm = term
	n.votedFor = votedFor
	n.savePersistedState()
}

// savePersistedState writes currentTerm/votedFor to disk. Callers already
// hold n.mu.
//
// A crash mid-write here is a real risk this doesn't fully solve (a torn
// write could leave a corrupt file) -- the standard fix is write-to-temp-file
// then atomic rename, deliberately left out of this increment to keep it
// focused on making state durable *at all* first.
func (n *node) savePersistedState() {
	data, err := json.Marshal(persistedState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
	if err != nil {
		log.Fatalf("[%s] marshal persisted state: %v", n.id, err)
	}
	if err := os.WriteFile(n.statePath(), data, 0o600); err != nil {
		log.Fatalf("[%s] write persisted state to %s: %v", n.id, n.statePath(), err)
	}
}

// loadPersistedState reads currentTerm/votedFor from disk at startup, before
// the HTTP server or any goroutine starts (so there's no concurrent access to
// guard against yet). No file present means a brand-new node: term 0, no
// vote, exactly the previous zero-value behavior. A file that exists but
// won't parse means something is actually wrong with this node's durable
// state -- refuse to start rather than silently re-voting in a term already
// voted in, which is the exact bug this item exists to close.
func (n *node) loadPersistedState() {
	data, err := os.ReadFile(n.statePath())
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatalf("[%s] read persisted state from %s: %v", n.id, n.statePath(), err)
	}

	var s persistedState
	if err := json.Unmarshal(data, &s); err != nil {
		log.Fatalf("[%s] parse persisted state from %s: %v", n.id, n.statePath(), err)
	}
	n.currentTerm = s.CurrentTerm
	n.votedFor = s.VotedFor
	log.Printf("[%s] loaded persisted state: term=%d voted_for=%q", n.id, n.currentTerm, n.votedFor)
}
