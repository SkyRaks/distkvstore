package main

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"
)

// store is a concurrency-safe string/string map. Everything that touches the
// map goes through here so the storage layer can be swapped out later.
type store struct {
	mu sync.RWMutex
	m  map[string]string
}

func newStore() *store {
	return &store{m: make(map[string]string)}
}

func (s *store) get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *store) put(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
}

// commitTimeout bounds how long /put waits for a majority before giving up
// and telling the client the write did not commit. Comfortably longer than
// several heartbeat intervals, so ordinary replication is never cut short.
const commitTimeout = 5 * time.Second

// notLeaderResponse is the body returned when a write is rejected because
// this node isn't the leader. LeaderID is "" if no leader is currently known
// (e.g. mid-election), which tells the client to just retry shortly rather
// than pointing it anywhere.
type notLeaderResponse struct {
	Error    string `json:"error"`
	LeaderID string `json:"leader_id,omitempty"`
}

// handleGet is unchanged in behavior from when it lived on *store -- it just
// moved so it sits next to handlePut, which does need node-level state.
func (n *node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `missing "key" query parameter`, http.StatusBadRequest)
		return
	}

	v, ok := n.store.get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// Trailing newline so curl output doesn't run into the shell prompt.
	w.Write([]byte(v + "\n"))
}

// handlePut only accepts writes while this node believes it's the leader.
// This doesn't make writes safe -- a "leader" that just won an election
// still only has this one node's map, with no replication yet (#3 in
// TODO.md) -- it just stops every node from silently diverging by accepting
// writes it has no business accepting.
func (n *node) handlePut(w http.ResponseWriter, r *http.Request) {
	// PUT is the semantically correct verb, but curl sends POST by default
	// with -d and GET with no flags at all, so accept all three: the point of
	// keying off the query string is that a bare `curl <url>` works.
	switch r.Method {
	case http.MethodGet, http.MethodPut, http.MethodPost:
	default:
		w.Header().Set("Allow", "GET, PUT, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	key := q.Get("key")
	if key == "" {
		http.Error(w, `missing "key" query parameter`, http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	if n.role != leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, notLeaderResponse{Error: "not the leader", LeaderID: leaderID})
		return
	}
	// An absent value stores the empty string; that's a legitimate value.
	index := n.appendCommand(key, q.Get("value"))
	term := n.currentTerm
	// Re-check commitability now that the log grew: in a single-node cluster
	// the leader alone is already a majority, so the entry is committed the
	// moment it's appended and no peer reply will ever arrive to notice.
	// With peers this is a no-op -- their matchIndex is still behind.
	n.advanceCommitIndex()
	n.mu.Unlock()

	log.Printf("[%s] appended %s=%s at index %d (term %d)", n.id, key, q.Get("value"), index, term)

	// Kick a replication round immediately instead of waiting for the next
	// heartbeat tick.
	select {
	case n.replicateCh <- struct{}{}:
	default:
	}

	// Bound the wait server-side. Without this the request hangs until the
	// client gives up: a leader that loses contact with its majority keeps
	// believing it's leader (Raft leaders don't step down just for silence),
	// so nothing else would ever end the wait.
	ctx, cancel := context.WithTimeout(r.Context(), commitTimeout)
	defer cancel()

	if !n.waitForCommit(ctx, index) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, notLeaderResponse{Error: "write not committed: no majority acknowledged in time"})
		return
	}

	log.Printf("[%s] committed %s=%s at index %d", n.id, key, q.Get("value"), index)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("OK\n"))
}

// waitForCommit blocks until the entry at index is committed, the request is
// cancelled, or this node stops being leader. Returns whether it committed.
//
// Polling rather than a sync.Cond broadcast: Cond over an RWMutex is awkward,
// and at this scale a 5ms poll is invisible. The tradeoff is deliberate --
// swap in a Cond if the write path ever gets hot.
func (n *node) waitForCommit(ctx context.Context, index int) bool {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.RLock()
		committed := n.commitIndex >= index
		stillLeader := n.role == leader
		n.mu.RUnlock()

		if committed {
			return true
		}
		// Losing leadership mid-write means this entry may be overwritten by
		// the next leader. Fail rather than leave the client hanging.
		if !stillLeader {
			return false
		}

		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}
