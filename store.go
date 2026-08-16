package main

import (
	"net/http"
	"sync"
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

	n.mu.RLock()
	role, leaderID := n.role, n.leaderID
	n.mu.RUnlock()

	if role != leader {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, notLeaderResponse{Error: "not the leader", LeaderID: leaderID})
		return
	}

	q := r.URL.Query()
	key := q.Get("key")
	if key == "" {
		http.Error(w, `missing "key" query parameter`, http.StatusBadRequest)
		return
	}
	// An absent value stores the empty string; that's a legitimate value.
	n.store.put(key, q.Get("value"))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("OK\n"))
}
