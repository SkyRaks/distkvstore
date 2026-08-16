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

func (s *store) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `missing "key" query parameter`, http.StatusBadRequest)
		return
	}

	v, ok := s.get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// Trailing newline so curl output doesn't run into the shell prompt.
	w.Write([]byte(v + "\n"))
}

func (s *store) handlePut(w http.ResponseWriter, r *http.Request) {
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
	// An absent value stores the empty string; that's a legitimate value.
	s.put(key, q.Get("value"))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("OK\n"))
}
