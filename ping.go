package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// handlePing is the inbound side: passive, answers whoever calls.
func (n *node) handlePing(w http.ResponseWriter, r *http.Request) {
	if n.slow > 0 {
		time.Sleep(n.slow)
	}

	n.mu.RLock()
	role, term, votedFor, leaderID := n.role, n.currentTerm, n.votedFor, n.leaderID
	n.mu.RUnlock()

	writeJSON(w, pingResponse{ID: n.id, Addr: n.addr, Role: role.String(), Term: term, VotedFor: votedFor, LeaderID: leaderID})
}

// handlePingPeer is the outbound side: curling this makes *this* node turn
// around and call every peer. Later an election timer fires this instead of
// you; the manual trigger exists so the transport can be watched in isolation.
func (n *node) handlePingPeer(w http.ResponseWriter, r *http.Request) {
	results := make([]peerResult, 0, len(n.peers))
	for _, peer := range n.peers {
		results = append(results, n.pingOne(peer))
	}
	writeJSON(w, results)
}

// pingOne calls a single peer's /ping. It is a separate function partly so the
// response body's defer fires per-peer rather than piling up until the whole
// handler returns.
//
// The named return plus deferred assignment means latency gets recorded on
// every exit path -- same reasoning as pairing a mutex Unlock with defer.
func (n *node) pingOne(peer string) (res peerResult) {
	start := time.Now()
	res.Peer = peer
	defer func() { res.LatencyMS = time.Since(start).Milliseconds() }()

	resp, err := n.client.Get("http://" + peer + "/ping")
	if err != nil {
		// A dead peer is normal data here, not an exception to propagate.
		res.Error = err.Error()
		return res
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		res.Error = "unexpected status: " + resp.Status
		return res
	}

	var body pingResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		res.Error = "bad response body: " + err.Error()
		return res
	}

	res.OK = true
	res.ID = body.ID
	res.Role = body.Role
	res.Term = body.Term
	return res
}
