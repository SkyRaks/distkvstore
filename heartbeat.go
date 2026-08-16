package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)

// handleAppendEntries is the follower side of a heartbeat: the leader calls
// this on every peer, repeatedly, to prove it's still alive and to keep
// followers from starting elections of their own. Real Raft's AppendEntries
// also carries log entries for replication; this project has no replicated
// log, so every call here is an empty heartbeat.
func (n *node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodPut, http.MethodPost:
	default:
		w.Header().Set("Allow", "GET, PUT, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	leaderID := q.Get("leaderId")
	term, err := strconv.Atoi(q.Get("term"))
	if leaderID == "" || err != nil {
		http.Error(w, `missing or invalid "term"/"leaderId" query parameter`, http.StatusBadRequest)
		return
	}

	n.mu.Lock()

	if term < n.currentTerm {
		// Stale leader; tell it our term so it knows to step down.
		resp := appendEntriesResponse{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		writeJSON(w, resp)
		return
	}

	prevTerm, prevLeader := n.currentTerm, n.leaderID

	if term > n.currentTerm {
		// A newer term exists. Adopt it and step down -- no matter what we
		// currently think we are -- same rule as handleRequestVote.
		n.setTermAndVote(term, "")
		n.role = follower
	} else if n.role == candidate {
		// Same term, but someone already won the election we're mid-running.
		// Concede: without this, resetCh below would keep restarting our
		// countdown forever while role stays stuck at "candidate", since
		// nothing else would ever flip it back to follower.
		n.role = follower
	}
	n.leaderID = leaderID

	resp := appendEntriesResponse{Term: n.currentTerm, Success: true}
	n.mu.Unlock()

	// Only log on an actual change -- heartbeats repeat every heartbeatInterval
	// forever, and logging every steady-state one would drown out everything
	// else.
	if term != prevTerm || leaderID != prevLeader {
		log.Printf("[%s] recognized %s as leader for term %d", n.id, leaderID, term)
	}

	// Legitimate leader contact resets our countdown, same mechanism and
	// channel used for vote grants.
	select {
	case n.resetCh <- struct{}{}:
	default:
	}

	writeJSON(w, resp)
}

// runHeartbeats is the leader-side counterpart to runElectionTimer: instead
// of counting down to a candidacy, it repeatedly proves this node is still
// leader, on an interval well under any follower's election timeout. It
// exits the moment this node is no longer leader for this exact term --
// either because a heartbeat response revealed a higher term
// (broadcastHeartbeat handles stepping down) or because state moved on some
// other way -- so at most one of these loops is ever actually sending.
func (n *node) runHeartbeats(ctx context.Context, term int) {
	n.broadcastHeartbeat(ctx, term) // announce the win now, don't wait for the first tick

	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.RLock()
			stillLeader := n.role == leader && n.currentTerm == term
			n.mu.RUnlock()
			if !stillLeader {
				return
			}
			n.broadcastHeartbeat(ctx, term)
		}
	}
}

// broadcastHeartbeat sends one round of heartbeats to every peer in
// parallel and waits for the round to finish, same fan-out shape as
// startElection. There's nothing to count here -- a leader doesn't need a
// majority to keep being leader on any single round -- the only thing that
// matters is whether any response reveals a higher term, meaning a newer
// leader already exists somewhere and this node must step down.
func (n *node) broadcastHeartbeat(ctx context.Context, term int) {
	if len(n.peers) == 0 {
		return
	}

	results := make(chan appendEntriesResponse, len(n.peers))
	for _, peer := range n.peers {
		go func(peer string) {
			results <- n.appendEntriesFrom(ctx, peer, term)
		}(peer)
	}

	for range n.peers {
		select {
		case <-ctx.Done():
			return
		case resp := <-results:
			if resp.Term > term {
				n.mu.Lock()
				if resp.Term > n.currentTerm {
					n.setTermAndVote(resp.Term, "")
					n.role = follower
					log.Printf("[%s] saw higher term %d while leader (was term %d) -> follower", n.id, resp.Term, term)
				}
				n.mu.Unlock()
			}
		}
	}
}

// appendEntriesFrom sends one heartbeat to a single peer. Same failure
// handling as requestVoteFrom: an unreachable peer just misses this round's
// proof of life, which is normal and not an application error.
func (n *node) appendEntriesFrom(ctx context.Context, peer string, term int) appendEntriesResponse {
	url := "http://" + peer + "/append-entries?term=" + strconv.Itoa(term) + "&leaderId=" + n.id

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.Printf("[%s] heartbeat to %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}

	resp, err := n.client.Do(req)
	if err != nil {
		log.Printf("[%s] heartbeat to %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}
	defer resp.Body.Close()

	var body appendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		log.Printf("[%s] heartbeat response from %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}
	return body
}
