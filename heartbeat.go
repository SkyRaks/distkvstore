package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// handleAppendEntries is the follower side of replication. A heartbeat is
// just this call with no entries, so one handler covers both.
//
// Order matters here: term check, then recognize the leader and reset the
// election countdown, and only then the log consistency check. A leader
// whose entries don't line up is still a legitimate leader -- it just needs
// to back up -- so a follower that skipped the reset on a log mismatch
// would time out and start a pointless election against a healthy cluster.
func (n *node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req appendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.LeaderID == "" {
		http.Error(w, `missing "leader_id"`, http.StatusBadRequest)
		return
	}

	n.mu.Lock()

	if req.Term < n.currentTerm {
		// Stale leader; tell it our term so it knows to step down.
		resp := appendEntriesResponse{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		writeJSON(w, resp)
		return
	}

	prevTerm, prevLeader := n.currentTerm, n.leaderID

	if req.Term > n.currentTerm {
		// A newer term exists. Adopt it and step down -- no matter what we
		// currently think we are -- same rule as handleRequestVote.
		n.setTermAndVote(req.Term, "")
		n.role = follower
	} else if n.role == candidate {
		// Same term, but someone already won the election we're mid-running.
		// Concede: without this, resetCh below would keep restarting our
		// countdown forever while role stays stuck at "candidate", since
		// nothing else would ever flip it back to follower.
		n.role = follower
	}
	n.leaderID = req.LeaderID

	// The consistency check: do we have an entry at prevLogIndex, and does
	// its term match what the leader thinks it is? If so the two logs are
	// identical everywhere up to that point, and the entries can be spliced
	// in safely.
	ok := n.termAt(req.PrevLogIndex) == req.PrevLogTerm
	if ok {
		n.truncateAndAppend(req.PrevLogIndex, req.Entries)
	}

	resp := appendEntriesResponse{Term: n.currentTerm, Success: ok}
	n.mu.Unlock()

	// Only log on an actual change -- heartbeats repeat every heartbeatInterval
	// forever, and logging every steady-state one would drown out everything
	// else.
	if req.Term != prevTerm || req.LeaderID != prevLeader {
		log.Printf("[%s] recognized %s as leader for term %d", n.id, req.LeaderID, req.Term)
	}
	if !ok {
		log.Printf("[%s] rejected entries from %s: no match at index %d term %d",
			n.id, req.LeaderID, req.PrevLogIndex, req.PrevLogTerm)
	}

	// Legitimate leader contact resets our countdown, same mechanism and
	// channel used for vote grants -- including when the log check failed,
	// per the comment above.
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
			n.mu.RLock()
			req := appendEntriesRequest{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: n.lastLogIndex(),
				PrevLogTerm:  n.lastLogTerm(),
				LeaderCommit: 0, // commitIndex arrives in Task 5
			}
			n.mu.RUnlock()
			results <- n.appendEntriesFrom(ctx, peer, req)
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

// appendEntriesFrom sends one AppendEntries to a single peer. Same failure
// handling as requestVoteFrom: an unreachable peer just misses this round,
// which is normal and not an application error.
func (n *node) appendEntriesFrom(ctx context.Context, peer string, body appendEntriesRequest) appendEntriesResponse {
	raw, err := json.Marshal(body)
	if err != nil {
		log.Printf("[%s] marshal AppendEntries for %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}

	url := "http://" + peer + "/append-entries"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		log.Printf("[%s] heartbeat to %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		log.Printf("[%s] heartbeat to %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}
	defer resp.Body.Close()

	var out appendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Printf("[%s] heartbeat response from %s: %v", n.id, peer, err)
		return appendEntriesResponse{}
	}
	return out
}
