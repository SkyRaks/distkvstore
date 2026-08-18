package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
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

		// Adopt the leader's commit point, clamped to what we actually hold:
		// the leader may have committed entries that haven't reached us yet.
		if req.LeaderCommit > n.commitIndex {
			n.commitIndex = min(req.LeaderCommit, n.lastLogIndex())
			n.applyCommitted()
		}
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

// buildAppendEntries assembles the RPC for one peer from that peer's
// nextIndex: everything from nextIndex onward, with the entry just before it
// as the consistency check.
//
// Caller holds n.mu (read lock suffices).
func (n *node) buildAppendEntries(peer string, term int) appendEntriesRequest {
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1

	// Copy rather than slice-alias the log: the caller releases n.mu before
	// the network call, and a later append could reallocate or overwrite the
	// backing array out from under an in-flight request.
	var entries []logEntry
	if next <= n.lastLogIndex() {
		entries = make([]logEntry, n.lastLogIndex()-next+1)
		copy(entries, n.log[next:])
	}

	return appendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.termAt(prevIndex),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

// handleAppendResult folds one peer's reply back into leader state.
//
// The two failure modes look identical on the wire and must not be confused:
// a higher term means this node is no longer leader and must step down, while
// an equal term with Success false means only that the logs don't line up
// yet -- back nextIndex up one and try again on the next round. Treating the
// second as the first would make a leader resign over a routine repair.
//
// Caller must NOT hold n.mu.
func (n *node) handleAppendResult(peer string, req appendEntriesRequest, resp appendEntriesResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.setTermAndVote(resp.Term, "")
		n.role = follower
		n.leaderID = ""
		log.Printf("[%s] saw higher term %d in an AppendEntries reply from %s -> follower", n.id, resp.Term, peer)
		return
	}

	// A reply from an older round of our own leadership tells us nothing.
	if n.role != leader || req.Term != n.currentTerm {
		return
	}

	if resp.Success {
		match := req.PrevLogIndex + len(req.Entries)
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndex()
		return
	}

	// Log mismatch: back up one and retry on the next round. Real Raft can
	// skip a whole conflicting term at once; one-at-a-time is slower but
	// obviously correct, and this cluster's logs are tiny.
	if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	log.Printf("[%s] %s rejected entries; backing nextIndex to %d", n.id, peer, n.nextIndex[peer])
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
		case <-n.replicateCh:
			// A client write just landed; replicate it now rather than
			// making the client wait out the rest of the tick.
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

// broadcastHeartbeat sends one round of AppendEntries to every peer in
// parallel and waits for the round to finish, same fan-out shape as
// startElection. Each peer's request is built from its own nextIndex, so
// what one peer receives has nothing to do with what another receives.
//
// There is no counting here: each reply is folded back into leader state by
// handleAppendResult in its own goroutine, which is also where a higher term
// triggers stepping down.
func (n *node) broadcastHeartbeat(ctx context.Context, term int) {
	if len(n.peers) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			n.mu.RLock()
			req := n.buildAppendEntries(peer, term)
			n.mu.RUnlock()

			resp := n.appendEntriesFrom(ctx, peer, req)
			if resp.Term == 0 && !resp.Success {
				return // unreachable peer; nothing to fold in
			}
			n.handleAppendResult(peer, req, resp)
		}(peer)
	}
	wg.Wait()
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
