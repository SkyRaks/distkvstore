package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// handleRequestVote is the voter side of an election: a candidate calls this
// on every peer to ask "will you vote for me in this term?" The three rules
// below, in order, are what make elections safe: reject anyone behind us,
// unconditionally catch up to (and step down for) anyone ahead of us before
// evaluating anything else, then grant at most one vote per term.
func (n *node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	// A vote grant mutates state (votedFor), same as /put -- accept the same
	// three methods for the same reason: a bare `curl <url>` should work.
	switch r.Method {
	case http.MethodGet, http.MethodPut, http.MethodPost:
	default:
		w.Header().Set("Allow", "GET, PUT, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	candidateID := q.Get("candidateId")
	term, err := strconv.Atoi(q.Get("term"))
	if candidateID == "" || err != nil {
		http.Error(w, `missing or invalid "term"/"candidateId" query parameter`, http.StatusBadRequest)
		return
	}
	// Absent or unparseable means 0, which is correct for a candidate with an
	// empty log -- and a candidate that omits them simply loses any vote from
	// a node that has entries, which is the safe default.
	lastLogIndex, _ := strconv.Atoi(q.Get("lastLogIndex"))
	lastLogTerm, _ := strconv.Atoi(q.Get("lastLogTerm"))

	n.mu.Lock()

	if term < n.currentTerm {
		// Stale candidate; tell it our term so it knows to catch up.
		resp := requestVoteResponse{Term: n.currentTerm, VoteGranted: false}
		n.mu.Unlock()
		writeJSON(w, resp)
		return
	}

	if term > n.currentTerm {
		// A newer term exists somewhere. Adopt it and step down -- no matter
		// what we currently think we are -- before deciding on the vote.
		n.setTermAndVote(term, "")
		n.role = follower
	}

	// Two conditions, both required: at most one vote per term, and the
	// candidate's log must be at least as up to date as ours. The second is
	// what stops a node with a stale log from winning and truncating
	// entries the cluster already committed.
	granted := (n.votedFor == "" || n.votedFor == candidateID) &&
		n.logIsUpToDate(lastLogIndex, lastLogTerm)
	if granted {
		n.setTermAndVote(n.currentTerm, candidateID)
	}
	resp := requestVoteResponse{Term: n.currentTerm, VoteGranted: granted}
	n.mu.Unlock()

	if granted {
		log.Printf("[%s] granted vote to %s for term %d", n.id, candidateID, term)
		// Non-blocking: if a reset is already pending and undrained, this one
		// is redundant -- both mean "restart the countdown."
		select {
		case n.resetCh <- struct{}{}:
		default:
		}
	}

	writeJSON(w, resp)
}

// randomElectionTimeout draws a duration uniformly from [min, max). A fresh
// draw every time this is called is what keeps peers from timing out in
// lockstep.
func randomElectionTimeout(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rand.Int64N(int64(max-min)))
}

// runElectionTimer is the clock that turns silence into a candidacy. Hearing
// from a legitimate candidate (handleRequestVote) or a legitimate leader
// (handleAppendEntries) resets the countdown via resetCh instead of it
// firing.
//
// This keeps running for the entire lifetime of the node, including while
// it's leader -- but a leader ignores its own firing (see the role check
// below) rather than challenging itself. Real Raft leaders simply don't run
// an election timer at all; keeping one goroutine running for every role,
// and just having it no-op when irrelevant, is a smaller change than
// starting and stopping a whole goroutine on every role transition.
//
// ctx is the same shutdown context main() waits on for Ctrl+C, so the timer
// goroutine exits cleanly alongside the HTTP server instead of leaking past
// process shutdown.
func (n *node) runElectionTimer(ctx context.Context) {
	for {
		d := randomElectionTimeout(n.electionTimeoutMin, n.electionTimeoutMax)
		select {
		case <-ctx.Done():
			return
		case <-n.resetCh:
			// Heard from a legitimate candidate or leader -- start the
			// countdown over instead of also declaring candidacy right now.
			continue
		case <-time.After(d):
			n.mu.Lock()
			if n.role == leader {
				// Already in charge; no reason to challenge ourselves. This
				// is what stops the churn seen before heartbeats existed --
				// a leader that just re-elects itself every timeout window.
				n.mu.Unlock()
				continue
			}
			n.role = candidate
			n.setTermAndVote(n.currentTerm+1, n.id) // a candidate always votes for itself
			term := n.currentTerm
			n.mu.Unlock()
			log.Printf("[%s] election timeout after %s -> candidate, term=%d", n.id, d, term)

			n.startElection(ctx, term)
		}
	}
}

// startElection asks every peer for a vote and, if a majority agrees, promotes
// this node to leader for the given term.
//
// Requests go out in parallel, one goroutine per peer: an election races its
// own timeout, so polling peers one at a time would burn most of the election
// window on sequencing alone (each unreachable peer costs a full client
// timeout). This is the fan-out that handlePingPeer deliberately skipped.
func (n *node) startElection(ctx context.Context, term int) {
	if len(n.peers) == 0 {
		return
	}

	results := make(chan requestVoteResponse, len(n.peers))
	for _, peer := range n.peers {
		go func(peer string) {
			results <- n.requestVoteFrom(ctx, peer, term)
		}(peer)
	}

	// votes needs no mutex: the peer goroutines only ever send to the channel,
	// and this is the sole goroutine that reads it. The channel does the
	// synchronizing, so the counter is just an ordinary local variable.
	votes := 1 // vote for self, cast when candidacy was declared
	needed := (len(n.peers)+1)/2 + 1

	for range n.peers {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			if res.Term > term {
				// Someone is already ahead of us; this election is moot.
				n.mu.Lock()
				if res.Term > n.currentTerm {
					n.setTermAndVote(res.Term, "")
					n.role = follower
				}
				n.mu.Unlock()
				log.Printf("[%s] saw higher term %d during election for term %d -> follower", n.id, res.Term, term)
				return
			}
			if res.VoteGranted {
				votes++
			}
		}

		if votes >= needed {
			break
		}
	}

	if votes < needed {
		log.Printf("[%s] election for term %d failed: %d/%d votes", n.id, term, votes, needed)
		return
	}

	// Votes were collected without the lock held, so this node's state may have
	// moved on in the meantime -- it could have stepped down for a higher term
	// while responses were in flight. Only claim leadership if the candidacy
	// this election was started for is still the current one.
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != candidate || n.currentTerm != term {
		log.Printf("[%s] won term %d but state moved on (now %s, term %d) -- not claiming leadership",
			n.id, term, n.role, n.currentTerm)
		return
	}
	n.role = leader
	n.leaderID = n.id

	// A new leader knows nothing about how far behind its peers are, so it
	// guesses they match it exactly and lets rejections walk that back.
	n.nextIndex = make(map[string]int, len(n.peers))
	n.matchIndex = make(map[string]int, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1 // optimistic: assume they match us
		n.matchIndex[peer] = 0                   // pessimistic: we know nothing yet
	}

	log.Printf("[%s] won election for term %d with %d/%d votes -> leader", n.id, term, votes, needed)

	go n.runHeartbeats(ctx, term)
}

// requestVoteFrom asks a single peer for its vote. A failed call is reported
// as a plain "not granted" rather than an error: an unreachable peer is
// normal, and simply doesn't count toward the majority.
func (n *node) requestVoteFrom(ctx context.Context, peer string, term int) requestVoteResponse {
	n.mu.RLock()
	lastIndex, lastTerm := n.lastLogIndex(), n.lastLogTerm()
	n.mu.RUnlock()

	url := "http://" + peer + "/request-vote" +
		"?term=" + strconv.Itoa(term) +
		"&candidateId=" + n.id +
		"&lastLogIndex=" + strconv.Itoa(lastIndex) +
		"&lastLogTerm=" + strconv.Itoa(lastTerm)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.Printf("[%s] vote request to %s: %v", n.id, peer, err)
		return requestVoteResponse{}
	}

	resp, err := n.client.Do(req)
	if err != nil {
		log.Printf("[%s] vote request to %s: %v", n.id, peer, err)
		return requestVoteResponse{}
	}
	defer resp.Body.Close()

	var body requestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		log.Printf("[%s] vote response from %s: %v", n.id, peer, err)
		return requestVoteResponse{}
	}
	return body
}
