// Command distkvstore runs one node of an in-memory key/value store over HTTP.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

// role is a Raft node's current job. Every node is exactly one of these at
// any moment.
type role int

const (
	follower  role = iota // passive; waiting to hear from a leader
	candidate             // mid-election, asking peers to vote for it
	leader                // in charge; sends heartbeats to keep that status
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return "unknown"
	}
}

// node is one member of the cluster. It is both a server (it answers requests
// from peers) and a client (it makes requests to them).
type node struct {
	id    string
	addr  string
	peers []string

	// slow delays this node's /ping reply. Purely a testing lever: it lets a
	// peer go *silent* rather than refusing, which is the failure mode that
	// actually exercises the client timeout below.
	slow time.Duration

	// One client, reused for every outbound call: it pools TCP connections,
	// so building a fresh one per request would throw that away. The explicit
	// Timeout matters more -- a zero-value http.Client waits forever, and
	// "didn't answer in time" is the only failure signal a distributed system
	// really has.
	client *http.Client

	store *store

	// Raft state. Guarded by its own mutex, separate from store.mu: election
	// state and KV data are unrelated and shouldn't contend with each other.
	// Zero values are correct starting state: every node begins a follower in
	// term 0, no election, no vote cast, no leader known.
	mu          sync.RWMutex
	role        role
	currentTerm int
	votedFor    string // candidateId this node voted for in currentTerm, "" if none yet
	leaderID    string // who this node currently believes is leader, "" if unknown

	// Election timeout is drawn fresh, uniformly, from
	// [electionTimeoutMin, electionTimeoutMax) every time it's armed.
	// Randomized so peers don't all time out in lockstep and split every vote
	// forever -- see runElectionTimer.
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration

	// heartbeatInterval is how often a leader proves it's still alive. Must
	// be comfortably shorter than electionTimeoutMin, or followers would
	// time out and start elections even with a perfectly healthy leader.
	heartbeatInterval time.Duration

	// resetCh tells runElectionTimer to abandon its current countdown and
	// draw a fresh one, without becoming a candidate. Sent to whenever this
	// node grants a vote: hearing from a legitimate candidate means there's
	// no need to also start competing right now. Buffered size 1 with a
	// non-blocking send so a burst of grants can't block the HTTP handler.
	resetCh chan struct{}
}

// pingResponse is what GET /ping returns -- this node's identity plus its
// current Raft state, so that with several nodes running you can watch who
// answered and what they currently believe about the election.
type pingResponse struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	Role     string `json:"role"`
	Term     int    `json:"term"`
	VotedFor string `json:"voted_for,omitempty"`
	LeaderID string `json:"leader_id,omitempty"`
}

// requestVoteResponse is what GET/POST/PUT /request-vote returns: whether
// the vote was granted, and the voter's term (which the candidate must
// adopt if it's higher than its own -- that's how a candidate discovers
// it's already behind).
type requestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// appendEntriesResponse is what GET/POST/PUT /append-entries returns. Real
// Raft's AppendEntries also carries log entries and a Success that reflects
// log consistency; this project has no replicated log, so Success here just
// means "your term checks out, you're a legitimate leader, my countdown is
// reset."
type appendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// peerResult is one row of GET /ping-peer's report: what happened when this
// node called one specific peer.
type peerResult struct {
	Peer      string `json:"peer"`
	OK        bool   `json:"ok"`
	ID        string `json:"id,omitempty"`
	Role      string `json:"role,omitempty"`
	Term      int    `json:"term"`
	Error     string `json:"error,omitempty"`
	LatencyMS int64  `json:"latency_ms"`
}

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
		n.currentTerm = term
		n.role = follower
		n.votedFor = ""
	}

	granted := n.votedFor == "" || n.votedFor == candidateID
	if granted {
		n.votedFor = candidateID
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
		n.currentTerm = term
		n.role = follower
		n.votedFor = ""
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
			n.currentTerm++
			n.role = candidate
			n.votedFor = n.id // a candidate always votes for itself
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
					n.currentTerm = res.Term
					n.role = follower
					n.votedFor = ""
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
	log.Printf("[%s] won election for term %d with %d/%d votes -> leader", n.id, term, votes, needed)

	go n.runHeartbeats(ctx, term)
}

// requestVoteFrom asks a single peer for its vote. A failed call is reported
// as a plain "not granted" rather than an error: an unreachable peer is
// normal, and simply doesn't count toward the majority.
func (n *node) requestVoteFrom(ctx context.Context, peer string, term int) requestVoteResponse {
	url := "http://" + peer + "/request-vote?term=" + strconv.Itoa(term) + "&candidateId=" + n.id

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
					n.currentTerm = resp.Term
					n.role = follower
					n.votedFor = ""
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
	slow := flag.Duration("slow", 0, "delay before answering /ping, e.g. 3s (simulates a hung node)")
	electionTimeoutMin := flag.Duration("election-timeout-min", 3*time.Second, "minimum election timeout")
	electionTimeoutMax := flag.Duration("election-timeout-max", 6*time.Second, "maximum election timeout")
	heartbeatInterval := flag.Duration("heartbeat-interval", 1*time.Second, "how often the leader sends heartbeats")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required (e.g. -id node1)")
	}

	n := &node{
		id:                 *id,
		addr:               *addr,
		peers:              parsePeers(*peers),
		slow:               *slow,
		client:             &http.Client{Timeout: 1 * time.Second},
		store:              newStore(),
		electionTimeoutMin: *electionTimeoutMin,
		electionTimeoutMax: *electionTimeoutMax,
		heartbeatInterval:  *heartbeatInterval,
		resetCh:            make(chan struct{}, 1),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /get", n.store.handleGet)
	mux.HandleFunc("/put", n.store.handlePut)
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
