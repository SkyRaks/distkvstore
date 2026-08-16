package main

import (
	"net/http"
	"sync"
	"time"
)

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

	// stateDir is where currentTerm/votedFor are persisted, as
	// <stateDir>/<id>.state.json. See persist.go.
	stateDir string

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
