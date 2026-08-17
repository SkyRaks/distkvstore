package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// postAppendEntries drives handleAppendEntries the way a leader would and
// returns the decoded response.
func postAppendEntries(t *testing.T, n *node, body appendEntriesRequest) appendEntriesResponse {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/append-entries", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	n.handleAppendEntries(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", w.Code, w.Body.String())
	}
	var resp appendEntriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response %q: %v", w.Body.String(), err)
	}
	return resp
}

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := testNode(t, "n1")
	n.currentTerm = 5

	resp := postAppendEntries(t, n, appendEntriesRequest{Term: 3, LeaderID: "old"})

	if resp.Success {
		t.Fatal("Success = true, want false for a leader from an older term")
	}
	if resp.Term != 5 {
		t.Fatalf("Term = %d, want 5 so the stale leader learns to step down", resp.Term)
	}
	if n.leaderID == "old" {
		t.Fatal("a stale leader must not be recorded as the leader")
	}
}

func TestAppendEntriesAcceptsFirstEntry(t *testing.T) {
	n := testNode(t, "n1")

	resp := postAppendEntries(t, n, appendEntriesRequest{
		Term: 1, LeaderID: "n2",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []logEntry{{Term: 1, Key: "a", Value: "1"}},
	})

	if !resp.Success {
		t.Fatal("Success = false, want true -- prevLogIndex 0 always matches")
	}
	if got := n.lastLogIndex(); got != 1 {
		t.Fatalf("lastLogIndex = %d, want 1", got)
	}
	if n.leaderID != "n2" {
		t.Fatalf("leaderID = %q, want %q", n.leaderID, "n2")
	}
}

func TestAppendEntriesRejectsPrevLogIndexPastEnd(t *testing.T) {
	n := testNode(t, "n1")
	n.currentTerm = 1

	resp := postAppendEntries(t, n, appendEntriesRequest{
		Term: 1, LeaderID: "n2",
		PrevLogIndex: 5, PrevLogTerm: 1, // we have nothing at index 5
		Entries: []logEntry{{Term: 1, Key: "a", Value: "1"}},
	})

	if resp.Success {
		t.Fatal("Success = true, want false -- there is no entry at prevLogIndex")
	}
	if got := n.lastLogIndex(); got != 0 {
		t.Fatalf("lastLogIndex = %d, want 0 -- a rejected append must not modify the log", got)
	}
}

func TestAppendEntriesRejectsPrevLogTermMismatch(t *testing.T) {
	n := testNode(t, "n1")
	n.currentTerm = 2
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})

	resp := postAppendEntries(t, n, appendEntriesRequest{
		Term: 2, LeaderID: "n2",
		PrevLogIndex: 1, PrevLogTerm: 2, // ours at index 1 is term 1, not 2
		Entries: []logEntry{{Term: 2, Key: "b", Value: "2"}},
	})

	if resp.Success {
		t.Fatal("Success = true, want false -- term at prevLogIndex disagrees")
	}
	if got := n.lastLogIndex(); got != 1 {
		t.Fatalf("lastLogIndex = %d, want 1 -- a rejected append must not modify the log", got)
	}
}

func TestBuildAppendEntriesSendsSuffixFromNextIndex(t *testing.T) {
	n := testNode(t, "leader")
	n.currentTerm = 2
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})
	n.log = append(n.log, logEntry{Term: 2, Key: "b", Value: "2"})
	n.nextIndex = map[string]int{"p1": 2}

	req := n.buildAppendEntries("p1", 2)

	if req.PrevLogIndex != 1 {
		t.Fatalf("PrevLogIndex = %d, want 1 (nextIndex-1)", req.PrevLogIndex)
	}
	if req.PrevLogTerm != 1 {
		t.Fatalf("PrevLogTerm = %d, want 1", req.PrevLogTerm)
	}
	if len(req.Entries) != 1 || req.Entries[0].Key != "b" {
		t.Fatalf("Entries = %+v, want just the entry at index 2", req.Entries)
	}
}

func TestBuildAppendEntriesIsEmptyWhenPeerIsCaughtUp(t *testing.T) {
	n := testNode(t, "leader")
	n.currentTerm = 1
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})
	n.nextIndex = map[string]int{"p1": 2} // peer already has index 1

	req := n.buildAppendEntries("p1", 1)

	if len(req.Entries) != 0 {
		t.Fatalf("Entries = %+v, want empty -- a caught-up peer gets a bare heartbeat", req.Entries)
	}
	if req.PrevLogIndex != 1 {
		t.Fatalf("PrevLogIndex = %d, want 1", req.PrevLogIndex)
	}
}

func TestHandleAppendResultAdvancesOnSuccess(t *testing.T) {
	n := testNode(t, "leader")
	n.currentTerm = 1
	n.role = leader
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})
	n.log = append(n.log, logEntry{Term: 1, Key: "b", Value: "2"})
	n.nextIndex = map[string]int{"p1": 1}
	n.matchIndex = map[string]int{"p1": 0}

	req := appendEntriesRequest{
		Term: 1, LeaderID: "leader", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []logEntry{{Term: 1, Key: "a", Value: "1"}, {Term: 1, Key: "b", Value: "2"}},
	}
	n.handleAppendResult("p1", req, appendEntriesResponse{Term: 1, Success: true})

	if n.matchIndex["p1"] != 2 {
		t.Fatalf("matchIndex = %d, want 2", n.matchIndex["p1"])
	}
	if n.nextIndex["p1"] != 3 {
		t.Fatalf("nextIndex = %d, want 3", n.nextIndex["p1"])
	}
}

func TestHandleAppendResultDecrementsOnLogMismatch(t *testing.T) {
	n := testNode(t, "leader")
	n.currentTerm = 3
	n.role = leader
	n.nextIndex = map[string]int{"p1": 5}
	n.matchIndex = map[string]int{"p1": 0}

	// Same term back, but Success false => log mismatch, not a stale leader.
	req := appendEntriesRequest{Term: 3, PrevLogIndex: 4, PrevLogTerm: 3}
	n.handleAppendResult("p1", req, appendEntriesResponse{Term: 3, Success: false})

	if n.nextIndex["p1"] != 4 {
		t.Fatalf("nextIndex = %d, want 4 -- back up one and retry next round", n.nextIndex["p1"])
	}
	if n.matchIndex["p1"] != 0 {
		t.Fatalf("matchIndex = %d, want 0 -- nothing is known to match yet", n.matchIndex["p1"])
	}
}

func TestHandleAppendResultNeverDecrementsBelowOne(t *testing.T) {
	n := testNode(t, "leader")
	n.currentTerm = 1
	n.role = leader
	n.nextIndex = map[string]int{"p1": 1}
	n.matchIndex = map[string]int{"p1": 0}

	req := appendEntriesRequest{Term: 1, PrevLogIndex: 0, PrevLogTerm: 0}
	n.handleAppendResult("p1", req, appendEntriesResponse{Term: 1, Success: false})

	if n.nextIndex["p1"] < 1 {
		t.Fatalf("nextIndex = %d, must never drop below 1 -- index 0 is the sentinel", n.nextIndex["p1"])
	}
}

func TestHandleAppendResultStepsDownOnHigherTerm(t *testing.T) {
	n := testNode(t, "leader")
	n.currentTerm = 2
	n.role = leader
	n.nextIndex = map[string]int{"p1": 1}
	n.matchIndex = map[string]int{"p1": 0}

	req := appendEntriesRequest{Term: 2}
	n.handleAppendResult("p1", req, appendEntriesResponse{Term: 7, Success: false})

	if n.role != follower {
		t.Fatalf("role = %v, want follower after seeing term 7", n.role)
	}
	if n.currentTerm != 7 {
		t.Fatalf("currentTerm = %d, want 7", n.currentTerm)
	}
}

// The subtle one: a follower whose log needs repair must still treat the
// caller as a legitimate leader and reset its election timer. Otherwise it
// times out and starts a pointless election against a healthy leader.
func TestAppendEntriesResetsTimerEvenWhenLogMismatches(t *testing.T) {
	n := testNode(t, "n1")
	n.currentTerm = 2

	resp := postAppendEntries(t, n, appendEntriesRequest{
		Term: 2, LeaderID: "n2",
		PrevLogIndex: 9, PrevLogTerm: 2, // guaranteed mismatch
	})

	if resp.Success {
		t.Fatal("Success = true, want false")
	}
	if n.leaderID != "n2" {
		t.Fatalf("leaderID = %q, want %q even on log mismatch", n.leaderID, "n2")
	}
	select {
	case <-n.resetCh:
		// good: the countdown was reset
	default:
		t.Fatal("no reset sent -- a follower needing log repair would start a bogus election")
	}
}
