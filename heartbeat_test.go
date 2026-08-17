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
