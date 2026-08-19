package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func requestVote(t *testing.T, n *node, query string) requestVoteResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/request-vote?"+query, nil)
	w := httptest.NewRecorder()
	n.handleRequestVote(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", w.Code, w.Body.String())
	}
	var resp requestVoteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return resp
}

func TestLogIsUpToDate(t *testing.T) {
	n := testNode(t, "voter")
	n.log = append(n.log, logEntry{Term: 1}, logEntry{Term: 2})
	// voter: lastIndex 2, lastTerm 2

	tests := []struct {
		name                string
		candIndex, candTerm int
		want                bool
	}{
		{"higher last term wins outright", 1, 3, true},
		{"lower last term loses outright", 9, 1, false},
		{"same term, longer log wins", 3, 2, true},
		{"same term, same length is up to date", 2, 2, true},
		{"same term, shorter log loses", 1, 2, false},
	}
	for _, tc := range tests {
		if got := n.logIsUpToDate(tc.candIndex, tc.candTerm); got != tc.want {
			t.Errorf("%s: logIsUpToDate(%d,%d) = %v, want %v",
				tc.name, tc.candIndex, tc.candTerm, got, tc.want)
		}
	}
}

func TestRequestVoteGrantsToUpToDateCandidate(t *testing.T) {
	n := testNode(t, "voter")
	n.log = append(n.log, logEntry{Term: 1})

	resp := requestVote(t, n, "term=2&candidateId=cand&lastLogIndex=1&lastLogTerm=1")

	if !resp.VoteGranted {
		t.Fatal("VoteGranted = false, want true for a candidate that is caught up")
	}
}

// The property this task exists for: a candidate with a stale log must lose,
// or it could win and overwrite entries the cluster already committed.
func TestRequestVoteRejectsStaleLogCandidate(t *testing.T) {
	n := testNode(t, "voter")
	n.log = append(n.log, logEntry{Term: 1}, logEntry{Term: 2}, logEntry{Term: 2})

	resp := requestVote(t, n, "term=5&candidateId=cand&lastLogIndex=1&lastLogTerm=1")

	if resp.VoteGranted {
		t.Fatal("VoteGranted = true for a candidate whose log is behind -- committed writes could be lost")
	}
	// It still adopted the higher term even while refusing the vote.
	if n.currentTerm != 5 {
		t.Fatalf("currentTerm = %d, want 5 -- a higher term is adopted regardless of the vote", n.currentTerm)
	}
}

func TestRequestVoteStillRefusesASecondVoteInTheSameTerm(t *testing.T) {
	n := testNode(t, "voter")

	if !requestVote(t, n, "term=1&candidateId=first&lastLogIndex=0&lastLogTerm=0").VoteGranted {
		t.Fatal("first vote should have been granted")
	}
	if requestVote(t, n, "term=1&candidateId=second&lastLogIndex=0&lastLogTerm=0").VoteGranted {
		t.Fatal("second candidate got a vote in a term already voted in")
	}
}
