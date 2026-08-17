package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlePutOnLeaderAppendsToLog(t *testing.T) {
	n := testNode(t, "n1")
	n.role = leader
	n.currentTerm = 2

	req := httptest.NewRequest(http.MethodGet, "/put?key=a&value=1", nil)
	w := httptest.NewRecorder()
	n.handlePut(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", w.Code, w.Body.String())
	}
	if got := n.lastLogIndex(); got != 1 {
		t.Fatalf("lastLogIndex = %d, want 1 -- the write should be in the log", got)
	}
	if n.log[1].Key != "a" || n.log[1].Value != "1" || n.log[1].Term != 2 {
		t.Fatalf("log[1] = %+v, want {Term:2 Key:a Value:1}", n.log[1])
	}
}

func TestHandlePutOnFollowerRejectsAndDoesNotAppend(t *testing.T) {
	n := testNode(t, "n1")
	n.role = follower
	n.leaderID = "n2"

	req := httptest.NewRequest(http.MethodGet, "/put?key=a&value=1", nil)
	w := httptest.NewRecorder()
	n.handlePut(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
	var body notLeaderResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body %q: %v", w.Body.String(), err)
	}
	if body.LeaderID != "n2" {
		t.Fatalf("leader_id = %q, want %q", body.LeaderID, "n2")
	}
	if got := n.lastLogIndex(); got != 0 {
		t.Fatalf("lastLogIndex = %d, want 0 -- a follower must not append", got)
	}
}
