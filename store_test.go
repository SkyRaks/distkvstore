package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWaitForCommitReturnsTrueOnceCommitted(t *testing.T) {
	n := testNode(t, "n1")
	n.role = leader
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})

	go func() {
		time.Sleep(20 * time.Millisecond)
		n.mu.Lock()
		n.commitIndex = 1
		n.mu.Unlock()
	}()

	if !n.waitForCommit(context.Background(), 1) {
		t.Fatal("waitForCommit = false, want true once commitIndex reaches the entry")
	}
}

func TestWaitForCommitGivesUpWhenCancelled(t *testing.T) {
	n := testNode(t, "n1")
	n.role = leader
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if n.waitForCommit(ctx, 1) {
		t.Fatal("waitForCommit = true, want false when the request is cancelled")
	}
}

func TestWaitForCommitFailsIfLeadershipIsLost(t *testing.T) {
	n := testNode(t, "n1")
	n.role = follower // no longer leader; the entry may be overwritten
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})

	if n.waitForCommit(context.Background(), 1) {
		t.Fatal("waitForCommit = true, want false after losing leadership")
	}
}

// A one-node cluster is legitimate Raft: the lone leader is already a
// majority of one, so a write commits the instant it is appended. Nothing
// else would ever notice, since no peer reply arrives to trigger the check.
func TestHandlePutCommitsImmediatelyInSingleNodeCluster(t *testing.T) {
	n := testNode(t, "n1")
	n.role = leader
	n.currentTerm = 1
	// no peers

	req := httptest.NewRequest(http.MethodGet, "/put?key=a&value=1", nil)
	w := httptest.NewRecorder()
	n.handlePut(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", w.Code, w.Body.String())
	}
	if n.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1", n.commitIndex)
	}
	if v, ok := n.store.get("a"); !ok || v != "1" {
		t.Fatalf("store[a] = %q,%v; want \"1\",true", v, ok)
	}
}

func TestHandlePutReturnsErrorWhenCommitNeverHappens(t *testing.T) {
	n := testNode(t, "n1")
	n.role = leader
	n.currentTerm = 1
	n.peers = []string{"p1", "p2"} // peers that will never answer
	n.replicateCh = make(chan struct{}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/put?key=a&value=1", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	n.handlePut(w, req)

	if w.Code == http.StatusOK {
		t.Fatal("status = 200, but the entry was never committed -- must not claim success")
	}
	if _, ok := n.store.get("a"); ok {
		t.Fatal("an uncommitted entry reached the store")
	}
}

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
