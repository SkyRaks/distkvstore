package main

import "testing"

// testNode builds a node suitable for unit tests: a real temp dir so
// setTermAndVote can persist, a real store, a sentinel-only log, and a
// buffered resetCh so non-blocking sends never block.
func testNode(t *testing.T, id string) *node {
	t.Helper()
	return &node{
		id:       id,
		stateDir: t.TempDir(),
		store:    newStore(),
		log:      []logEntry{{}},
		resetCh:  make(chan struct{}, 1),
	}
}

func TestEmptyLogHasSentinelOnly(t *testing.T) {
	n := testNode(t, "n1")

	if got := n.lastLogIndex(); got != 0 {
		t.Fatalf("lastLogIndex() = %d, want 0 for an empty log", got)
	}
	if got := n.lastLogTerm(); got != 0 {
		t.Fatalf("lastLogTerm() = %d, want 0 for an empty log", got)
	}
}

func TestLastLogIndexAndTerm(t *testing.T) {
	n := testNode(t, "n1")
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})
	n.log = append(n.log, logEntry{Term: 3, Key: "b", Value: "2"})

	if got := n.lastLogIndex(); got != 2 {
		t.Fatalf("lastLogIndex() = %d, want 2", got)
	}
	if got := n.lastLogTerm(); got != 3 {
		t.Fatalf("lastLogTerm() = %d, want 3", got)
	}
}

func TestTruncateAndAppendOnEmptyLog(t *testing.T) {
	n := testNode(t, "n1")

	n.truncateAndAppend(0, []logEntry{{Term: 1, Key: "a", Value: "1"}})

	if got := n.lastLogIndex(); got != 1 {
		t.Fatalf("lastLogIndex = %d, want 1", got)
	}
	if n.log[1].Key != "a" {
		t.Fatalf("log[1].Key = %q, want %q", n.log[1].Key, "a")
	}
}

func TestTruncateAndAppendDropsConflictingSuffix(t *testing.T) {
	n := testNode(t, "n1")
	// Follower has two entries from an old term that the leader never had.
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "old"})
	n.log = append(n.log, logEntry{Term: 1, Key: "b", Value: "stale"})

	// Leader says: after index 1, the entry is term 2 key=b value=new.
	n.truncateAndAppend(1, []logEntry{{Term: 2, Key: "b", Value: "new"}})

	if got := n.lastLogIndex(); got != 2 {
		t.Fatalf("lastLogIndex = %d, want 2", got)
	}
	if n.log[2].Term != 2 || n.log[2].Value != "new" {
		t.Fatalf("log[2] = %+v, want {Term:2 Key:b Value:new}", n.log[2])
	}
	if n.log[1].Value != "old" {
		t.Fatalf("log[1] = %+v, want the matching prefix left intact", n.log[1])
	}
}

func TestTruncateAndAppendIsIdempotentOnRepeatedEntries(t *testing.T) {
	n := testNode(t, "n1")
	entries := []logEntry{{Term: 1, Key: "a", Value: "1"}, {Term: 1, Key: "b", Value: "2"}}

	n.truncateAndAppend(0, entries)
	n.truncateAndAppend(0, entries) // a retried/duplicated RPC

	if got := n.lastLogIndex(); got != 2 {
		t.Fatalf("lastLogIndex = %d, want 2 -- a duplicate AppendEntries must not grow the log", got)
	}
}

func TestAppendCommandReturnsIndexAndStampsTerm(t *testing.T) {
	n := testNode(t, "n1")
	n.currentTerm = 3

	idx := n.appendCommand("a", "1")
	if idx != 1 {
		t.Fatalf("appendCommand returned %d, want 1 for the first real entry", idx)
	}
	if got := n.log[1].Term; got != 3 {
		t.Fatalf("log[1].Term = %d, want 3 (the leader's current term)", got)
	}
	if n.log[1].Key != "a" || n.log[1].Value != "1" {
		t.Fatalf("log[1] = %+v, want key=a value=1", n.log[1])
	}

	if idx2 := n.appendCommand("b", "2"); idx2 != 2 {
		t.Fatalf("second appendCommand returned %d, want 2", idx2)
	}
}

func TestTermAt(t *testing.T) {
	n := testNode(t, "n1")
	n.log = append(n.log, logEntry{Term: 1})
	n.log = append(n.log, logEntry{Term: 4})

	tests := []struct {
		index int
		want  int
	}{
		{0, 0}, // sentinel
		{1, 1},
		{2, 4},
		{3, -1}, // past the end
		{-1, -1},
	}
	for _, tc := range tests {
		if got := n.termAt(tc.index); got != tc.want {
			t.Errorf("termAt(%d) = %d, want %d", tc.index, got, tc.want)
		}
	}
}
