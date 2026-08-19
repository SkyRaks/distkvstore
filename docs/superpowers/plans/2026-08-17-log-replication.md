# Log Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a `/put` durable by replicating it to a majority of nodes before answering the client, so a write survives the leader dying immediately afterward.

**Architecture:** The leader appends each write to an in-memory replicated log, ships entries to followers via `AppendEntries` (now POST + JSON), and advances `commitIndex` once a majority has stored an entry. Both leader and followers apply committed entries to the KV map. `RequestVote` gains a log up-to-dateness check so a node with a stale log can never win and erase committed writes.

**Tech Stack:** Go 1.26.5 stdlib only — `net/http`, `encoding/json`, `sync`, plus `testing` and `net/http/httptest` (first tests in this project).

**Spec:** [TODO.md](../../../TODO.md) items 3 and 4.

## Global Constraints

- Go 1.26.5, stdlib only — no new module dependencies.
- All files stay in `package main`, following the item-0 split (one file per Raft RPC, holding both sides of it).
- `gofmt -l .` must print nothing; `go vet ./...` and `go build ./...` must be clean before every commit.
- Raft state (`role`, `currentTerm`, `votedFor`, `leaderID`, and everything added here) is guarded by `n.mu`. The KV map has its own `store.mu`.
- **Lock ordering:** `n.mu` may be held while taking `store.mu`. Never the reverse. Nothing in this plan takes `n.mu` from inside a `store` method.
- Every mutation of `currentTerm`/`votedFor` goes through `n.setTermAndVote(...)` (persist.go), never direct assignment. Callers hold `n.mu`.
- The log is **in-memory only** in this plan. A committed write survives a leader crash, but not simultaneous loss of a majority. Task 7 records this gap in TODO.md rather than papering over it.
- Commit messages match this repo's existing style: plain imperative subject, no `feat:`/`fix:` prefix, with a `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` trailer.

---

## File Structure

| File | Status | Responsibility |
| --- | --- | --- |
| `raft.go` | modify | `logEntry` type; new `node` fields (`log`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`, `replicateCh`); `appendEntriesRequest` type |
| `log.go` | **create** | Pure log helpers: `lastLogIndex`, `lastLogTerm`, `termAt`, `appendCommand`, `truncateAndAppend`, `majorityMatchIndex`, `applyCommitted`, `logIsUpToDate` |
| `log_test.go` | **create** | Unit tests for everything in `log.go` |
| `heartbeat.go` | modify | `AppendEntries` both sides: POST+JSON, consistency check, `nextIndex`/`matchIndex`, commit advance |
| `heartbeat_test.go` | **create** | Handler-level tests via `httptest` |
| `election.go` | modify | Up-to-dateness check in `handleRequestVote`; send `lastLogIndex`/`lastLogTerm` in `requestVoteFrom`; init `nextIndex`/`matchIndex` on winning |
| `election_test.go` | **create** | Vote-granting tests including stale-log rejection |
| `store.go` | modify | `handlePut` appends to the log and waits for commit instead of writing directly |
| `main.go` | modify | Initialize new `node` fields |
| `TODO.md` | modify | Mark items 3 and 4 done; add the log-persistence follow-up |

Rationale for the new `log.go`: the log helpers are pure functions over node state, shared by *both* RPCs (`AppendEntries` uses them for matching, `RequestVote` for up-to-dateness). Putting them in either RPC file would make the other file reach across. They are also the only genuinely unit-testable logic here, so isolating them is what makes the tests cheap.

---

## Design Decisions (settled before writing this plan)

**1-indexed log with a sentinel.** Raft's log is 1-indexed; Go slices are 0-indexed. Rather than doing `index-1` arithmetic at a dozen call sites (where one missed conversion is a silent corruption bug), `n.log[0]` is a permanent sentinel `logEntry{Term: 0}` that is never replicated and never applied. Slice index then *equals* Raft index, and `prevLogIndex: 0` naturally means "before the beginning," matching the paper. `lastLogIndex()` is `len(n.log)-1`, which is `0` for an empty log — exactly what the paper wants.

**Followers reset their election timer even when they reject for log mismatch.** A leader whose entries don't line up is still a legitimate leader; it just needs to back up. If a follower only reset its timer on `Success: true`, a follower needing repair would time out and start a pointless election, disrupting a healthy cluster. So `handleAppendEntries` does: term check → recognize leader + reset timer → *then* the log consistency check. Task 3 has a test pinning this.

**`/put` waits by polling `commitIndex`.** A `sync.Cond` broadcast would avoid the polling, but `sync.Cond` with an `RWMutex` is awkward and adds a concurrency primitive to a project that has stayed on plain mutexes and channels. Polling every 5ms against a deadline is obvious, easy to reason about, and the latency is invisible at this scale. Noted in code as the deliberate simple choice.

**Replication is triggered, not just ticked.** Waiting for the next 1s heartbeat to start replicating would make every `/put` feel broken. Task 6 adds a buffered `replicateCh` so an append kicks a replication round immediately, with the ticker remaining as the steady-state fallback.

---

## Task 1: Log data structure and helpers

**Files:**
- Modify: `raft.go` (add `logEntry`, add `log` field to `node`)
- Create: `log.go`
- Create: `log_test.go`
- Modify: `main.go` (initialize `log`)

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `type logEntry struct { Term int; Key string; Value string }` with JSON tags `term`, `key`, `value`
  - `n.log []logEntry` field on `node`
  - `func (n *node) lastLogIndex() int`
  - `func (n *node) lastLogTerm() int`
  - `func (n *node) termAt(index int) int`

- [ ] **Step 1: Write the failing tests**

Create `log_test.go`:

```go
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

func TestTermAt(t *testing.T) {
	n := testNode(t, "n1")
	n.log = append(n.log, logEntry{Term: 1})
	n.log = append(n.log, logEntry{Term: 4})

	tests := []struct {
		index int
		want  int
	}{
		{0, 0},  // sentinel
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestEmptyLog|TestLastLogIndex|TestTermAt' -v`

Expected: compile failure — `undefined: logEntry`, `n.log undefined`, `n.lastLogIndex undefined`.

- [ ] **Step 3: Add the type and field**

In `raft.go`, add after the `role` `String()` method and before the `node` struct:

```go
// logEntry is one client command in the replicated log. Term is the term of
// the leader that created the entry -- it's what lets a follower detect that
// its log diverged from the leader's at a given index, since two logs that
// agree on (index, term) are guaranteed to be identical up to that point.
type logEntry struct {
	Term  int    `json:"term"`
	Key   string `json:"key"`
	Value string `json:"value"`
}
```

In `raft.go`, inside the `node` struct, immediately after the `leaderID` field:

```go
	// log is the replicated log. It is 1-indexed to match the paper: log[0]
	// is a permanent sentinel that is never replicated and never applied, so
	// a Go slice index equals a Raft log index and "index 0" naturally means
	// "before the first entry." In-memory only for now -- see TODO.md.
	log []logEntry
```

- [ ] **Step 4: Create `log.go` with the helpers**

```go
package main

// This file holds the pure log helpers shared by both Raft RPCs:
// AppendEntries uses them to decide whether a follower's log lines up with
// the leader's, and RequestVote uses them to decide whether a candidate is
// up to date enough to deserve a vote. They are plain functions over node
// state so they can be unit-tested without standing up any HTTP servers.
//
// Every function here assumes the caller already holds n.mu. None of them
// lock anything themselves.

// lastLogIndex is the index of the newest entry, or 0 when the log holds
// only the sentinel. That 0 is meaningful rather than an error case: it is
// exactly the prevLogIndex a leader sends when replicating the very first
// entry.
func (n *node) lastLogIndex() int {
	return len(n.log) - 1
}

// lastLogTerm is the term of the newest entry, or 0 for an empty log.
func (n *node) lastLogTerm() int {
	return n.log[len(n.log)-1].Term
}

// termAt returns the term of the entry at index, or -1 if the index is out
// of range. -1 rather than 0 on purpose: 0 is a legitimate term (the
// sentinel's), so a caller comparing terms must be able to tell "no entry
// there" apart from "an entry from term 0."
func (n *node) termAt(index int) int {
	if index < 0 || index >= len(n.log) {
		return -1
	}
	return n.log[index].Term
}
```

- [ ] **Step 5: Initialize the log in `main.go`**

In the `n := &node{...}` literal, add after `store: newStore(),`:

```go
		log:                []logEntry{{}}, // index 0 is the sentinel
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./... -run 'TestEmptyLog|TestLastLogIndex|TestTermAt' -v`

Expected: PASS (3 tests).

- [ ] **Step 7: Verify the whole project is still clean**

Run: `gofmt -l . && go vet ./... && go build ./... && go test ./...`

Expected: no `gofmt` output, no vet/build errors, tests pass.

- [ ] **Step 8: Commit**

```bash
git add raft.go log.go log_test.go main.go
git commit -m "$(cat <<'EOF'
Add the replicated log structure and helpers

logEntry plus a 1-indexed log on node, with log[0] a permanent sentinel so
a Go slice index equals a Raft log index -- avoiding index-1 arithmetic at
every call site, where one missed conversion would silently corrupt the
log.

Nothing replicates yet; this is just the data structure and its accessors,
plus the project's first unit tests.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test:** none needed — no behavior changed yet. `go run . -id node1` should still start and serve `/ping` exactly as before.

---

## Task 2: Leader appends writes to its log

**Files:**
- Modify: `store.go:66-100` (`handlePut`)
- Create: `log.go` addition (`appendCommand`)
- Modify: `log_test.go` (add tests)

**Interfaces:**
- Consumes: `logEntry`, `n.log`, `n.lastLogIndex()` from Task 1.
- Produces: `func (n *node) appendCommand(key, value string) int` — appends one entry at the current term, returns the index it landed at. Caller holds `n.mu`.

- [ ] **Step 1: Write the failing tests**

Append to `log_test.go`:

```go
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
```

Create `store_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestAppendCommand|TestHandlePut' -v`

Expected: compile failure — `n.appendCommand undefined`. (`TestHandlePutOnFollower...` would pass already; it is here to pin existing behavior against regression.)

- [ ] **Step 3: Add `appendCommand` to `log.go`**

```go
// appendCommand adds one client command to the log at the leader's current
// term and returns the index it landed at. Only a leader calls this: entries
// enter the log exactly one way, from the leader, which is what makes the
// log a single authoritative ordering rather than a merge problem.
//
// Caller holds n.mu.
func (n *node) appendCommand(key, value string) int {
	n.log = append(n.log, logEntry{Term: n.currentTerm, Key: key, Value: value})
	return n.lastLogIndex()
}
```

- [ ] **Step 4: Rewrite `handlePut` to append instead of writing directly**

Replace the body of `handlePut` in `store.go` from the `n.mu.RLock()` line to the end of the function:

```go
	q := r.URL.Query()
	key := q.Get("key")
	if key == "" {
		http.Error(w, `missing "key" query parameter`, http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	if n.role != leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, notLeaderResponse{Error: "not the leader", LeaderID: leaderID})
		return
	}
	// An absent value stores the empty string; that's a legitimate value.
	index := n.appendCommand(key, q.Get("value"))
	n.mu.Unlock()

	// The entry is in the leader's log but has not been replicated to anyone,
	// so it is NOT yet safe. Applying and answering OK here is a deliberate
	// placeholder that keeps /get working while replication is built; Task 4
	// moves the apply behind commitIndex and Task 6 makes this wait for a
	// majority before answering.
	n.store.put(key, q.Get("value"))

	log.Printf("[%s] appended %s=%s at index %d (term %d)", n.id, key, q.Get("value"), index, n.currentTerm)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("OK\n"))
}
```

Note the ordering change: the `key == ""` validation moved *above* the leader check so the handler doesn't take the write lock for a malformed request. Add `"log"` to `store.go`'s imports.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./... -run 'TestAppendCommand|TestHandlePut' -v`

Expected: PASS (3 tests).

- [ ] **Step 6: Verify clean**

Run: `gofmt -l . && go vet ./... && go build ./... && go test ./...`

- [ ] **Step 7: Commit**

```bash
git add store.go log.go log_test.go store_test.go
git commit -m "$(cat <<'EOF'
Append client writes to the leader's log

/put on the leader now appends a logEntry at the current term instead of
only touching the map. It still applies immediately and still answers OK
right away -- a deliberate placeholder so /get keeps working while
replication is built out; the entry is in one node's log and is not yet
safe.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test:**

```bash
# Terminal 1-3 (Git Bash), from /d/DistKVStore:
go run . -addr :8080 -id node1 -peers localhost:8081,localhost:8082 -state-dir /tmp/kv1
go run . -addr :8081 -id node2 -peers localhost:8080,localhost:8082 -state-dir /tmp/kv2
go run . -addr :8082 -id node3 -peers localhost:8080,localhost:8081 -state-dir /tmp/kv3

# Terminal 4: find the leader, write to it
curl -s localhost:8080/ping   # look for "role":"leader"
curl "localhost:8080/put?key=a&value=1"    # -> OK on the leader
```

Expect a new log line on the leader: `appended a=1 at index 1 (term N)`. `/get` still returns the value from the leader, and followers still 409. Nothing replicates yet — that's the next task.

---

## Task 3: AppendEntries over POST+JSON with the log consistency check

**Files:**
- Modify: `raft.go` (add `appendEntriesRequest`)
- Modify: `heartbeat.go:17-78` (`handleAppendEntries`), `heartbeat.go:148-170` (`appendEntriesFrom`)
- Create: `log.go` addition (`truncateAndAppend`)
- Create: `heartbeat_test.go`
- Modify: `log_test.go`

**Interfaces:**
- Consumes: `logEntry`, `n.log`, `n.termAt`, `n.lastLogIndex` from Tasks 1-2.
- Produces:
  - `type appendEntriesRequest struct { Term int; LeaderID string; PrevLogIndex int; PrevLogTerm int; Entries []logEntry; LeaderCommit int }` with JSON tags `term`, `leader_id`, `prev_log_index`, `prev_log_term`, `entries`, `leader_commit`
  - `func (n *node) truncateAndAppend(prevLogIndex int, entries []logEntry)` — caller holds `n.mu`
  - `appendEntriesFrom(ctx context.Context, peer string, req appendEntriesRequest) appendEntriesResponse` — **signature change** from `(ctx, peer, term)`

- [ ] **Step 1: Write the failing tests**

Add to `log_test.go`:

```go
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
```

Create `heartbeat_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestTruncateAndAppend|TestAppendEntries' -v`

Expected: compile failure — `undefined: appendEntriesRequest`, `n.truncateAndAppend undefined`.

- [ ] **Step 3: Add the request type**

In `raft.go`, immediately before `appendEntriesResponse`:

```go
// appendEntriesRequest is the body of POST /append-entries. This replaced
// the old query-string form once entries had to travel with the call: a
// heartbeat is simply this with Entries empty, which is exactly how the
// paper models it.
//
// PrevLogIndex/PrevLogTerm are the consistency check. They describe the
// entry immediately before Entries[0]. A follower accepts only if its own
// log has that exact (index, term) -- which, by induction, proves the two
// logs are identical everywhere up to that point.
type appendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []logEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}
```

- [ ] **Step 4: Add `truncateAndAppend` to `log.go`**

```go
// truncateAndAppend splices entries into the log starting right after
// prevLogIndex. Caller has already verified the log matches at prevLogIndex.
//
// It walks entry by entry rather than blindly truncating, because
// AppendEntries can legitimately be delivered twice (a retry, or a slow
// duplicate). Blind truncation would chop off entries the leader already
// considers committed. Only a genuine term conflict at an index causes a
// truncation; matching entries are skipped over.
//
// Caller holds n.mu.
func (n *node) truncateAndAppend(prevLogIndex int, entries []logEntry) {
	for i, entry := range entries {
		index := prevLogIndex + 1 + i

		if index < len(n.log) {
			if n.log[index].Term == entry.Term {
				continue // already have this exact entry
			}
			// Conflict: this index and everything after it is wrong.
			n.log = n.log[:index]
		}
		n.log = append(n.log, entry)
	}
}
```

- [ ] **Step 5: Rewrite `handleAppendEntries`**

Replace the whole function in `heartbeat.go`:

```go
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
		n.role = follower
	}
	n.leaderID = req.LeaderID

	// The consistency check: do we have an entry at prevLogIndex, and does
	// its term match what the leader thinks it is?
	ok := n.termAt(req.PrevLogIndex) == req.PrevLogTerm
	if ok {
		n.truncateAndAppend(req.PrevLogIndex, req.Entries)
	}

	resp := appendEntriesResponse{Term: n.currentTerm, Success: ok}
	n.mu.Unlock()

	if req.Term != prevTerm || req.LeaderID != prevLeader {
		log.Printf("[%s] recognized %s as leader for term %d", n.id, req.LeaderID, req.Term)
	}
	if !ok {
		log.Printf("[%s] rejected entries from %s: no match at index %d term %d",
			n.id, req.LeaderID, req.PrevLogIndex, req.PrevLogTerm)
	}

	// Legitimate leader contact resets our countdown -- including when the
	// log check failed, per the comment above.
	select {
	case n.resetCh <- struct{}{}:
	default:
	}

	writeJSON(w, resp)
}
```

- [ ] **Step 6: Rewrite `appendEntriesFrom` to POST JSON**

Replace the function in `heartbeat.go`:

```go
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
```

Update `heartbeat.go` imports: add `"bytes"`, drop `"strconv"` if nothing else in the file uses it (verify with `go build`).

- [ ] **Step 7: Update the one existing caller in `broadcastHeartbeat`**

Inside the goroutine loop, replace `n.appendEntriesFrom(ctx, peer, term)` with:

```go
			go func(peer string) {
				n.mu.RLock()
				req := appendEntriesRequest{
					Term:         term,
					LeaderID:     n.id,
					PrevLogIndex: n.lastLogIndex(),
					PrevLogTerm:  n.lastLogTerm(),
					LeaderCommit: n.commitIndex,
				}
				n.mu.RUnlock()
				results <- n.appendEntriesFrom(ctx, peer, req)
			}(peer)
```

`n.commitIndex` does not exist until Task 5 — for this task use `LeaderCommit: 0`. Task 5 replaces that line.

This still sends **no entries** — followers with matching logs succeed, and a follower behind the leader will now correctly return `Success: false`. Fixing that is Task 4.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./... -v`

Expected: PASS, including the 5 new `heartbeat_test.go` tests and 3 new `truncateAndAppend` tests.

- [ ] **Step 9: Verify clean**

Run: `gofmt -l . && go vet ./... && go build ./... && go test ./...`

- [ ] **Step 10: Commit**

```bash
git add raft.go heartbeat.go log.go log_test.go heartbeat_test.go
git commit -m "$(cat <<'EOF'
Move AppendEntries to POST+JSON and add the log consistency check

Entries can't travel in a query string, so /append-entries now takes a JSON
body carrying term, leader_id, prev_log_index, prev_log_term, entries and
leader_commit. A heartbeat is the same call with entries empty.

Followers now verify prev_log_index/prev_log_term against their own log
before accepting anything, and splice entries in entry-by-entry so a
duplicated RPC can't truncate entries the leader already committed.

A follower that rejects for log mismatch still recognizes the leader and
resets its election timer -- otherwise it would start a pointless election
against a healthy leader while waiting to be repaired. Test pins this.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test:** start three nodes as in Task 2. Steady state should look unchanged — one election, then quiet heartbeats. Then, because `/append-entries` is now POST-only, confirm the protocol change directly:

```bash
# Old bare-GET form must now be refused:
curl -s -w '\n%{http_code}\n' localhost:8081/append-entries    # -> 405

# New form, against a follower (use a term >= its current term):
curl -s -X POST localhost:8081/append-entries \
  -H 'Content-Type: application/json' \
  -d '{"term":1,"leader_id":"fake","prev_log_index":0,"prev_log_term":0,
       "entries":[{"term":1,"key":"a","value":"1"}],"leader_commit":0}'
# -> {"term":1,"success":true}

# Now a deliberate mismatch:
curl -s -X POST localhost:8081/append-entries \
  -H 'Content-Type: application/json' \
  -d '{"term":1,"leader_id":"fake","prev_log_index":9,"prev_log_term":1,
       "entries":[],"leader_commit":0}'
# -> {"term":1,"success":false}   and a "rejected entries" line in that node's log
```

---

## Task 4: nextIndex/matchIndex with decrement-and-retry

**Files:**
- Modify: `raft.go` (add `nextIndex`, `matchIndex` to `node`)
- Modify: `election.go:196-198` (initialize both on winning an election, right after `n.role = leader` / `n.leaderID = n.id`)
- Modify: `heartbeat.go` (`broadcastHeartbeat` sends real entries and reacts to the reply)
- Modify: `main.go` (initialize the maps)
- Modify: `heartbeat_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-3.
- Produces:
  - `n.nextIndex map[string]int`, `n.matchIndex map[string]int` on `node`
  - `func (n *node) buildAppendEntries(peer string, term int) appendEntriesRequest` — caller holds `n.mu` (read is enough)
  - `func (n *node) handleAppendResult(peer string, req appendEntriesRequest, resp appendEntriesResponse)` — caller must NOT hold `n.mu`

- [ ] **Step 1: Write the failing tests**

Add to `heartbeat_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestBuildAppendEntries|TestHandleAppendResult' -v`

Expected: compile failure — `n.buildAppendEntries undefined`, `n.handleAppendResult undefined`, `n.nextIndex undefined`.

- [ ] **Step 3: Add the leader-only fields to `node` in `raft.go`**

After the `log` field:

```go
	// Leader-only bookkeeping, one entry per peer. Reinitialized every time
	// this node wins an election, because a new leader knows nothing about
	// how far behind anyone is and must rediscover it.
	//
	// nextIndex is the leader's *guess* at the next index to send a peer --
	// optimistically lastLogIndex+1, walked backwards on rejection until the
	// logs line up. matchIndex is what the leader *knows* is replicated
	// there, and only it is safe to count toward a majority.
	nextIndex  map[string]int
	matchIndex map[string]int
```

- [ ] **Step 4: Initialize on winning an election, in `election.go`**

In `startElection`, just after `n.role = leader` / `n.leaderID = n.id`:

```go
	n.nextIndex = make(map[string]int, len(n.peers))
	n.matchIndex = make(map[string]int, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1 // optimistic: assume they match us
		n.matchIndex[peer] = 0                   // pessimistic: we know nothing yet
	}
```

- [ ] **Step 5: Add the two functions to `heartbeat.go`**

```go
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
		LeaderCommit: 0, // Task 5 fills this in
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
```

- [ ] **Step 6: Rewrite `broadcastHeartbeat` to use them**

```go
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
```

Add `"sync"` to `heartbeat.go`'s imports. The channel-and-count shape is gone: each reply is now folded in by its own goroutine via `handleAppendResult`, so there is nothing left for the collector loop to do.

- [ ] **Step 7: Initialize the maps in `main.go`**

In the `node` literal, after `log:`:

```go
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./... -v`

Expected: PASS — 6 new tests plus everything from Tasks 1-3.

- [ ] **Step 9: Verify clean, including the race detector**

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./... && go test -race ./...
```

- [ ] **Step 10: Commit**

```bash
git add raft.go heartbeat.go election.go main.go heartbeat_test.go
git commit -m "$(cat <<'EOF'
Track nextIndex/matchIndex and repair follower logs

A new leader now initializes nextIndex optimistically to lastLogIndex+1 and
matchIndex pessimistically to 0 for every peer, then sends each peer the
suffix of the log from its own nextIndex. A rejection walks nextIndex back
one and retries on the next round until the logs line up.

The two failure modes are deliberately kept apart: a higher term in the
reply means step down, while an equal term with success=false means only
that the logs disagree. Conflating them would make a leader resign over a
routine repair.

Entries are copied out of the log under the lock rather than slice-aliased,
since a later append can reallocate the backing array while a request is
still in flight.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test:** three nodes as before. Write several keys to the leader, then watch a follower catch up:

```bash
for i in 1 2 3; do curl -s "localhost:<LEADER>/put?key=k$i&value=$i"; done

# Kill a follower, write more, restart it, and watch the repair in its log:
#   "recognized <leader> as leader for term N"
# and on the leader, if repair was needed:
#   "<peer> rejected entries; backing nextIndex to N"
```

Entries now reach followers, but nothing is committed or applied on them yet — a follower's `/get` still 404s. That's Task 5.

---

## Task 5: Commit on majority and apply to the store

**Files:**
- Modify: `raft.go` (add `commitIndex`, `lastApplied`)
- Create: `log.go` additions (`majorityMatchIndex`, `advanceCommitIndex`, `applyCommitted`)
- Modify: `heartbeat.go` (send `LeaderCommit`; advance commit after a successful reply; followers adopt `leaderCommit`)
- Modify: `store.go` (drop the placeholder immediate `n.store.put`)
- Modify: `main.go`
- Modify: `log_test.go`, `heartbeat_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces:
  - `n.commitIndex int`, `n.lastApplied int` on `node`
  - `func (n *node) majorityMatchIndex() int` — caller holds `n.mu`
  - `func (n *node) advanceCommitIndex()` — caller holds `n.mu` (write)
  - `func (n *node) applyCommitted()` — caller holds `n.mu` (write); takes `store.mu` internally

- [ ] **Step 1: Write the failing tests**

Add to `log_test.go`:

```go
func TestMajorityMatchIndexThreeNodes(t *testing.T) {
	n := testNode(t, "leader")
	n.peers = []string{"p1", "p2"}
	n.currentTerm = 1
	n.log = append(n.log, logEntry{Term: 1}, logEntry{Term: 1})

	// Leader has index 2 by definition; p1 has 2; p2 has nothing.
	// Majority of 3 is 2 nodes -> index 2 is replicated on a majority.
	n.matchIndex = map[string]int{"p1": 2, "p2": 0}

	if got := n.majorityMatchIndex(); got != 2 {
		t.Fatalf("majorityMatchIndex() = %d, want 2", got)
	}
}

func TestMajorityMatchIndexWhenOnlyLeaderHasIt(t *testing.T) {
	n := testNode(t, "leader")
	n.peers = []string{"p1", "p2"}
	n.currentTerm = 1
	n.log = append(n.log, logEntry{Term: 1})
	n.matchIndex = map[string]int{"p1": 0, "p2": 0}

	if got := n.majorityMatchIndex(); got != 0 {
		t.Fatalf("majorityMatchIndex() = %d, want 0 -- one node alone is not a majority", got)
	}
}

// Figure 8 of the paper: a leader may only commit an entry from its OWN
// term by counting replicas. Committing an older-term entry that way can be
// undone by a later leader, silently losing an acknowledged write.
func TestAdvanceCommitIndexRefusesEntriesFromOlderTerms(t *testing.T) {
	n := testNode(t, "leader")
	n.peers = []string{"p1", "p2"}
	n.role = leader
	n.currentTerm = 3
	n.log = append(n.log, logEntry{Term: 1, Key: "old", Value: "x"})
	n.matchIndex = map[string]int{"p1": 1, "p2": 1} // replicated everywhere

	n.advanceCommitIndex()

	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0 -- entry is from term 1, leader is in term 3", n.commitIndex)
	}
}

func TestAdvanceCommitIndexCommitsCurrentTermEntry(t *testing.T) {
	n := testNode(t, "leader")
	n.peers = []string{"p1", "p2"}
	n.role = leader
	n.currentTerm = 3
	n.log = append(n.log, logEntry{Term: 1, Key: "old", Value: "x"})
	n.log = append(n.log, logEntry{Term: 3, Key: "new", Value: "y"})
	n.matchIndex = map[string]int{"p1": 2, "p2": 0}

	n.advanceCommitIndex()

	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2", n.commitIndex)
	}
}

func TestApplyCommittedWritesToStoreExactlyOnce(t *testing.T) {
	n := testNode(t, "n1")
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})
	n.log = append(n.log, logEntry{Term: 1, Key: "b", Value: "2"})
	n.commitIndex = 2

	n.applyCommitted()

	if v, ok := n.store.get("a"); !ok || v != "1" {
		t.Fatalf("store[a] = %q,%v; want \"1\",true", v, ok)
	}
	if v, ok := n.store.get("b"); !ok || v != "2" {
		t.Fatalf("store[b] = %q,%v; want \"2\",true", v, ok)
	}
	if n.lastApplied != 2 {
		t.Fatalf("lastApplied = %d, want 2", n.lastApplied)
	}

	// A second call must be a no-op, not a replay.
	n.store.put("a", "clobbered")
	n.applyCommitted()
	if v, _ := n.store.get("a"); v != "clobbered" {
		t.Fatal("applyCommitted replayed an already-applied entry")
	}
}

func TestApplyCommittedStopsAtCommitIndex(t *testing.T) {
	n := testNode(t, "n1")
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})
	n.log = append(n.log, logEntry{Term: 1, Key: "b", Value: "2"})
	n.commitIndex = 1 // only the first entry is committed

	n.applyCommitted()

	if _, ok := n.store.get("b"); ok {
		t.Fatal("applied an uncommitted entry -- it could still be rolled back")
	}
}
```

Add to `heartbeat_test.go`:

```go
func TestAppendEntriesAdoptsLeaderCommitAndApplies(t *testing.T) {
	n := testNode(t, "n1")

	resp := postAppendEntries(t, n, appendEntriesRequest{
		Term: 1, LeaderID: "n2",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []logEntry{{Term: 1, Key: "a", Value: "1"}},
		LeaderCommit: 1,
	})

	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if n.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1", n.commitIndex)
	}
	if v, ok := n.store.get("a"); !ok || v != "1" {
		t.Fatalf("store[a] = %q,%v -- a committed entry should be applied", v, ok)
	}
}

// A follower must never trust leaderCommit past the end of its own log:
// the leader may have committed entries this follower hasn't received.
func TestAppendEntriesClampsLeaderCommitToOwnLog(t *testing.T) {
	n := testNode(t, "n1")

	postAppendEntries(t, n, appendEntriesRequest{
		Term: 1, LeaderID: "n2",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []logEntry{{Term: 1, Key: "a", Value: "1"}},
		LeaderCommit: 99,
	})

	if n.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1 -- clamped to this node's last index", n.commitIndex)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestMajorityMatchIndex|TestAdvanceCommitIndex|TestApplyCommitted|TestAppendEntriesAdopts|TestAppendEntriesClamps' -v`

Expected: compile failure — `n.commitIndex undefined`, `n.majorityMatchIndex undefined`, etc.

- [ ] **Step 3: Add the fields to `node` in `raft.go`**

After `matchIndex`:

```go
	// commitIndex is the highest index known to be replicated on a majority
	// and therefore safe to apply -- the line between "the leader has this"
	// and "this write can never be lost." lastApplied is how far the store
	// has actually consumed. Both start at 0 and only move forward.
	commitIndex int
	lastApplied int
```

- [ ] **Step 4: Add the three functions to `log.go`**

```go
// majorityMatchIndex is the highest log index that a majority of the cluster
// (this leader plus its peers) is known to hold.
//
// Sort every node's match position descending and take the element at the
// majority boundary: with 3 nodes, needing 2, that's the 2nd highest. Whatever
// index sits there is held by at least that many nodes.
//
// Caller holds n.mu.
func (n *node) majorityMatchIndex() int {
	matches := make([]int, 0, len(n.peers)+1)
	matches = append(matches, n.lastLogIndex()) // the leader holds its own log
	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[peer])
	}

	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	needed := len(matches)/2 + 1
	return matches[needed-1]
}

// advanceCommitIndex moves commitIndex forward if a majority has caught up,
// then applies whatever that newly makes safe.
//
// The currentTerm guard is Figure 8 of the paper, and it is subtle: a leader
// may NOT commit an entry from an earlier term just because it now sits on a
// majority of logs. Such an entry can still be overwritten by a future
// leader, so acknowledging it would mean losing a write we already promised.
// An entry from the leader's own term, once on a majority, is safe -- and
// committing it commits everything before it by extension.
//
// Caller holds n.mu (write).
func (n *node) advanceCommitIndex() {
	if n.role != leader {
		return
	}

	candidate := n.majorityMatchIndex()
	if candidate <= n.commitIndex {
		return
	}
	if n.termAt(candidate) != n.currentTerm {
		return
	}

	n.commitIndex = candidate
	n.applyCommitted()
}

// applyCommitted hands every entry between lastApplied and commitIndex to
// the KV map, in order. This is the only place the map is written on the
// replication path, which is what makes every node's map a replay of the
// same ordered log.
//
// Caller holds n.mu (write); this takes store.mu internally. Lock ordering
// is n.mu then store.mu, never the reverse.
func (n *node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		n.store.put(entry.Key, entry.Value)
	}
}
```

Add `"sort"` to `log.go`'s imports.

- [ ] **Step 5: Send and honor `leaderCommit`**

In `heartbeat.go` `buildAppendEntries`, replace the placeholder:

```go
		LeaderCommit: n.commitIndex,
```

In `heartbeat.go` `handleAppendResult`, inside the `if resp.Success` branch, after updating `nextIndex`:

```go
		n.advanceCommitIndex()
		return
```

In `heartbeat.go` `handleAppendEntries`, inside `if ok { ... }` right after `n.truncateAndAppend(...)`:

```go
		// Adopt the leader's commit point, clamped to what we actually hold:
		// the leader may have committed entries that haven't reached us yet.
		if req.LeaderCommit > n.commitIndex {
			n.commitIndex = min(req.LeaderCommit, n.lastLogIndex())
			n.applyCommitted()
		}
```

(`min` is a Go 1.21+ builtin — no import needed.)

- [ ] **Step 6: Remove the placeholder write from `handlePut`**

In `store.go`, delete these two lines added in Task 2:

```go
	// The entry is in the leader's log but has not been replicated to anyone,
	// ... (the whole comment block)
	n.store.put(key, q.Get("value"))
```

The entry now reaches the map only via `applyCommitted`, once a majority holds it.

- [ ] **Step 7: Initialize in `main.go`** — nothing to add; `commitIndex` and `lastApplied` are correct at their zero values. Confirm the `node` literal still compiles.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./... -v`

- [ ] **Step 9: Verify clean, with race detector**

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./... && go test -race ./...
```

- [ ] **Step 10: Commit**

```bash
git add raft.go log.go heartbeat.go store.go log_test.go heartbeat_test.go
git commit -m "$(cat <<'EOF'
Commit entries on majority replication and apply them

The leader now advances commitIndex to the highest index a majority holds,
and both sides apply everything up to commitIndex into the KV map. A
follower adopts the leader's commit point clamped to its own last index,
since the leader may have committed entries that haven't arrived yet.

Includes the Figure 8 guard: a leader may only commit an entry from its own
term by counting replicas. An older-term entry sitting on a majority can
still be overwritten by a future leader, so committing it would mean losing
a write we already acknowledged.

/put no longer writes to the map directly -- entries reach it only through
applyCommitted, so every node's map is a replay of the same ordered log.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test — the first real payoff:**

```bash
# Write to the leader, then read from a FOLLOWER:
curl "localhost:<LEADER>/put?key=hello&value=world"
sleep 2   # wait for a heartbeat round to carry the commit
curl "localhost:<FOLLOWER-A>/get?key=hello"   # -> world
curl "localhost:<FOLLOWER-B>/get?key=hello"   # -> world
```

That read from a follower is data that replicated. Before this task it was a 404.

---

## Task 6: `/put` waits for its entry to commit

**Files:**
- Modify: `raft.go` (add `replicateCh`)
- Modify: `store.go` (`handlePut` waits)
- Modify: `heartbeat.go` (`runHeartbeats` also wakes on `replicateCh`)
- Modify: `main.go`
- Modify: `store_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-5.
- Produces: `n.replicateCh chan struct{}` (buffered, size 1); `func (n *node) waitForCommit(ctx context.Context, index int) bool`

- [ ] **Step 1: Write the failing tests**

Add to `store_test.go`:

```go
import (
	"context"
	"time"
)

func TestWaitForCommitReturnsTrueOnceCommitted(t *testing.T) {
	n := testNode(t, "n1")
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
	n.log = append(n.log, logEntry{Term: 1, Key: "a", Value: "1"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if n.waitForCommit(ctx, 1) {
		t.Fatal("waitForCommit = true, want false when the request is cancelled")
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestWaitForCommit|TestHandlePutReturnsError' -v`

Expected: compile failure — `n.waitForCommit undefined`, `n.replicateCh undefined`.

- [ ] **Step 3: Add `replicateCh` to `node` in `raft.go`**

After `resetCh`:

```go
	// replicateCh nudges the leader's heartbeat loop to send a round right
	// now rather than waiting for the next tick. Without it every /put would
	// stall until the heartbeat interval elapsed. Buffered size 1 with a
	// non-blocking send, same pattern as resetCh: a pending nudge already
	// covers any later one.
	replicateCh chan struct{}
```

- [ ] **Step 4: Add `waitForCommit` to `store.go`**

```go
// waitForCommit blocks until the entry at index is committed, the request is
// cancelled, or the client's deadline passes. Returns whether it committed.
//
// Polling rather than a sync.Cond broadcast: Cond over an RWMutex is awkward,
// and at this scale a 5ms poll is invisible. The tradeoff is deliberate --
// swap in a Cond if the write path ever gets hot.
func (n *node) waitForCommit(ctx context.Context, index int) bool {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.RLock()
		committed := n.commitIndex >= index
		stillLeader := n.role == leader
		n.mu.RUnlock()

		if committed {
			return true
		}
		// Losing leadership mid-write means this entry may be overwritten by
		// the next leader. Fail rather than leave the client hanging.
		if !stillLeader {
			return false
		}

		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}
```

Add `"context"` and `"time"` to `store.go`'s imports.

- [ ] **Step 5: Make `handlePut` wait**

Replace the tail of `handlePut` (from the `index := n.appendCommand(...)` line):

```go
	index := n.appendCommand(key, q.Get("value"))
	n.mu.Unlock()

	// Kick a replication round immediately instead of waiting for the next
	// heartbeat tick.
	select {
	case n.replicateCh <- struct{}{}:
	default:
	}

	if !n.waitForCommit(r.Context(), index) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, notLeaderResponse{Error: "write not committed: no majority acknowledged in time"})
		return
	}

	log.Printf("[%s] committed %s=%s at index %d", n.id, key, q.Get("value"), index)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("OK\n"))
}
```

- [ ] **Step 6: Wake `runHeartbeats` on `replicateCh`**

In `heartbeat.go`, add a case to the `select` inside `runHeartbeats`:

```go
		case <-n.replicateCh:
			n.mu.RLock()
			stillLeader := n.role == leader && n.currentTerm == term
			n.mu.RUnlock()
			if !stillLeader {
				return
			}
			n.broadcastHeartbeat(ctx, term)
```

- [ ] **Step 7: Initialize in `main.go`**

In the `node` literal, after `resetCh`:

```go
		replicateCh:        make(chan struct{}, 1),
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./... -v`

- [ ] **Step 9: Verify clean, with race detector**

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./... && go test -race ./...
```

- [ ] **Step 10: Commit**

```bash
git add raft.go store.go heartbeat.go main.go store_test.go
git commit -m "$(cat <<'EOF'
Make /put wait for a majority before answering OK

The client no longer gets OK the moment the leader appends. handlePut now
blocks until the entry's index is committed, and returns 503 if it never is
-- so a successful response finally means what it claims: a majority holds
this write and it survives the leader dying.

Appending also nudges replicateCh so the leader replicates immediately
rather than waiting out the heartbeat interval.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test — the property this whole item exists for:**

```bash
# 1. Normal write is still fast (should be well under a second):
time curl "localhost:<LEADER>/put?key=a&value=1"

# 2. Break the majority: kill BOTH followers, then write.
#    The leader is alone, cannot reach a majority, and must NOT claim success.
curl -s -w '\n%{http_code}\n' "localhost:<LEADER>/put?key=b&value=2"
# -> 503, "write not committed"    (before this task: a bogus 200)

# 3. Restart the followers, write again, then kill the LEADER and
#    read from whichever node wins the next election:
curl "localhost:<NEW-LEADER>/get?key=a"    # -> 1, survived the leader's death
```

---

## Task 7: Log up-to-dateness check in RequestVote (TODO item 4)

**Files:**
- Modify: `election.go:18-72` (`handleRequestVote`), `election.go:206-228` (`requestVoteFrom`)
- Create: `log.go` addition (`logIsUpToDate`)
- Create: `election_test.go`
- Modify: `TODO.md`

**Interfaces:**
- Consumes: everything from Tasks 1-6.
- Produces: `func (n *node) logIsUpToDate(candidateLastIndex, candidateLastTerm int) bool` — caller holds `n.mu`

- [ ] **Step 1: Write the failing tests**

Create `election_test.go`:

```go
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
		name             string
		candIndex, candTerm int
		want             bool
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestLogIsUpToDate|TestRequestVote' -v`

Expected: `n.logIsUpToDate undefined`, and `TestRequestVoteRejectsStaleLogCandidate` failing because the check doesn't exist yet.

- [ ] **Step 3: Add `logIsUpToDate` to `log.go`**

```go
// logIsUpToDate reports whether a candidate's log is at least as current as
// this node's, per section 5.4.1 of the paper: compare last terms first, and
// only on a tie does length decide.
//
// Term before length, and the order is not arbitrary. A longer log is not
// automatically better -- a node can pile up entries from a term that never
// committed anywhere, while a shorter log ending in a newer term reflects
// what the cluster actually agreed on. This comparison is what enforces
// Leader Completeness: a candidate missing a committed entry can never
// assemble a majority, because every node holding that entry refuses it.
//
// Caller holds n.mu.
func (n *node) logIsUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	ourTerm := n.lastLogTerm()
	if candidateLastTerm != ourTerm {
		return candidateLastTerm > ourTerm
	}
	return candidateLastIndex >= n.lastLogIndex()
}
```

- [ ] **Step 4: Use it in `handleRequestVote`**

In `election.go`, parse the two new params after `candidateID`/`term`:

```go
	// Absent or unparseable means 0, which is correct for a candidate with an
	// empty log -- and a candidate that omits them simply loses any vote from
	// a node that has entries, which is the safe default.
	lastLogIndex, _ := strconv.Atoi(q.Get("lastLogIndex"))
	lastLogTerm, _ := strconv.Atoi(q.Get("lastLogTerm"))
```

Replace the grant decision:

```go
	granted := (n.votedFor == "" || n.votedFor == candidateID) &&
		n.logIsUpToDate(lastLogIndex, lastLogTerm)
	if granted {
		n.setTermAndVote(n.currentTerm, candidateID)
	}
```

- [ ] **Step 5: Send the fields in `requestVoteFrom`**

In `election.go`, replace the URL construction:

```go
	n.mu.RLock()
	lastIndex, lastTerm := n.lastLogIndex(), n.lastLogTerm()
	n.mu.RUnlock()

	url := "http://" + peer + "/request-vote" +
		"?term=" + strconv.Itoa(term) +
		"&candidateId=" + n.id +
		"&lastLogIndex=" + strconv.Itoa(lastIndex) +
		"&lastLogTerm=" + strconv.Itoa(lastTerm)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./... -v`

- [ ] **Step 7: Verify clean, with race detector**

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./... && go test -race ./...
```

- [ ] **Step 8: Update `TODO.md`**

Mark items 3 and 4 `-- DONE` with a Fix/Verified block matching the style of items 0-2. Update the "Current state" paragraph at the top, which still says "Log replication does not exist, and the KV store is not connected to Raft at all." Add a new entry under **Bugs and smaller gaps**:

```markdown
- **The log itself is not persisted.** Figure 2 marks `log[]` as persistent
  state alongside `currentTerm`/`votedFor`, but [log.go](log.go) keeps it in
  memory only. A committed write survives the leader crashing, but not a
  majority restarting at once -- they come back with empty logs and no
  memory of the entry. Fix: extend [persist.go](persist.go) to write the log,
  ideally append-only rather than rewriting a JSON blob per entry.
```

- [ ] **Step 9: Commit**

```bash
git add election.go log.go election_test.go TODO.md
git commit -m "$(cat <<'EOF'
Reject votes for candidates with a stale log

RequestVote now carries lastLogIndex/lastLogTerm and a voter grants only if
the candidate's log is at least as up to date as its own: last term first,
length only as the tiebreak. A longer log is not automatically better -- a
node can accumulate entries from a term that never committed, while a
shorter log ending in a newer term reflects what the cluster agreed on.

This enforces Leader Completeness. Without it a node missing a committed
entry could win an election and silently destroy that write.

Closes TODO items 3 and 4; records the remaining gap that log[] is still
in-memory only.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

**Manual test — the full end-to-end story:**

```bash
# Three nodes, fresh state dirs.
# 1. Write several keys to the leader.
for i in 1 2 3; do curl -s "localhost:<LEADER>/put?key=k$i&value=$i"; done

# 2. Read them from every node -- all three agree.
for p in 8080 8081 8082; do curl -s "localhost:$p/get?key=k2"; done

# 3. Kill the leader. A new one is elected. Every committed write is still
#    there, because only a node holding them could have won:
curl -s "localhost:<NEW-LEADER>/get?key=k3"    # -> 3

# 4. Restart the old leader; it rejoins as a follower and catches up:
curl -s "localhost:<OLD-LEADER>/get?key=k3"    # -> 3
```

---

## Self-Review

**Spec coverage** (TODO.md items 3 and 4):

| Spec requirement | Task |
| --- | --- |
| `log[]` state | 1 |
| `commitIndex`, `lastApplied` | 5 |
| `nextIndex[]`, `matchIndex[]` | 4 |
| `prevLogIndex`/`prevLogTerm` fields + consistency check | 3 |
| `entries[]` field | 3 |
| `leaderCommit` field | 5 |
| Append on write | 2 |
| Replicate, wait for majority, then answer client | 6 |
| Followers apply once `leaderCommit` allows | 5 |
| Decrement-and-retry on mismatch | 4 |
| `lastLogIndex`/`lastLogTerm` in RequestVote + up-to-dateness | 7 |

No gaps.

**Type consistency check:** `logEntry` fields (`Term`/`Key`/`Value`) are used identically in Tasks 1-7. `appendEntriesRequest` is introduced in Task 3 and its `LeaderCommit` placeholder (`0`) is explicitly replaced in Task 5, Step 5. `appendEntriesFrom`'s signature changes in Task 3 Step 6 and every caller is updated in Task 3 Step 7 and Task 4 Step 6. `n.commitIndex` is referenced in Task 3 Step 7 with an explicit note that it doesn't exist yet and must be `0` until Task 5.

**Known deviations from real Raft, deliberately kept:**
- `nextIndex` backs up one index at a time rather than skipping a conflicting term wholesale. Slower to repair, obviously correct, and irrelevant at this log size.
- The log is in-memory. Recorded as a TODO entry in Task 7 rather than silently ignored.
- The pre-existing bugs listed in TODO.md (single-node cluster never elects, `startElection` blocking the timer, no cluster-config agreement) are untouched by this plan.

---

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-08-17-log-replication.md`.
