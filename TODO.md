# TODO

Roadmap from the Raft-paper gap analysis. Ordered: each item is roughly a
single increment, and later ones assume the earlier ones are done.

Current state: leader election works (elections, terms, votes, heartbeats,
failover). Log replication does not exist, and the KV store is not connected
to Raft at all.

---

## 0. Split `main.go` into multiple files (do this first)

**The problem.** [main.go](main.go) is ~730 lines holding four unrelated
concerns: the KV store, the node/type definitions, elections, and heartbeats.
Hard to navigate, and every item below adds to it.

**Fix.** Six files, all staying in `package main` -- a pure move, no logic
changes, no signature changes, nothing newly exported (Go doesn't care which
file a function lives in within a package):

| File | Contents |
| --- | --- |
| `store.go` | `store`, `newStore`, `get`, `put`, `handleGet`, `handlePut` |
| `raft.go` | `role` + `String()`, `node` struct, the four request/response types |
| `election.go` | `randomElectionTimeout`, `runElectionTimer`, `startElection`, `requestVoteFrom`, `handleRequestVote` |
| `heartbeat.go` | `runHeartbeats`, `broadcastHeartbeat`, `appendEntriesFrom`, `handleAppendEntries` |
| `ping.go` | `handlePing`, `handlePingPeer`, `pingOne` |
| `main.go` | flags, `node` construction, route registration, server + shutdown, `writeJSON`, `parsePeers` |

Organizing principle: **one file per Raft RPC, holding both sides of it** --
`election.go` has both the candidate asking (`requestVoteFrom`) and the voter
answering (`handleRequestVote`); `heartbeat.go` likewise. Those pairs only
make sense read together, since each side's rules mirror the other's.
Splitting by "handlers vs. clients" would scatter each RPC across two files.

`writeJSON` and `parsePeers` stay in `main.go` rather than getting a
`util.go`, which tends to become a junk drawer.

**Why first:** it invalidates every `main.go:NNN` reference in this file
(~12 of them), so the references below must be updated in the same commit or
they immediately become misleading.

**Verify:** `go vet`, `go build`, plus a 3-node run confirming an election
still completes and failover still works. A pure move should be provably
identical, so demonstrate it rather than assume it.

*Considered and deferred:* moving Raft into a real `raft/` package with
`main.go` as a thin binary. Better long-term structure, but it forces
export decisions and touches every identifier. Revisit if the code grows
again after log replication.

## 1. Connect `/put` to Raft (leader-only writes)

**The problem.** `/get` and `/put` are registered as `*store` methods
([main.go:692-693](main.go#L692-L693)), so `handlePut`
([main.go:61](main.go#L61)) has no access to `role`, `currentTerm`, or
`peers` -- it structurally *cannot* consult Raft. Every node accepts writes,
each write lands only in that node's local map, and nothing replicates.
Three running nodes are three silently diverging key-value stores. Reproduce:
`curl "localhost:8080/put?key=a&value=1"` then
`curl "localhost:8081/get?key=a"` -> 404.

**Fix.** Move the KV handlers onto `*node` (keeping `store` itself as the
storage type). Reject writes unless `role == leader`; otherwise return a
redirect or a hint pointing at `leaderID`. This doesn't add replication --
it makes the cluster honest about what it can currently guarantee, and
forces the leader-redirect problem that log replication needs anyway.

## 2. Persist `currentTerm` and `votedFor`

**The problem.** Figure 2 of the Raft paper marks these as persistent state,
written to stable storage *before* responding to any RPC. Ours are in-memory
only ([main.go:136-137](main.go#L136-L137)). This is a genuine safety bug,
not a nicety: a node that votes in term 5, crashes, and restarts comes back
with no memory of voting, votes a second time in the same term, and lets two
different candidates each collect a majority -> **two leaders in one term**,
breaking Election Safety.

**Fix.** Write both values to a file (JSON is fine) before responding in
`handleRequestVote` / `handleAppendEntries` and wherever the term changes.
Load on startup.

## 3. Log replication -- Raft's second pillar

The largest item; expect several increments. Nothing here exists yet.

Missing state (Figure 2): `log[]`, `commitIndex`, `lastApplied`, and the
leader's `nextIndex[]` / `matchIndex[]`.

Missing `AppendEntries` fields: `prevLogIndex`, `prevLogTerm`, `entries[]`,
`leaderCommit`. Today `Success`
([main.go:181-189](main.go#L181-L189)) only means "your term checks out",
not log consistency -- that comment marks exactly the gap.

**The mechanism to build.** A client write goes only to the leader, which
appends it to its local log, replicates via `AppendEntries`, waits for a
**majority to acknowledge**, marks it committed, applies it to the map, and
only then answers the client. Followers apply entries once `leaderCommit`
says it's safe. The "wait for a majority before applying" step is the entire
point of Raft -- it's what makes a write survive the leader dying immediately
afterward. Today `/put` returns `OK` as soon as one node's map is updated.

Suggested sub-steps: log struct and append-on-write -> `AppendEntries`
consistency check (`prevLogIndex`/`prevLogTerm`) -> `nextIndex`/`matchIndex`
with decrement-and-retry on mismatch -> `commitIndex` advance on majority ->
apply committed entries to the store.

## 4. Log up-to-dateness check in `RequestVote`

**Do this together with #3, not after.** Real Raft grants a vote only if
`votedFor` is null-or-candidateId **and** the candidate's log is at least as
up-to-date as the voter's. We have the first half
([main.go:306](main.go#L306)); the second is absent, along with the
`lastLogIndex` / `lastLogTerm` request fields.

Currently moot (no logs to compare), but mandatory the moment #3 lands: it
enforces the Leader Completeness Property. Without it a node with a stale log
can win an election and silently destroy committed writes.

---

## Bugs and smaller gaps

- **Single-node cluster never elects a leader.**
  `startElection` returns early when `len(peers) == 0`
  ([main.go:459-461](main.go#L459-L461)) -- but with zero peers `votes = 1`
  and `needed = (0+1)/2 + 1 = 1`, so it already *has* a majority and should
  win instantly. The early return prevents it from ever checking. A one-node
  cluster is legitimate Raft.

- **`startElection` blocks the election timer.** It's called synchronously
  ([main.go:446](main.go#L446)), so the timer isn't re-armed until the
  election finishes. Real Raft keeps the timer running *during* an election so
  a stalled one retries on schedule. Bounded by the 1s client timeout with
  parallel fan-out, so the deviation is small: retry interval becomes
  `election duration + new timeout` instead of just the timeout.

- **No agreement on cluster configuration.** `needed` is derived from each
  node's own `-peers` flag ([main.go:474](main.go#L474)). Misconfigure one
  node's list and different nodes compute different majorities -- split brain
  with no warning. Real Raft treats membership as replicated state.

- **`leaderID` goes briefly stale during a candidacy.** When a node becomes a
  candidate ([main.go:439-441](main.go#L439-L441)), `leaderID` isn't cleared,
  so it still reports the last known leader. Self-corrects on the next
  heartbeat or resolved election. Fix: clear it alongside `votedFor`.

- **Heartbeat failures to a down peer log every attempt.**
  `appendEntriesFrom` ([main.go:622](main.go#L622)) logs every failed call.
  Rare for vote requests (once per election), but a heartbeat against a dead
  peer produces a log line every `heartbeatInterval`, indefinitely. Fix: log
  only the first failure per peer, or suppress repeats within a window.

---

## Deliberately out of scope (real Raft, not needed for this project)

- **Cluster membership changes** (paper §6, joint consensus).
- **Log compaction / snapshotting** (§7).
- **Full client protocol** (§8): request dedup via serial numbers, and the
  read-index/lease mechanism that makes reads linearizable rather than
  possibly-stale. Note that even after #3, a `/get` served by a follower --
  or by a leader that was just deposed -- can return stale data.
