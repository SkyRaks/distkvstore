# TODO

Roadmap from the Raft-paper gap analysis. Ordered: each item is roughly a
single increment, and later ones assume the earlier ones are done.

Current state: leader election works (elections, terms, votes, heartbeats,
failover). Log replication does not exist, and the KV store is not connected
to Raft at all.

---

## 0. Split `main.go` into multiple files -- DONE

**The problem.** `main.go` was ~730 lines holding four unrelated
concerns: the KV store, the node/type definitions, elections, and heartbeats.
Hard to navigate, and every item below adds to it.

**Fix.** Six files, all staying in `package main` -- a pure move, no logic
changes, no signature changes, nothing newly exported (Go doesn't care which
file a function lives in within a package):

| File | Contents |
| --- | --- |
| [store.go](store.go) | `store`, `newStore`, `get`, `put`, `handleGet`, `handlePut` |
| [raft.go](raft.go) | `role` + `String()`, `node` struct, the four request/response types |
| [election.go](election.go) | `randomElectionTimeout`, `runElectionTimer`, `startElection`, `requestVoteFrom`, `handleRequestVote` |
| [heartbeat.go](heartbeat.go) | `runHeartbeats`, `broadcastHeartbeat`, `appendEntriesFrom`, `handleAppendEntries` |
| [ping.go](ping.go) | `handlePing`, `handlePingPeer`, `pingOne` |
| [main.go](main.go) | flags, `node` construction, route registration, server + shutdown, `writeJSON`, `parsePeers` |

Organizing principle: **one file per Raft RPC, holding both sides of it** --
`election.go` has both the candidate asking (`requestVoteFrom`) and the voter
answering (`handleRequestVote`); `heartbeat.go` likewise. Those pairs only
make sense read together, since each side's rules mirror the other's.
Splitting by "handlers vs. clients" would scatter each RPC across two files.

`writeJSON` and `parsePeers` stay in `main.go` rather than getting a
`util.go`, which tends to become a junk drawer.

**Why first:** it invalidated every `main.go:NNN` reference in this file
(~12 of them), so the references below were updated in the same commit rather
than being left to go misleading.

**Verified:** `gofmt -l` clean, `go vet ./...` and `go build ./...` clean, plus
a 3-node run: node3 won term 1 unanimously, term held steady for 8s with no
churn, `/get` `/put` `/ping-peer` unchanged, and killing node3 produced a
clean failover to node1 in term 2 with node2 agreeing. A pure move should be
provably identical, so this was demonstrated rather than assumed.

*Considered and deferred:* moving Raft into a real `raft/` package with
`main.go` as a thin binary. Better long-term structure, but it forces
export decisions and touches every identifier. Revisit if the code grows
again after log replication.

## 1. Connect `/put` to Raft (leader-only writes) -- DONE

**The problem.** `/get` and `/put` were registered as `*store` methods, so
`handlePut` had no access to `role`, `currentTerm`, or `peers` -- it
structurally *could not* consult Raft. Every node accepted writes, each
write landed only in that node's local map, and nothing replicated. Three
running nodes were three silently diverging key-value stores.

**Fix.** Moved `handleGet`/`handlePut` onto `*node`
([store.go](store.go)), keeping `store` itself as the storage type
(`n.store.get`/`n.store.put`). `handlePut` now rejects writes unless
`n.role == leader`, returning `409 Conflict` with a JSON body
`{"error":"not the leader","leader_id":"<id or empty>"}` -- empty when no
leader is currently known (e.g. mid-election), telling the client to just
retry shortly rather than pointing it anywhere.

This doesn't add replication -- a write still only lands on one node's map,
it's just now the *correct* node -- but it makes the cluster honest about
what it can currently guarantee, and forces the leader-redirect problem
that log replication needs anyway.

**Verified:** `gofmt`/`vet`/`build` clean. 3-node run: writes to the leader
succeed and are readable; writes to either follower are rejected with
`leader_id` pointing at the real leader; after killing the leader, the new
leader accepts writes and the remaining follower's rejection correctly
repoints at the new leader.

## 2. Persist `currentTerm` and `votedFor` -- DONE

**The problem.** Figure 2 of the Raft paper marks these as persistent state,
written to stable storage *before* responding to any RPC. They were
in-memory only. This was a genuine safety bug, not a nicety: a node that
votes in term 5, crashes, and restarts comes back with no memory of voting,
votes a second time in the same term, and lets two different candidates
each collect a majority -> **two leaders in one term**, breaking Election
Safety.

**Fix.** New [persist.go](persist.go): `n.setTermAndVote(term, votedFor)`
mutates both fields and writes them to `<state-dir>/<id>.state.json` before
returning, so "changed in memory" and "flushed to disk" never drift apart.
All five call sites that used to assign `currentTerm`/`votedFor` directly
(`handleRequestVote`, `handleAppendEntries`, `runElectionTimer`,
`startElection`, `broadcastHeartbeat`) now go through it. `main.go` gained
a `-state-dir` flag (default `.`) and calls `n.loadPersistedState()` at
startup, before any goroutine runs. A state file that exists but won't
parse is treated as a real problem, not a fresh start: `log.Fatal` rather
than silently resetting to term 0, since silently resetting is exactly the
bug this item closes.

**Verified:** restart preserves term/vote (confirmed via the state file
and `/ping`). The actual double-vote-after-crash scenario was reproduced
and fixed: a node granted a vote in term 5, was killed and restarted, and
then correctly *rejected* a second vote request for term 5 (would have
been granted before this fix). Corrupt state file -> `log.Fatal`, exit
code 1. 3-node election, leader-only writes, and failover all still work
with three nodes sharing one `-state-dir`. `gofmt`/`vet`/`build` clean,
`-race` clean under concurrent stress across all endpoints.

*Left out of this increment:* the write in `savePersistedState` is a plain
`os.WriteFile`, not write-to-temp-then-atomic-rename -- a crash mid-write
could in principle leave a corrupt file. Noted in the code; revisit if it
ever actually bites.

## 3. Log replication -- Raft's second pillar

The largest item; expect several increments. Nothing here exists yet.

Missing state (Figure 2): `log[]`, `commitIndex`, `lastApplied`, and the
leader's `nextIndex[]` / `matchIndex[]`.

Missing `AppendEntries` fields: `prevLogIndex`, `prevLogTerm`, `entries[]`,
`leaderCommit`. Today `Success`
([raft.go:104-112](raft.go#L104-L112)) only means "your term checks out",
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
([election.go:55](election.go#L55)); the second is absent, along with the
`lastLogIndex` / `lastLogTerm` request fields.

Currently moot (no logs to compare), but mandatory the moment #3 lands: it
enforces the Leader Completeness Property. Without it a node with a stale log
can win an election and silently destroy committed writes.

---

## Bugs and smaller gaps

- **Single-node cluster never elects a leader.**
  `startElection` returns early when `len(peers) == 0`
  ([election.go:139-141](election.go#L139-L141)) -- but with zero peers `votes = 1`
  and `needed = (0+1)/2 + 1 = 1`, so it already *has* a majority and should
  win instantly. The early return prevents it from ever checking. A one-node
  cluster is legitimate Raft.

- **`startElection` blocks the election timer.** It's called synchronously
  ([election.go:126](election.go#L126)), so the timer isn't re-armed until the
  election finishes. Real Raft keeps the timer running *during* an election so
  a stalled one retries on schedule. Bounded by the 1s client timeout with
  parallel fan-out, so the deviation is small: retry interval becomes
  `election duration + new timeout` instead of just the timeout.

- **No agreement on cluster configuration.** `needed` is derived from each
  node's own `-peers` flag ([election.go:154](election.go#L154)). Misconfigure one
  node's list and different nodes compute different majorities -- split brain
  with no warning. Real Raft treats membership as replicated state.

- **`leaderID` goes briefly stale during a candidacy.** When a node becomes a
  candidate ([election.go:119-121](election.go#L119-L121)), `leaderID` isn't cleared,
  so it still reports the last known leader. Self-corrects on the next
  heartbeat or resolved election. Fix: clear it alongside `votedFor`.

- **Heartbeat failures to a down peer log every attempt.**
  `appendEntriesFrom` ([heartbeat.go:150](heartbeat.go#L150)) logs every failed call.
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
