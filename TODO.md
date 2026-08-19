# TODO

Roadmap from the Raft-paper gap analysis. Ordered: each item is roughly a
single increment, and later ones assume the earlier ones are done.

Current state: leader election and log replication both work. A `/put` is
appended to the leader's log, replicated to followers, and only answered
`OK` once a majority holds it -- so a committed write survives the leader
dying. Remaining gap: the log itself is in-memory, so it does not survive a
majority of nodes restarting at once (see Bugs below).

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

## 3. Log replication -- Raft's second pillar -- DONE

Built over six increments; plan in
[docs/superpowers/plans/2026-08-17-log-replication.md](docs/superpowers/plans/2026-08-17-log-replication.md).

**What exists now.** `log[]`, `commitIndex`, `lastApplied`, and the leader's
`nextIndex[]`/`matchIndex[]` ([raft.go](raft.go)); the log helpers in
[log.go](log.go). `AppendEntries` moved from query params to **POST + JSON**
and now carries `prevLogIndex`, `prevLogTerm`, `entries[]` and
`leaderCommit` -- a heartbeat is simply the same call with `entries` empty.

**The mechanism.** A client write reaches only the leader, which appends it
to its log, replicates via `AppendEntries`, waits for a **majority to
acknowledge**, marks it committed, applies it to the map, and only then
answers the client. Followers apply entries once `leaderCommit` says it is
safe, clamped to their own last index. `/put` returns `503` if no majority
acknowledges within `commitTimeout`.

The log is 1-indexed with `log[0]` a permanent sentinel, so a Go slice index
equals a Raft log index -- no `index-1` arithmetic anywhere.

**Two subtleties worth remembering:**
- A follower that rejects for log mismatch still recognizes the leader and
  resets its election timer. Otherwise it would start pointless elections
  against a healthy leader while waiting to be repaired.
- Figure 8: a leader may only commit an entry from its **own** term by
  counting replicas. An older-term entry on a majority can still be
  overwritten by a future leader.

**Bug found while building this:** `advanceCommitIndex` was only reachable
from a peer reply, so a single-node cluster -- where the lone leader is
already a majority of one -- could never commit and `/put` hung forever.
`handlePut` now re-checks commitability after appending. Caught by a
hanging unit test.

**Verified:** 34 unit tests, `-race` clean. 3-node runs confirmed: writes
replicate to all nodes; killing the leader preserves every committed write;
a node restarted with an empty log is rebuilt from scratch by the leader;
and with both followers killed the leader correctly returns 503 rather than
falsely claiming success.

*Deliberate deviation:* `nextIndex` backs up one index per round rather than
skipping a whole conflicting term. Slower to repair, obviously correct,
irrelevant at this log size.

## 4. Log up-to-dateness check in `RequestVote` -- DONE

`RequestVote` now carries `lastLogIndex`/`lastLogTerm`, and a voter grants
only if `votedFor` is null-or-candidateId **and** `logIsUpToDate`
([log.go](log.go)) passes: last term first, length only as the tiebreak.

Term before length is not arbitrary -- a longer log is not automatically
better, since a node can accumulate entries from a term that never
committed, while a shorter log ending in a newer term reflects what the
cluster agreed on.

This enforces the Leader Completeness Property. A committed entry is on a
majority; any election needs a majority; those two majorities overlap; and
the overlapping node refuses to vote for a candidate missing the entry. So
a node with a stale log mathematically cannot win.

---

## Bugs and smaller gaps

- **The log itself is not persisted.** Figure 2 marks `log[]` as persistent
  state alongside `currentTerm`/`votedFor`, but [log.go](log.go) keeps it in
  memory only. A committed write survives the leader crashing, but not a
  majority restarting at once -- they come back with empty logs and no
  memory of the entry, and a client that was told `OK` loses its write.
  Fix: extend [persist.go](persist.go) to write the log, ideally append-only
  rather than rewriting a JSON blob per entry. **This is the largest
  remaining correctness gap.**

- **Single-node cluster never elects a leader.**
  `startElection` returns early when `len(peers) == 0`
  ([election.go:147](election.go#L147)) -- but with zero peers `votes = 1`
  and `needed = (0+1)/2 + 1 = 1`, so it already *has* a majority and should
  win instantly. The early return prevents it from ever checking. A one-node
  cluster is legitimate Raft. (The *commit* half of this was fixed during
  item 3; only the election half remains.)

- **`startElection` blocks the election timer.** It's called synchronously
  ([election.go:134](election.go#L134)), so the timer isn't re-armed until the
  election finishes. Real Raft keeps the timer running *during* an election so
  a stalled one retries on schedule. Bounded by the 1s client timeout with
  parallel fan-out, so the deviation is small: retry interval becomes
  `election duration + new timeout` instead of just the timeout.

- **No agreement on cluster configuration.** `needed` is derived from each
  node's own `-peers` flag ([election.go:162](election.go#L162)), and
  `majorityMatchIndex` ([log.go](log.go)) derives the commit majority the same
  way. Misconfigure one node's list and different nodes compute different
  majorities -- split brain with no warning. Real Raft treats membership as
  replicated state.

- **`leaderID` goes briefly stale during a candidacy.** When a node becomes a
  candidate ([election.go:128](election.go#L128)), `leaderID` isn't cleared,
  so it still reports the last known leader. Self-corrects on the next
  heartbeat or resolved election. Fix: clear it alongside `votedFor`.

- **Heartbeat failures to a down peer log every attempt.**
  `appendEntriesFrom` ([heartbeat.go:256](heartbeat.go#L256)) logs every failed call.
  Rare for vote requests (once per election), but a heartbeat against a dead
  peer produces a log line every `heartbeatInterval`, indefinitely. Fix: log
  only the first failure per peer, or suppress repeats within a window.

- **A follower's `/get` can be stale by one round.** The leader commits as
  soon as a majority *stores* an entry, but followers only *apply* it when
  the next `AppendEntries` carries the advanced `leaderCommit`. So a read
  from a follower immediately after a successful write may miss it. This is
  correct Raft, not a bug in the implementation -- see the linearizable-read
  note under "out of scope".

---

## Deliberately out of scope (real Raft, not needed for this project)

- **Cluster membership changes** (paper §6, joint consensus).
- **Log compaction / snapshotting** (§7).
- **Full client protocol** (§8): request dedup via serial numbers, and the
  read-index/lease mechanism that makes reads linearizable rather than
  possibly-stale. Note that even after #3, a `/get` served by a follower --
  or by a leader that was just deposed -- can return stale data.
