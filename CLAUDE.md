# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A from-scratch Raft consensus implementation in Go (single package `main`, no
external dependencies). It's a **learning project**: the goal is to
implement leader election, log replication, and majority commit correctly,
not to build a production system. Cluster membership changes, log
compaction/snapshotting, and linearizable reads are deliberately out of
scope (see TODO.md).

**Read `README.md` before making non-trivial changes** — it documents the
election and write-path state machines (with sequence diagrams), the
key invariants (log[0] sentinel, matchIndex vs. nextIndex, the Figure 8
term-guard on commit), and the two subtle-but-load-bearing behaviors
(a follower rejecting on log mismatch still resets its election timer;
a leader may only commit entries from its own term by counting replicas).
`TODO.md` tracks the current gap analysis and known bugs — check it before
assuming something is unimplemented or broken by accident.

## Commands

```bash
go build .              # build the binary
go vet ./...
gofmt -l .              # should print nothing
go test ./...           # all unit tests
go test -race ./...     # concurrency-safety check (run before any commit touching Raft logic)
go test -run TestName ./...   # single test
```

Manual 3-node smoke test (one node per terminal):

```bash
./distkvstore -addr :8080 -id node1 -peers localhost:8081,localhost:8082
./distkvstore -addr :8081 -id node2 -peers localhost:8080,localhost:8082
./distkvstore -addr :8082 -id node3 -peers localhost:8080,localhost:8081
```

Then `curl localhost:8080/ping` to find the leader, `curl "<leader>/put?key=a&value=1"`,
and `curl "<other-node>/get?key=a"` to confirm replication. State persists to
`data/<id>.state.json` (gitignored). See README.md's "Using it" section for
the full set of manual scenarios worth checking by hand (failover, no-majority
503, etc.) — these aren't covered by the unit tests.

## Architecture

One file per Raft RPC, holding **both sides** of it — the asking code and the
answering code live together because the rules on each side only make sense
read as a pair:

| File | Contents |
| --- | --- |
| `raft.go` | `role`, the `node` struct (all shared state), RPC request/response types, `logEntry` |
| `election.go` | `RequestVote` — timer, candidate side (`startElection`, `requestVoteFrom`) and voter side (`handleRequestVote`) |
| `heartbeat.go` | `AppendEntries` — leader side (`runHeartbeats`, `broadcastHeartbeat`, `appendEntriesFrom`) and follower side (`handleAppendEntries`) |
| `log.go` | pure log helpers: indexing, `logIsUpToDate`, `majorityMatchIndex`, `advanceCommitIndex`, `applyCommitted` |
| `store.go` | the KV map + `/get`, `/put` (leader check, append, `waitForCommit`) |
| `persist.go` | `currentTerm`/`votedFor` durability across restarts (`setTermAndVote`, `loadPersistedState`) |
| `ping.go` | `/ping`, `/ping-peer` — manual peer-reachability check, not part of Raft |
| `main.go` | flags, `node` construction, route registration, HTTP server + graceful shutdown |

Everything is one package (`main`) and one `node` struct — there is no
`raft/` subpackage split (considered and deliberately deferred; see TODO.md
item 0).

**Core invariants to preserve when touching Raft logic:**

- The log is the source of truth; the KV map is only ever updated by
  `applyCommitted` replaying committed log entries — never write to the map
  directly from a request handler.
- `n.log[0]` is a permanent sentinel entry so a Go slice index always equals
  a Raft log index. Don't introduce `index-1` arithmetic to work around this.
- `matchIndex` is only set from a confirmed `AppendEntries` reply (proven);
  `nextIndex` is an optimistic guess corrected by rejections. Only
  `matchIndex` feeds `majorityMatchIndex`.
- A node adopting a higher term reverts to `follower` immediately, before
  anything else in that RPC handler runs.
- `currentTerm`/`votedFor` changes must go through `setTermAndVote` (persists
  before returning) — never assign those fields directly.
- **The replicated log itself is not yet persisted** (only
  `currentTerm`/`votedFor` are) — this is the largest known correctness gap,
  tracked in TODO.md item on "log itself is not persisted."

# Working mode for this project

I am using this project to learn Go and distributed systems concepts,
not to produce working software as fast as possible. Optimize for my
understanding, not for task completion.

## Rules

- Do NOT write, generate, or complete code for me by default — not
  even "small" snippets, examples, or scaffolding — unless I explicitly
  say "write the code" or "implement this" in that message.
- When I ask how to do something, explain:
  - the concept/mechanism involved
  - the relevant Go idioms or stdlib/library pieces I'd use
  - the tradeoffs or design decisions involved
  - pseudocode is fine if it helps, but not compilable Go
- If I'm about to make a design mistake, tell me directly and explain
  why — don't silently "fix" it by writing correct code instead.
- If I paste my own code, review/critique it, point out bugs or bad
  patterns, but don't rewrite it wholesale unless asked.
- Assume I want to struggle with the implementation myself. Struggling
  is the point. Don't shortcut it out of helpfulness.
- Only write actual code when I say "write the code," "implement this,"
  or "just show me" explicitly in that message.

## Current stage

Deploying distkvstore to a Kubernetes cluster, with each Raft/KV
node running in its own container/pod. Focus areas right now:
containerizing a Go binary, pod networking, service discovery between
nodes, and how StatefulSets vs Deployments matter for this use case.
