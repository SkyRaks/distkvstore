# TODO

Open work only; finished items are removed once done (git history has them).

Current state: leader election, log replication, and log persistence all work.
A `/put` is answered `OK` only once a majority holds it, and the log survives
restarts, including a majority restarting at once.

---

## 1. Kubernetes deployment

Review of the project for blockers. Nothing stops the code from running on
Kubernetes, but item 1a must be settled first or the majority math is
silently wrong.

### 1a. Self in `-peers` breaks majorities -- MUST FIX FIRST

A StatefulSet has one pod template, so every pod gets the same flags, and the
natural `-peers` value lists all three FQDNs, including the node itself.
Compose avoids this because each service has its own `command:` listing only
the *other* nodes. The code assumes `peers` excludes self:
- `needed := (len(n.peers)+1)/2 + 1` ([election.go:162](election.go#L162))
  computes a 4-node cluster and needs 3 votes.
- `majorityMatchIndex` ([log.go](log.go)) uses `len(n.peers)+1` the same way.
- The node sends `RequestVote`/`AppendEntries` to itself over HTTP.

Nothing crashes; it is the same silent split-brain as the "no agreement on
cluster configuration" bug below. Options:
- *Filter self out of `peers` at startup* (recommended): the pod name is the
  DNS prefix (`raft-0.raft...`), so a peer whose host starts with `-id` is
  self. Do it once, before `node` is built, so both majority calculations stay
  correct.
- *Per-pod args from a shell entrypoint* (debian-slim has `sh`): works, hacky.
- *Three separate StatefulSets/Deployments*: Compose's shape, but gives up
  what StatefulSets are for.

### 1b. Manifest checklist (no code changes)

- **`-id`:** env `POD_NAME` from the downward API (`metadata.name`), then
  `args: ["-id", "$(POD_NAME)"]`. Pod name as id also makes `leader_id` in the
  409 response usable as a DNS prefix.
- **`-state-dir`:** defaults to `data`, relative to `/` -- ephemeral. Point it
  at the PVC mount path. Mandatory (reopens the double-vote-after-crash bug
  otherwise).
- **Storage:** StatefulSet with `volumeClaimTemplates`, one PVC per pod. Not a
  Deployment.
- **Peers:** `raft-N.<headless-svc>.<ns>.svc.cluster.local:8080`. All pods can
  share one port, so the 8080/8081/8082 scheme is not needed.
- **Headless Service:** `clusterIP: None`, plus `publishNotReadyAddresses:
  true`.
- **Startup:** `podManagementPolicy: Parallel`; the default `OrderedReady`
  waits for pod-0 Ready before creating pod-1, which suits a consensus group
  badly. Failed peer lookups just count as "vote not granted", so startup
  races are harmless.
- **Probes:** no `/healthz`, but `/ping` always returns 200 -- use it for
  liveness and readiness. Do not gate readiness on having a leader. Do not
  pass `-slow`.
- **Image:** push to a registry, or load into kind/minikube
  (`kind load docker-image` / `minikube image load`) and set `imagePullPolicy`.
- **Replicas:** fixed at 3. Membership is static, so no scaling and no HPA.

### 1c. Known sharp edges on Kubernetes (not blockers)

- **Writes through a ClusterIP Service mostly fail.** It load-balances, so
  about 2 in 3 `PUT`s hit a follower and get 409 with `leader_id`. Retry using
  `leader_id` as the pod DNS name, or `kubectl port-forward` to the leader.
- **Follower `/get` lags ~1 heartbeat** (same staleness seen in Compose).
- **Torn state-file write -> CrashLoopBackOff.** `savePersistedState` is a
  plain `WriteFile` (no fsync, no atomic rename) and a parse failure is
  `log.Fatal` by design. Pod kills and evictions make a mid-write kill likelier
  than on a laptop. Recovery: wipe that pod's PVC; the leader rebuilds it.
- **No `.dockerignore`.** `COPY . .` sends `.git`, `raft-logs/`, `data/` and
  the `.exe` to the builder. Slows the build only; the final stage copies just
  the binary. The image runs as root, which sidesteps PVC permission issues
  for now.

## 2. Reorganize the project into folders for readability

**The problem.** Everything -- Raft logic, KV store, HTTP handlers, tests,
docs, Docker files -- sits in one flat directory, which is getting hard to
scan.

**Decision to settle first.** In Go a folder is a *package*. Moving `.go`
files into subfolders forces export decisions (lowercase `node`, `store`,
`logEntry` etc. become inaccessible across packages unless capitalized or
wrapped in a constructor), and `main` would have to import them. Pick one
before moving anything:
- *Non-Go files only:* leave all `.go` files in the root and move just docs,
  Docker/Compose files, and sample data into folders (e.g. `docs/`,
  `deploy/`). No code changes, lowest risk.
- *Real packages:* e.g. `raft/` (node, election, heartbeat, log, persist),
  `kv/` (store), and a thin `main.go`. Better structure, touches every
  identifier.

**Watch for:** `Dockerfile` build context and `COPY` paths, `docker-compose.yml`
volume/`build:` paths, the `data/` state dir default, relative links in
README.md, TODO.md and CLAUDE.md (the file table there), and `go test ./...`
still finding the tests.

---

## Bugs and smaller gaps

- **A state file without the `log[0]` sentinel loads silently wrong.**
  `loadPersistedState` trusts the file, so a log missing the leading
  `{term 0}` entry shifts every index by one and drops the first real entry
  with no warning. Options: persist only `log[1:]` and rebuild the sentinel
  on load (it can then never go missing), or keep it in the file and refuse
  to start if `NodeLogs` is empty or `NodeLogs[0].Term != 0`, matching the
  existing "refuse to start on a corrupt file" rule.

- **The log is persisted as one JSON blob per change, with no `File.Sync()`
  and no write-to-temp-then-rename.** Neither efficient nor torn-write safe
  (this also applies to `currentTerm`/`votedFor`). A better design would be
  append-only records (one JSON line per entry, optional checksum), `Sync()`
  before acknowledging, and a way to truncate a conflicting suffix.

- **Leftover debug output.** `fmt.Print` lines in
  [persist.go](persist.go) and [log.go](log.go) (`applyCommitted`) should be
  removed, and `gofmt -w log.go` run (it is currently flagged).

- **Single-node cluster never elects a leader.**
  `startElection` returns early when `len(peers) == 0`
  ([election.go:147](election.go#L147)) -- but with zero peers `votes = 1`
  and `needed = (0+1)/2 + 1 = 1`, so it already *has* a majority and should
  win instantly. The early return prevents it from ever checking. A one-node
  cluster is legitimate Raft.

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
  the next `AppendEntries` carries the advanced `leaderCommit`. Correct Raft,
  not an implementation bug -- see the linearizable-read note under "out of
  scope". Observed in practice with a 3-container Compose cluster (`PUT` to
  the leader, immediate `GET` on a follower -> `404`, fine a few seconds
  later). Open decision: lower the default `-heartbeat-interval` (1s,
  [main.go:44](main.go#L44)), or pass a shorter one via each service's
  `command:`, purely for demo ergonomics.

---

## Notes

- **Compose `ports:` needs both sides set correctly, per service.** Docker's
  `"HOST:CONTAINER"` has two independent numbers: container-side must match
  that node's own `-addr`, and host-side must be unique per service. Wrong
  container-side means traffic lands on a port nothing listens on; a host-side
  collision makes `docker compose up` fail with "port is already allocated".

---

## Deliberately out of scope (real Raft, not needed for this project)

- **Cluster membership changes** (paper §6, joint consensus).
- **Log compaction / snapshotting** (§7).
- **Full client protocol** (§8): request dedup via serial numbers, and the
  read-index/lease mechanism that makes reads linearizable rather than
  possibly-stale. Even now, a `/get` served by a follower -- or by a leader
  that was just deposed -- can return stale data.
