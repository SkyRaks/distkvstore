# TODO

Open work only; finished items are removed once done (git history has them).

Current state: leader election, log replication, and log persistence all work.
A `/put` is answered `OK` only once a majority holds it, and the log survives
restarts, including a majority restarting at once. A 3-pod StatefulSet elects a
leader on a local kind cluster; pod state is still ephemeral and there is no
settled way to reach the pods from the host (see 1a, 1b).

---

## 1. Kubernetes deployment

**Working as of 2026-10-15.** A 3-pod StatefulSet runs on a local kind cluster
(`kind-raft`, node container `raft-control-plane`), elects a leader, and each
pod's `peers=[...]` log line lists exactly the two others -- so the `parsePeers`
self-filter (first DNS label vs `os.Hostname()`, [main.go](main.go)) works in a
real pod. `-id` comes from the downward API (`POD_NAME` <- `metadata.name`) and
is substituted into `args` as `$(POD_NAME)`; the headless Service `raft` gives
each pod the DNS name `raft-N.raft:8080`.

Rebuild loop: `docker build -t raft-node:v1 .` from the repo root (not `k8s/`),
then `kind load docker-image raft-node:v1 --name raft`, then delete the pods so
they restart on the new image.

### 1a. Remaining manifest work

- **`-state-dir` + storage -- the big one.** `-state-dir` defaults to `data`
  inside the container's ephemeral filesystem, so a deleted pod loses
  `currentTerm`/`votedFor` *and* the replicated log. That reopens the
  double-vote-after-crash bug, and makes the failover test unsound. Needs
  `volumeClaimTemplates` (one PVC per pod -- this is what StatefulSets are for)
  plus a `volumeMounts` entry, and `-state-dir` pointed at the mount path.
- **`imagePullPolicy: IfNotPresent`.** Not set. A `:v1` tag means Kubernetes may
  try to pull from a registry even though the image was side-loaded by `kind
  load`. Works by luck right now.
- **Probes.** No `/healthz`, but `/ping` always returns 200 -- use it for
  liveness and readiness. Do not gate readiness on having a leader (a cluster
  mid-election would take all pods out of the Service). Do not pass `-slow`.
- **`podManagementPolicy: Parallel`.** The default `OrderedReady` waits for
  pod-0 Ready before creating pod-1, which suits a consensus group badly.
  Failed peer lookups just count as "vote not granted", so startup races are
  harmless.
- **`publishNotReadyAddresses: true`** on the headless Service, so peers can
  resolve each other before they pass readiness.
- **Replicas:** fixed at 3. Membership is static, so no scaling and no HPA
  (`kubectl scale` is still useful to *simulate* losing a majority).

### 1b. Talking to the pods from outside -- decide an approach

Pod IPs (`10.244.0.x`) are not routable from Windows, and the headless Service
has no cluster IP by design, so there is no address to curl. Separately, the
app image (`debian-trixie-slim`) ships no `curl`/`wget`/shell tools -- verified
-- so `kubectl exec ... curl` does not work either.

Do **not** fix this by adding curl to the app image: it would only let pods
curl each other, not help from the host, and it bloats the thing being
deployed. Debug tooling belongs in a separate throwaway container.

Options, roughly in order of setup cost:
- *`kubectl port-forward raft-N 8080:8080`* (works today, no changes): blocking,
  one terminal per pod, but it is the normal dev workflow. Point it at the
  leader for `/put`.
- *Ephemeral debug container* for testing from **inside** the cluster -- pod DNS
  resolution, pod-to-pod reachability:
  `kubectl debug -it raft-0 --image=curlimages/curl --target=raft-node`
  (shares the pod's network namespace). Or a standalone
  `kubectl run curl --rm -it --image=curlimages/curl -- sh`.
- *NodePort Service per pod* (most realistic, most setup): Kubernetes auto-labels
  each pod `statefulset.kubernetes.io/pod-name=raft-N`, so a Service selecting
  that label targets exactly one pod -> a stable external port per node, and
  the leader can be addressed directly. **Catch:** on kind, NodePorts only reach
  the Windows host if the cluster was created with `extraPortMappings` in a kind
  config file. No such file exists, so this means deleting and recreating the
  cluster (cheap -- all state is ephemeral anyway, see 1a).
- *Ingress / LoadBalancer*: the production answer, overkill here, and
  load-balancing is actively wrong for a write path that must reach the leader.

### 1c. Known sharp edges on Kubernetes (not blockers)

- **Writes through a load-balancing Service mostly fail.** About 2 in 3 `PUT`s
  hit a follower and get 409 with `leader_id`. Retry using `leader_id` as the
  pod DNS name, or address the leader directly (see 1b).
- **Follower `/get` lags ~1 heartbeat** (same staleness seen in Compose).
- **Torn state-file write -> CrashLoopBackOff.** `savePersistedState` is a
  plain `WriteFile` (no fsync, no atomic rename) and a parse failure is
  `log.Fatal` by design. Pod kills and evictions make a mid-write kill likelier
  than on a laptop. Recovery: wipe that pod's PVC; the leader rebuilds it.
- **No `.dockerignore`.** `COPY . .` sends `.git`, `raft-logs/`, `data/` and
  the `.exe` to the builder. Slows the build only; the final stage copies just
  the binary. The image runs as root, which sidesteps PVC permission issues
  for now.
- **`Dockerfile` leftovers from the tutorial it came from.** The build output is
  named `/docker-gs-ping` before being copied to `/raft-node`, and `COPY go.mod
  ./` + `go mod download` is a no-op with zero dependencies. Harmless, but
  confusing to read.
- **Manual scenarios not yet run on Kubernetes.** Failover
  (`kubectl delete pod <leader>`) and the no-majority 503
  (`kubectl scale statefulset raft --replicas=1`) have only been checked under
  Compose. Both need 1a's persistence work first to mean anything.

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
