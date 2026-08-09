# TODO

- **No heartbeats yet, so leaders don't stay leaders.** `runElectionTimer`
  ([main.go:328](main.go#L328)) keeps running regardless of role, so a node
  that just won an election has nothing resetting its own timer. Within one
  more election-timeout window it fires again, re-declares candidacy, and
  re-elects itself, bumping the term for no reason. Fix: implement leader
  heartbeats (`AppendEntries` with no entries) sent periodically to
  followers, which should reset both the followers' timers and suppress the
  leader's own.

- **Concurrency in `startElection`/`requestVoteFrom` is untested by the race
  detector.** `go run -race` requires cgo, and this machine has
  `CGO_ENABLED=0` with no C compiler (gcc/mingw) installed. The vote-counting
  logic ([main.go:352](main.go#L352)) has been reviewed by hand but not
  machine-verified for data races. Fix: install a C compiler (e.g. mingw-w64)
  and run `go run -race .` across a multi-node test, or run the race
  detector in CI/WSL where cgo is available.
