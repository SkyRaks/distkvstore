# TODO

- **`leaderId` can go briefly stale during a candidacy.** When a node's own
  election timeout fires and it becomes a candidate ([main.go:344](main.go#L344)),
  `leaderID` isn't cleared -- it still shows whatever leader was last known,
  even though that leader may no longer be in charge. Minor: it self-corrects
  the moment a real heartbeat or a losing candidacy resolves things. Fix (if
  it matters later): clear `leaderID` alongside `votedFor` when becoming a
  candidate.

- **Heartbeat failures to an unreachable peer log on every attempt.**
  `appendEntriesFrom` ([main.go:588](main.go#L588)) logs every failed call,
  same as `requestVoteFrom`. For a vote request that's rare (once per
  election); for a heartbeat firing every `heartbeatInterval` against a
  genuinely down peer, it's a log line every second, indefinitely. Fix (if it
  gets noisy in practice): only log the first failure to a given peer, or
  suppress repeats within some window.
