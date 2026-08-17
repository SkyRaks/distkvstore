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
