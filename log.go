package main

import "sort"

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

// majorityMatchIndex is the highest log index that a majority of the cluster
// (this leader plus its peers) is known to hold.
//
// Sort every node's match position descending and take the element at the
// majority boundary: with 3 nodes, needing 2, that's the 2nd highest.
// Whatever index sits there is held by at least that many nodes.
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
