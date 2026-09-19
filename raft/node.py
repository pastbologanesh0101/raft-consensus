"""The Raft node state machine: follower / candidate / leader transitions,
RequestVote handling (leader election) and AppendEntries handling (log
replication), all driven by an injected randomized election timeout and a
send callback supplied by the Cluster. No I/O happens here -- a RaftNode
never talks to the network directly, which is what makes it unit-testable
in isolation (see the "stale log loses election" and "log truncation"
tests, which call these handlers directly with no Cluster involved).
"""

import random
from enum import Enum
from typing import Callable, Dict, List, Optional

from .log import Log, LogEntry
from .rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: List[str],
        send_fn: Callable[[str, object], None],
        rng: Optional[random.Random] = None,
        election_timeout_range=(10, 20),
        heartbeat_interval: int = 3,
        on_state_change: Optional[Callable] = None,
        on_commit: Optional[Callable] = None,
    ):
        self.id = node_id
        self.peer_ids = list(peer_ids)
        self._send = send_fn
        self.rng = rng if rng is not None else random.Random()
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.on_state_change = on_state_change or (lambda *a, **k: None)
        self.on_commit = on_commit or (lambda *a, **k: None)

        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log = Log()
        self.commit_index = 0

        self.leader_id: Optional[str] = None
        self.votes_received: set = set()

        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self._election_elapsed = 0
        self._election_timeout = self._new_election_timeout()
        self._heartbeat_elapsed = 0

        self.alive = True

        if not self.peer_ids:
            # A single-node "cluster" has no one to ask for votes from --
            # it trivially constitutes its own majority.
            self._become_leader()

    # ---------- helpers ----------

    def _new_election_timeout(self) -> int:
        lo, hi = self.election_timeout_range
        return self.rng.randint(lo, hi)

    def _majority(self) -> int:
        return (len(self.peer_ids) + 1) // 2 + 1

    def _set_state(self, new_state: NodeState) -> None:
        if self.state != new_state:
            self.state = new_state
            self.on_state_change(self.id, self.current_term, new_state)

    def committed_entries(self) -> List[LogEntry]:
        return [self.log.get(i) for i in range(1, self.commit_index + 1)]

    # ---------- timer-driven ----------

    def tick(self) -> None:
        if not self.alive:
            return
        if self.state == NodeState.LEADER:
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self.heartbeat_interval:
                self._heartbeat_elapsed = 0
                self._broadcast_append_entries()
        else:
            self._election_elapsed += 1
            if self._election_elapsed >= self._election_timeout:
                self.start_election()

    def start_election(self) -> None:
        if not self.alive:
            return
        self.current_term += 1
        self._set_state(NodeState.CANDIDATE)
        self.voted_for = self.id
        self.votes_received = {self.id}
        self.leader_id = None
        self._election_elapsed = 0
        self._election_timeout = self._new_election_timeout()

        if not self.peer_ids:
            self._become_leader()
            return

        request = RequestVoteRequest(
            term=self.current_term,
            candidate_id=self.id,
            last_log_index=self.log.last_index(),
            last_log_term=self.log.last_term(),
        )
        for peer in self.peer_ids:
            self._send(peer, request)

    def _become_leader(self) -> None:
        self._set_state(NodeState.LEADER)
        self.leader_id = self.id
        self._heartbeat_elapsed = 0
        for peer in self.peer_ids:
            self.next_index[peer] = self.log.last_index() + 1
            self.match_index[peer] = 0
        self._broadcast_append_entries()

    def _step_down(self, term: int) -> None:
        self.current_term = term
        self.voted_for = None
        self._set_state(NodeState.FOLLOWER)
        self._election_elapsed = 0

    # ---------- client interface ----------

    def propose(self, command) -> bool:
        """Client write. Only succeeds on the leader; returns False
        otherwise. Appends to the leader's own log and immediately tries
        to replicate (in addition to the periodic heartbeat)."""
        if self.state != NodeState.LEADER or not self.alive:
            return False
        self.log.append(LogEntry(term=self.current_term, command=command))
        if not self.peer_ids:
            self.commit_index = self.log.last_index()
            self.on_commit(self.id, self.commit_index)
        else:
            self._broadcast_append_entries()
        return True

    # ---------- RequestVote (leader election) ----------

    def handle_request_vote_request(self, req: RequestVoteRequest) -> RequestVoteResponse:
        if not self.alive:
            return RequestVoteResponse(term=self.current_term, vote_granted=False, voter_id=self.id)

        if req.term > self.current_term:
            self._step_down(req.term)

        if req.term < self.current_term:
            return RequestVoteResponse(term=self.current_term, vote_granted=False, voter_id=self.id)

        my_last_index = self.log.last_index()
        my_last_term = self.log.last_term()
        log_is_at_least_as_up_to_date = (req.last_log_term > my_last_term) or (
            req.last_log_term == my_last_term and req.last_log_index >= my_last_index
        )
        may_vote = self.voted_for in (None, req.candidate_id)

        if may_vote and log_is_at_least_as_up_to_date:
            self.voted_for = req.candidate_id
            self._election_elapsed = 0  # granting a vote defers our own timeout
            return RequestVoteResponse(term=self.current_term, vote_granted=True, voter_id=self.id)

        return RequestVoteResponse(term=self.current_term, vote_granted=False, voter_id=self.id)

    def handle_request_vote_response(self, resp: RequestVoteResponse) -> None:
        if not self.alive:
            return
        if resp.term > self.current_term:
            self._step_down(resp.term)
            return
        if self.state != NodeState.CANDIDATE or resp.term != self.current_term:
            return
        if resp.vote_granted:
            self.votes_received.add(resp.voter_id)
            if len(self.votes_received) >= self._majority():
                self._become_leader()

    # ---------- AppendEntries (log replication) ----------

    def _broadcast_append_entries(self) -> None:
        for peer in self.peer_ids:
            self._send_append_entries_to(peer)

    def _send_append_entries_to(self, peer: str) -> None:
        next_idx = self.next_index.get(peer, self.log.last_index() + 1)
        prev_index = next_idx - 1
        prev_term = self.log.term_at(prev_index)
        entries = self.log.entries_from(next_idx)
        request = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        self._send(peer, request)

    def handle_append_entries_request(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        if not self.alive:
            return AppendEntriesResponse(term=self.current_term, success=False, follower_id=self.id)

        if req.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False, follower_id=self.id)

        if req.term > self.current_term or self.state != NodeState.FOLLOWER:
            self._step_down(req.term)
        self.leader_id = req.leader_id
        self._election_elapsed = 0

        if req.prev_log_index > 0:
            term_at_prev = self.log.term_at(req.prev_log_index)
            if term_at_prev != req.prev_log_term:
                conflict_index = min(req.prev_log_index, self.log.last_index() + 1)
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    follower_id=self.id,
                    conflict_index=conflict_index,
                )

        # Append new entries, truncating any conflicting suffix first.
        insert_index = req.prev_log_index + 1
        for i, entry in enumerate(req.entries):
            idx = insert_index + i
            existing = self.log.get(idx)
            if existing is not None and existing.term != entry.term:
                self.log.truncate_after(idx - 1)
                self.log.append_entries(req.entries[i:])
                break
            if existing is None:
                self.log.append_entries(req.entries[i:])
                break
            # existing entry already matches -- keep scanning

        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, self.log.last_index())
            self.on_commit(self.id, self.commit_index)

        return AppendEntriesResponse(
            term=self.current_term,
            success=True,
            follower_id=self.id,
            match_index=req.prev_log_index + len(req.entries),
        )

    def handle_append_entries_response(self, resp: AppendEntriesResponse) -> None:
        if not self.alive:
            return
        if resp.term > self.current_term:
            self._step_down(resp.term)
            return
        if self.state != NodeState.LEADER or resp.term != self.current_term:
            return

        peer = resp.follower_id
        if resp.success:
            self.match_index[peer] = max(self.match_index.get(peer, 0), resp.match_index)
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit_index()
        else:
            if resp.conflict_index:
                self.next_index[peer] = max(1, resp.conflict_index)
            else:
                self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)
            self._send_append_entries_to(peer)

    def _advance_commit_index(self) -> None:
        """The core majority-quorum commit rule: an index is committed once
        it is replicated on a majority of nodes (leader included), AND the
        entry at that index was created in the leader's current term (the
        Raft safety rule that prevents committing -- and then losing -- an
        old-term entry via an indirect majority)."""
        match_values = sorted([self.log.last_index()] + [self.match_index.get(p, 0) for p in self.peer_ids])
        majority_index = match_values[len(match_values) - self._majority()]
        if majority_index > self.commit_index and self.log.term_at(majority_index) == self.current_term:
            self.commit_index = majority_index
            self.on_commit(self.id, self.commit_index)
