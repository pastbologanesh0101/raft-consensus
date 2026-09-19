"""Cluster wires a set of RaftNodes to a simulated Network and drives the
logical clock. It owns the message queue (messages sent by nodes are
routed through the Network for drop/delay/partition decisions, then
delivered to their target's RPC handler on the tick they're due) and
exposes convenience helpers (run_until_leader, propose, kill/revive, ...)
used by the demo and the test suite.
"""

import heapq
import itertools
import random
from typing import Callable, Dict, List, Optional

from .network import Message, Network
from .node import NodeState, RaftNode
from .rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)


class Cluster:
    def __init__(
        self,
        node_ids: List[str],
        network: Optional[Network] = None,
        rng_factory: Optional[Callable[[str], random.Random]] = None,
        election_timeout_range=(10, 20),
        heartbeat_interval: int = 3,
        on_event: Optional[Callable] = None,
    ):
        self.node_ids = list(node_ids)
        self.network = network or Network()
        self.clock = 0
        self._queue: List = []
        self._seq_counter = itertools.count()
        self.on_event = on_event or (lambda *a, **k: None)
        self.events: List[tuple] = []

        self.nodes: Dict[str, RaftNode] = {}
        for nid in self.node_ids:
            peers = [p for p in self.node_ids if p != nid]
            rng = rng_factory(nid) if rng_factory else None
            self.nodes[nid] = RaftNode(
                node_id=nid,
                peer_ids=peers,
                send_fn=self._make_send_fn(nid),
                rng=rng,
                election_timeout_range=election_timeout_range,
                heartbeat_interval=heartbeat_interval,
                on_state_change=self._log_state_change,
                on_commit=self._log_commit,
            )

    def _make_send_fn(self, sender: str) -> Callable[[str, object], None]:
        def send(receiver: str, payload: object) -> None:
            self._enqueue(sender, receiver, payload)
        return send

    def _log_state_change(self, node_id, term, state) -> None:
        self.events.append(("state", node_id, term, state.value))
        self.on_event("state", node_id, term, state.value)

    def _log_commit(self, node_id, commit_index) -> None:
        self.events.append(("commit", node_id, commit_index))
        self.on_event("commit", node_id, commit_index)

    def _enqueue(self, sender: str, receiver: str, payload: object) -> None:
        if receiver not in self.nodes or sender not in self.nodes:
            return
        if not self.nodes[receiver].alive or not self.nodes[sender].alive:
            return
        msg = self.network.send(sender, receiver, payload, self.clock)
        if msg is not None:
            heapq.heappush(self._queue, (msg.deliver_at, next(self._seq_counter), msg))

    def tick(self) -> None:
        self.clock += 1
        due: List[Message] = []
        while self._queue and self._queue[0][0] <= self.clock:
            _, _, msg = heapq.heappop(self._queue)
            due.append(msg)
        for msg in due:
            self._deliver(msg)
        for node in self.nodes.values():
            node.tick()

    def _deliver(self, msg: Message) -> None:
        receiver = self.nodes.get(msg.receiver)
        if receiver is None or not receiver.alive:
            return
        payload = msg.payload
        if isinstance(payload, RequestVoteRequest):
            resp = receiver.handle_request_vote_request(payload)
            self._enqueue(msg.receiver, msg.sender, resp)
        elif isinstance(payload, RequestVoteResponse):
            receiver.handle_request_vote_response(payload)
        elif isinstance(payload, AppendEntriesRequest):
            resp = receiver.handle_append_entries_request(payload)
            self._enqueue(msg.receiver, msg.sender, resp)
        elif isinstance(payload, AppendEntriesResponse):
            receiver.handle_append_entries_response(payload)

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.tick()

    # ---------- convenience helpers ----------

    def leaders(self, term: Optional[int] = None) -> List[RaftNode]:
        result = [n for n in self.nodes.values() if n.alive and n.state == NodeState.LEADER]
        if term is not None:
            result = [n for n in result if n.current_term == term]
        return result

    def leader(self) -> Optional[RaftNode]:
        ls = self.leaders()
        return ls[0] if ls else None

    def kill(self, node_id: str) -> None:
        self.nodes[node_id].alive = False

    def revive(self, node_id: str) -> None:
        self.nodes[node_id].alive = True

    def propose(self, command) -> bool:
        leader = self.leader()
        if leader is None:
            return False
        return leader.propose(command)

    def run_until_leader(self, max_ticks: int = 300) -> Optional[RaftNode]:
        for _ in range(max_ticks):
            self.tick()
            leader = self.leader()
            if leader is not None:
                return leader
        return None

    def run_until_committed(self, index: int, max_ticks: int = 300) -> bool:
        for _ in range(max_ticks):
            self.tick()
            leader = self.leader()
            if leader and leader.commit_index >= index:
                return True
        return False

    def alive_nodes(self) -> List[RaftNode]:
        return [n for n in self.nodes.values() if n.alive]
