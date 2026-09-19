"""A controllable, in-memory network for simulating message delivery
between Raft nodes: normal delivery with a configurable delay, permanent
or temporary drops between specific node pairs, and network partitions
that split the cluster into isolated groups.

No sockets, no threads -- messages are handed to a Cluster's event queue
and delivered on a future logical tick, which is what makes the whole
simulation deterministic and replayable.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set


@dataclass
class Message:
    sender: str
    receiver: str
    payload: Any
    deliver_at: int


class Network:
    def __init__(self, default_delay: int = 1):
        self.default_delay = default_delay
        self._partitions: Dict[str, int] = {}  # node_id -> partition group id
        self._dropped_pairs: Set[frozenset] = set()
        self._pair_delay: Dict[frozenset, int] = {}
        self.dropped_count = 0
        self.delivered_count = 0

    # ---- partition control ----

    def partition(self, groups: List[List[str]]) -> None:
        """Split the network into isolated groups. Nodes in different
        groups cannot exchange messages in either direction until healed."""
        self._partitions = {}
        for gid, group in enumerate(groups):
            for node_id in group:
                self._partitions[node_id] = gid

    def heal(self) -> None:
        """Remove all partitions. Explicit drop_between() rules, if any,
        still apply."""
        self._partitions = {}

    def is_partitioned(self) -> bool:
        return len(set(self._partitions.values())) > 1

    # ---- explicit drop / delay control ----

    def drop_between(self, a: str, b: str) -> None:
        self._dropped_pairs.add(frozenset((a, b)))

    def restore_between(self, a: str, b: str) -> None:
        self._dropped_pairs.discard(frozenset((a, b)))

    def set_delay(self, a: str, b: str, delay: int) -> None:
        self._pair_delay[frozenset((a, b))] = delay

    # ---- delivery ----

    def _is_connected(self, a: str, b: str) -> bool:
        if frozenset((a, b)) in self._dropped_pairs:
            return False
        ga = self._partitions.get(a)
        gb = self._partitions.get(b)
        if ga is not None and gb is not None and ga != gb:
            return False
        return True

    def send(self, sender: str, receiver: str, payload: Any, now: int) -> Optional[Message]:
        """Attempt to hand a message to the network.

        Returns the Message (with its delivery tick set) if it will be
        delivered, or None if it was dropped -- callers should queue the
        returned Message and not expect anything if None comes back.
        """
        if sender == receiver:
            return None
        if not self._is_connected(sender, receiver):
            self.dropped_count += 1
            return None
        delay = self._pair_delay.get(frozenset((sender, receiver)), self.default_delay)
        self.delivered_count += 1
        return Message(sender=sender, receiver=receiver, payload=payload, deliver_at=now + delay)
