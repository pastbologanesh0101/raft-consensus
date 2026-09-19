"""raft: a from-scratch, in-process simulation of the Raft consensus algorithm.

Public API:
    NodeState, RaftNode          -- the per-node state machine
    Log, LogEntry                -- the replicated log
    Network                      -- controllable simulated network (drops/delays/partitions)
    Cluster                      -- wires nodes + network together and drives the clock
    RequestVoteRequest/Response, AppendEntriesRequest/Response -- RPC messages
"""

from .log import Log, LogEntry
from .node import RaftNode, NodeState
from .network import Network, Message
from .cluster import Cluster
from .rpc import (
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)

__all__ = [
    "Log",
    "LogEntry",
    "RaftNode",
    "NodeState",
    "Network",
    "Message",
    "Cluster",
    "RequestVoteRequest",
    "RequestVoteResponse",
    "AppendEntriesRequest",
    "AppendEntriesResponse",
]
