"""The four Raft RPC message types (RequestVote and AppendEntries,
request + response for each), used by both leader election and log
replication. Plain dataclasses -- they are handed around in-process by the
simulated Network, never serialized.
"""

from dataclasses import dataclass, field
from typing import List

from .log import LogEntry


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    follower_id: str
    match_index: int = 0
    conflict_index: int = 0
