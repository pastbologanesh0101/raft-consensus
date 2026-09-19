"""The replicated log.

Indices are 1-based, matching the Raft paper (index 0 means "no entry" /
"before the log begins"). This keeps prev_log_index == 0 as a clean sentinel
for "the log has no predecessor entry yet".
"""

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: Any


class Log:
    def __init__(self):
        self._entries: List[LogEntry] = []

    def __len__(self) -> int:
        return len(self._entries)

    def last_index(self) -> int:
        return len(self._entries)

    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def get(self, index: int) -> Optional[LogEntry]:
        """1-indexed lookup. Returns None if index is out of range."""
        if index <= 0 or index > len(self._entries):
            return None
        return self._entries[index - 1]

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        entry = self.get(index)
        return entry.term if entry else 0

    def append(self, entry: LogEntry) -> None:
        self._entries.append(entry)

    def append_entries(self, entries: List[LogEntry]) -> None:
        self._entries.extend(entries)

    def truncate_after(self, index: int) -> None:
        """Keep entries [1..index], drop everything after (index may be 0)."""
        self._entries = self._entries[:index]

    def entries_from(self, start_index: int) -> List[LogEntry]:
        """All entries with 1-based index >= start_index."""
        if start_index <= 1:
            return list(self._entries)
        return list(self._entries[start_index - 1:])

    def as_list(self) -> List[LogEntry]:
        return list(self._entries)
