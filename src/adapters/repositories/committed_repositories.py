from __future__ import annotations

import bisect

from src.domain.journals import CompositeJournal
from src.domain.core import Journal, CommittedItem, Counter, CommittedRepository


class InMemoryCommittedRepository(CommittedRepository):
    def __init__(self):
        self.offset_counter = Counter()
        self.items: list[CommittedItem] = []

    def get_journal(
            self,
            start_offset: int = 0,
            end_offset: int = None
    ) -> Journal:
        start_index = (
            0 if start_offset == 0
            else bisect.bisect_left(self.items, start_offset, key=lambda x: x.offset)
        )
        stop_index = (
            None if end_offset is None
            else bisect.bisect_right(self.items, end_offset, key=lambda x: x.offset)
        )
        return CompositeJournal(
            journals=reversed([i.payload for i in self.items[start_index:stop_index]])
        )

    def add_committed_item(self, item: CommittedItem) -> None:
        self.items.append(item)
