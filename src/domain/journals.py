from __future__ import annotations

import collections
from typing import Iterable

from src.domain.core import Journal


class LeafJournal(Journal):
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    def __getitem__(self, item):
        return self.journal[item]

    def __len__(self):
        return len(self.journal)

    def __iter__(self):
        return iter(self.journal)


class CompositeJournal(Journal):
    def __init__(self, journals: Iterable[Journal]) -> None:
        self.journal = collections.ChainMap(*journals)

    def __getitem__(self, item):
        return self.journal[item]

    def __len__(self):
        return len(self.journal)

    def __iter__(self):
        return iter(self.journal)


class MutableJournal(Journal):
    def __init__(self):
        self.journal = {}

    def __getitem__(self, item):
        return self.journal[item]

    def __len__(self):
        return len(self.journal)

    def __iter__(self):
        return iter(self.journal)

    def __setitem__(self, key, value):
        self.journal[key] = value

    def __delitem__(self, key):
        del self.journal[key]

    def clear(self):
        self.journal = {}
