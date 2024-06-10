from __future__ import annotations
import dataclasses
import datetime
import uuid
from collections import deque
from typing import Optional


class Deleted:
    pass


class ActiveJournal:
    def __init__(self, transaction):
        self.transaction = transaction
        self.data = {}

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __contains__(self, item):
        return item in self.data


@dataclasses.dataclass
class CommitedJournal:
    offset: int
    data: dict
    prev: CommitedJournal = None

    def __getitem__(self, item):
        return self.data[item]


class CommitedJournalRepository:
    def __init__(self):
        self.offset_counter = 0
        self.journal = CommitedJournal(
            offset=self.offset_counter,
            data={}
        )

    def commit_active_journal(self, journal: ActiveJournal):
        self.offset_counter += 1
        self.journal = CommitedJournal(
            offset=self.offset_counter,
            data=journal.data,
            prev=self.journal
        )

    def getitem(self, item, offset: int = None):
        offset = offset or self.offset_counter
        journal = self.journal
        while journal and journal.offset > offset:
            journal = journal.prev
        while journal:
            try:
                return journal[item]
            except KeyError:
                journal = journal.prev
        raise KeyError()


class JournalRepository:
    # Хранилище для журналов транзакций.
    def __init__(self):
        self._commited = deque()
        self._active = {}


class LockManager:
    # Декоратор над репозиторием. Отслеживает блокировки
    def __init__(self):
        ...


