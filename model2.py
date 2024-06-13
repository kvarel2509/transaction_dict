from __future__ import annotations
import dataclasses
import datetime
import uuid
from collections import deque
from typing import Optional, Any, Hashable

from model import Transaction


class IntegrityError(Exception):
    pass


class Deleted:
    pass


@dataclasses.dataclass
class UncommittedValue:
    value: Any
    transaction: int


class UncommittedJournalRepository:
    def __init__(self):
        self.journal: dict[Hashable, UncommittedValue] = {}

    def set(self, transaction: int, key: Hashable, value: Any):
        self.journal[key] = UncommittedValue(
            transaction=transaction,
            value=value
        )

    def __getitem__(self, key: Hashable) -> UncommittedValue:
        return self.journal[key]

    def __contains__(self, key: Hashable) -> bool:
        return key in self.journal

    def pop_journal(self, transaction: int) -> dict:
        journal = {
            key: value
            for key, value in self.journal.items()
            if value.transaction == transaction
        }
        self.delete_journal(transaction=transaction)
        return journal

    def delete_journal(self, transaction: int) -> None:
        self.journal = {
            key: value
            for key, value in self.journal.items()
            if value.transaction != transaction
        }


class LockManager:
    def __init__(self, repo: UncommittedJournalRepository):
        self.repo = repo
        self.locks = {}

    def set(self, transaction: int, key: Hashable, value: Any):
        if key not in self.locks:
            self.locks[key] = deque([transaction])
            self.repo.set(transaction=transaction, key=key, value=value)
        elif key in self.repo and self.repo[key].transaction == transaction:
            self.repo.set(transaction=transaction, key=key, value=value)
        else:
            while


@dataclasses.dataclass
class CommittedJournal:
    offset: int
    data: dict
    prev: CommittedJournal = None

    def __getitem__(self, key):
        return self.data[key]


class CommittedJournalRepository:
    def __init__(self):
        self._current_offset = 0
        self._journal = CommittedJournal(
            offset=self._current_offset,
            data={}
        )

    def commit(self, data: dict):
        self._current_offset += 1
        self._journal = CommittedJournal(
            offset=self._current_offset,
            data=data,
            prev=self._journal
        )

    def getitem(self, key, offset: int = None):
        offset = offset or self._current_offset
        journal = self._journal
        while journal and journal.offset > offset:
            journal = journal.prev
        while journal:
            try:
                return journal[key]
            except KeyError:
                journal = journal.prev
        raise KeyError()

    @property
    def current_offset(self):
        return self._current_offset


class JournalRepository:
    def __init__(self):
        self.committed = CommittedJournalRepository()
        self.uncommitted = UncommittedJournalRepository()

    def set(self, transaction: int, key: Hashable, value: Any):
        self.uncommitted.set(transaction=transaction, key=key, value=value)

    def commit(self, transaction: int):
        data = self.uncommitted.pop_journal(transaction=transaction)
        self.committed.commit(data=data)

    def rollback(self, transaction: int):
        self.uncommitted.delete_journal(transaction=transaction)


