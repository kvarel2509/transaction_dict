from __future__ import annotations

import abc
import dataclasses
import enum
from typing import Any, Hashable


class IntegrityError(Exception):
    pass


class Deleted:
    pass


@dataclasses.dataclass
class UncommittedValue:
    value: Any
    transaction: Transaction


class UncommittedJournalRepository:
    def __init__(self):
        self._journal: dict[Hashable, UncommittedValue] = {}

    def get(self, key: Hashable) -> UncommittedValue:
        return self._journal[key]

    def set(self, key: Hashable, value: UncommittedValue):
        self._journal[key] = value

    def __contains__(self, key: Hashable) -> bool:
        return key in self._journal


@dataclasses.dataclass
class CommittedJournal:
    offset: int
    payload: dict
    prev: CommittedJournal = None

    def __getitem__(self, key):
        return self.payload[key]


class CommittedJournalRepository:
    def __init__(self):
        self._current_offset = 0
        self._journal = CommittedJournal(
            offset=self._current_offset,
            payload={}
        )

    def commit(self, data: dict):
        self._current_offset += 1
        self._journal = CommittedJournal(
            offset=self._current_offset,
            payload=data,
            prev=self._journal
        )

    def get(self, key, min_offset: int = 0, max_offset: int = None):
        max_offset = max_offset or self._current_offset
        journal = self._journal
        while journal and journal.offset > max_offset:
            journal = journal.prev
        while journal and journal.offset >= min_offset:
            try:
                return journal[key]
            except KeyError:
                journal = journal.prev
        raise KeyError()

    @property
    def current_offset(self):
        return self._current_offset


class Journal:
    def __init__(self):
        self.committed = CommittedJournalRepository()
        self.uncommitted = UncommittedJournalRepository()

    def commit(self, transaction: Transaction):
        ...

    def rollback(self, transaction: Transaction):
        ...


class IsolationLevel(enum.Enum):
    READ_UNCOMMITTED = 'read_uncommitted'
    READ_COMMITTED = 'read_committed'
    SERIALIZABLE = 'serializable'


class Transaction:
    def __init__(self, journal: Journal, isolation_level: IsolationLevel):
        self.journal = journal
        self.access_strategy: AccessStrategy = self.initialize_access_strategy(isolation_level=isolation_level)

    def initialize_access_strategy(self, isolation_level: IsolationLevel) -> AccessStrategy:
        if isolation_level == IsolationLevel.READ_UNCOMMITTED:
            return ReadUncommittedAccessStrategy(transaction=self)
        elif isolation_level == IsolationLevel.READ_COMMITTED:
            return ReadCommittedAccessStrategy(transaction=self)
        elif isolation_level == IsolationLevel.SERIALIZABLE:
            return SerializableAccessStrategy(transaction=self)

    def __getitem__(self, item):
        return self.access_strategy[item]

    def __setitem__(self, key, value):
        self.access_strategy[key] = value

    def __delitem__(self, key):
        del self.access_strategy[key]

    def commit(self):
        ...

    def rollback(self):
        ...

    def set_isolation_level(self, isolation_level: IsolationLevel) -> None:
        access_strategy = self.initialize_access_strategy(isolation_level)
        self.access_strategy = access_strategy


class AccessStrategy(abc.ABC):
    def __init__(self, transaction: Transaction):
        self.transaction = transaction

    def __getitem__(self, item):
        value = self.execute(item)
        if isinstance(value, Deleted):
            raise KeyError()
        return value

    @abc.abstractmethod
    def execute(self, item) -> Any:
        ...

    def __setitem__(self, key, value):
        if not self.is_setitem_allowed(key=key):
            raise IntegrityError()
        uncommitted_value = UncommittedValue(
            value=value,
            transaction=self.transaction
        )
        self.transaction.journal.uncommitted.set(
            key=key,
            value=uncommitted_value
        )

    def is_setitem_allowed(self, key) -> bool:
        return (
                key in self.transaction.journal.uncommitted
                and self.transaction.journal.uncommitted.get(key=key).transaction != self.transaction
        )

    def __delitem__(self, key):
        ...


class ReadUncommittedAccessStrategy(AccessStrategy):
    def execute(self, item):
        try:
            uncommitted_value = self.transaction.journal.uncommitted.get(key=item)
            return uncommitted_value.value
        except KeyError:
            committed_value = self.transaction.journal.committed.get(key=item)
            return committed_value


class ReadCommittedAccessStrategy(AccessStrategy):
    def execute(self, item):
        committed_value = self.transaction.journal.committed.get(key=item)
        return committed_value


class SerializableAccessStrategy(AccessStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_offset = self.transaction.journal.committed.current_offset

    def execute(self, item):
        committed_value = self.transaction.journal.committed.get(key=item, max_offset=self.target_offset)
        return committed_value

    def is_setitem_allowed(self, key) -> bool:
        try:
            self.transaction.journal.committed.get(key=key, min_offset=self.target_offset)
            return False
        except KeyError:
            return super().is_setitem_allowed(key=key)


class TransactionFactory:
    def __init__(self, journal: Journal):
        self.journal = journal

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        transaction = Transaction(
            journal=self.journal,
            isolation_level=isolation_level
        )
        return transaction
