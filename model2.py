from __future__ import annotations
import dataclasses
import datetime
import enum
import uuid
from collections import deque
from typing import Optional, Any, Hashable


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
        self._journal: dict[Hashable, UncommittedValue] = {}
        self._transaction_log: dict[int, set[Hashable]] = {}

    def set(self, transaction: int, key: Hashable, value: Any):
        if key in self._journal and self._journal[key].transaction != transaction:
            raise IntegrityError()
        self._transaction_log.setdefault(transaction, set()).add(key)
        self._journal[key] = UncommittedValue(
            transaction=transaction,
            value=value
        )

    def __getitem__(self, key: Hashable) -> UncommittedValue:
        return self._journal[key]

    def __contains__(self, key: Hashable) -> bool:
        return key in self._journal

    def pop_journal(self, transaction: int) -> dict:
        journal = {
            key: self._journal.pop(key)
            for key in self._transaction_log[transaction]
        }
        del self._transaction_log[transaction]
        return journal

    def delete_journal(self, transaction: int) -> None:
        for key in self._transaction_log[transaction]:
            del self._journal[key]
        del self._transaction_log[transaction]


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


class Transaction:
    def __init__(self, journal_repository: JournalRepository):
        self.journal_repository = journal_repository




class ReadUncommittedTransaction(Transaction):
    pass


class ReadCommittedTransaction(Transaction):
    pass


class SerializableTransaction(Transaction):
    pass


class IsolationLevel(enum.Enum):
    READ_UNCOMMITTED = 'read_uncommitted'
    READ_COMMITTED = 'read_committed'
    SERIALIZABLE = 'serializable'


class TransactionManager:
    def __init__(self, transaction_factory: TransactionFactory):
        self.transaction_factory = transaction_factory
        self.transactions: dict[uuid.UUID, Transaction] = {}

    def __getitem__(self, item: uuid.UUID) -> Transaction:
        return self.transactions[item]

    def create_transaction(self, isolation_level: IsolationLevel) -> uuid.UUID:
        transaction_id = uuid.uuid4()
        transaction = self.transaction_factory.create_transaction(isolation_level=isolation_level)
        self.transactions[transaction_id] = transaction
        return transaction_id


class TransactionFactory:
    def __init__(self, journal_repository: JournalRepository):
        self.journal_repository = journal_repository

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        if isolation_level == IsolationLevel.READ_UNCOMMITTED:
            return ReadUncommittedTransaction()
        elif isolation_level == IsolationLevel.READ_COMMITTED:
            return ReadCommittedTransaction()
        elif isolation_level == IsolationLevel.SERIALIZABLE:
            return SerializableTransaction()
        else:
            raise NotImplementedError()

