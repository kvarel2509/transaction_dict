from __future__ import annotations

import abc
import collections
import dataclasses
import enum
from collections.abc import MutableMapping, Mapping, Iterable, Hashable
from typing import Any


class Void:
    pass


class Journal(Mapping, abc.ABC):
    pass


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


class UncommittedRepository(abc.ABC):
    @abc.abstractmethod
    def create_journal(self, transaction: Transaction) -> None:
        ...

    @abc.abstractmethod
    def get_journal(self, transaction: Transaction = None) -> Journal:
        ...

    @abc.abstractmethod
    def add_value_to_journal(self, transaction: Transaction, key: Hashable, value: Any) -> None:
        ...

    @abc.abstractmethod
    def delete_journal(self, transaction: Transaction) -> None:
        ...

    def recreate_journal(self, transaction: Transaction) -> None:
        self.delete_journal(transaction=transaction)
        self.create_journal(transaction=transaction)


@dataclasses.dataclass
class CommittedItem:
    offset: int
    payload: Journal


class Counter:
    def __init__(self, start: int = 0, step: int = 1) -> None:
        self._current = start
        self._step = step

    def shift(self):
        self._current += self._step

    @property
    def current(self):
        return self._current


class CommittedRepository(abc.ABC):
    offset_counter: Counter

    def __getitem__(self, item):
        match item:
            case slice(item):
                assert item.step is None, 'The step is not supported'
                return self.get_journal(
                    start_offset=item.start or 0,
                    end_offset=item.stop or None,
                )
            case _:
                journal = self.get_journal()
                return journal[item]

    @abc.abstractmethod
    def get_journal(self, start_offset: int = 0, end_offset: int = None) -> Journal:
        ...

    def commit_journal(self, journal: Journal) -> None:
        self.offset_counter.shift()
        item = CommittedItem(
            offset=self.offset_counter.current,
            payload=journal
        )
        self.add_committed_item(item=item)

    @abc.abstractmethod
    def add_committed_item(self, item: CommittedItem) -> None:
        ...

    @property
    def last_offset(self) -> int:
        return self.offset_counter.current


class JournalRepository:
    def __init__(
            self,
            committed_repository: CommittedRepository,
            uncommitted_repository: UncommittedRepository
    ) -> None:
        self._committed_repository = committed_repository
        self._uncommitted_repository = uncommitted_repository

    def create_uncommitted_journal(self, transaction: Transaction) -> None:
        self._uncommitted_repository.create_journal(transaction=transaction)

    def delete_uncommitted_journal(self, transaction: Transaction) -> None:
        self._uncommitted_repository.delete_journal(transaction=transaction)

    def add_value_to_uncommitted_journal(self, transaction: Transaction, key: Hashable, value: Any) -> None:
        self._uncommitted_repository.add_value_to_journal(transaction=transaction, key=key, value=value)

    def get_uncommitted_journal_by_transaction(self, transaction: Transaction) -> Journal:
        return self._uncommitted_repository.get_journal(transaction=transaction)

    def get_aggregated_uncommitted_journal(self) -> Journal:
        return self._uncommitted_repository.get_journal()

    def get_committed_journal(self, start_offset: int = 0, end_offset: int = None) -> Journal:
        return self._committed_repository.get_journal(start_offset=start_offset, end_offset=end_offset)

    def commit(self, transaction: Transaction):
        journal = self._uncommitted_repository.get_journal(transaction=transaction)
        self._committed_repository.commit_journal(journal=journal)
        self._uncommitted_repository.recreate_journal(transaction=transaction)

    def rollback(self, transaction: Transaction):
        self._uncommitted_repository.recreate_journal(transaction=transaction)

    @property
    def last_offset(self) -> int:
        return self._committed_repository.last_offset


class IsolationLevel(enum.Enum):
    READ_UNCOMMITTED = 'read_uncommitted'
    READ_COMMITTED = 'read_committed'
    REPEATABLE_READ = 'repeatable_read'
    SERIALIZABLE = 'serializable'


class Transaction(abc.ABC, MutableMapping):
    journal_repository: JournalRepository

    def __getitem__(self, item):
        value = self.state[item]
        if isinstance(value, Void):
            raise KeyError()
        return value

    @property
    @abc.abstractmethod
    def state(self) -> Journal:
        ...

    def __setitem__(self, key, value):
        self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=key, value=value)

    def __delitem__(self, key):
        if isinstance(self.state.get(key, Void()), Void):
            raise KeyError()
        self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=key, value=Void())

    def __contains__(self, item):
        value = self.state.get(item, Void())
        return not isinstance(value, Void)

    def __iter__(self):
        return (
            key
            for key, value in self.state.items()
            if not isinstance(value, Void)
        )

    def __len__(self):
        return sum(
            not isinstance(value, Void)
            for value in self.state.values()
        )

    def commit(self):
        self.journal_repository.commit(transaction=self)

    def rollback(self):
        self.journal_repository.rollback(transaction=self)

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()

    def start(self):
        self.journal_repository.create_uncommitted_journal(transaction=self)

    def end(self):
        self.journal_repository.delete_uncommitted_journal(transaction=self)


class TransactionFactory(abc.ABC):
    @abc.abstractmethod
    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        ...


class TransactionDict(MutableMapping):
    def __init__(self, transaction_factory: TransactionFactory):
        self.transaction_factory = transaction_factory

    def __getitem__(self, item):
        with self.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return transaction[item]

    def __setitem__(self, key, value):
        with self.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            transaction[key] = value
            transaction.commit()

    def __delitem__(self, key):
        with self.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            del transaction[key]
            transaction.commit()

    def __contains__(self, item):
        with self.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return item in transaction

    def __iter__(self):
        with self.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return iter(transaction)

    def __len__(self):
        with self.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return len(transaction)

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        return self.transaction_factory.create_transaction(isolation_level=isolation_level)
