from __future__ import annotations

import abc
import collections
import dataclasses
import enum
import bisect
import itertools


class IntegrityError(Exception):
    pass


class Deleted:
    pass


class TransactionJournal(collections.UserDict):
    pass


class UncommittedPool(collections.UserDict):
    data: dict[Transaction, TransactionJournal]

    def register_transaction(self, transaction: Transaction) -> None:
        if transaction in self.data:
            raise IntegrityError("Transaction journal already exists")
        self.data[transaction] = TransactionJournal()

    def delete_transaction(self, transaction: Transaction) -> None:
        del self.data[transaction]

    def execute_transaction_journal(self, transaction: Transaction) -> TransactionJournal:
        journal = self.data[transaction]
        self.data[transaction] = TransactionJournal()
        return journal


@dataclasses.dataclass
class CommittedItem:
    offset: int
    payload: TransactionJournal


class Counter:
    def __init__(self, start: int = 0, step: int = 1) -> None:
        self._current = start
        self._step = step

    def shift(self):
        self._current += self._step

    @property
    def current(self):
        return self._current


class CommittedPool:
    items: list[CommittedItem]

    def __init__(self):
        self._offset_counter = Counter(start=-1)
        self.items = []
        self.commit(journal=TransactionJournal())

    def commit(self, journal: TransactionJournal):
        self._offset_counter.shift()
        self.items.append(
            CommittedItem(
                offset=self.last_offset,
                payload=journal
            )
        )

    @property
    def last_offset(self):
        return self._offset_counter.current


class Journal:
    def __init__(self):
        self._committed_pool = CommittedPool()
        self._uncommitted_pool = UncommittedPool()

    def commit(self, transaction: Transaction):
        transaction_journal = self.uncommitted_pool.execute_transaction_journal(transaction=transaction)
        self.committed_pool.commit(journal=transaction_journal)

    def rollback(self, transaction: Transaction):
        self.uncommitted_pool.execute_transaction_journal(transaction=transaction)

    @property
    def committed_pool(self):
        return self._committed_pool

    @property
    def uncommitted_pool(self):
        return self._uncommitted_pool


class IsolationLevel(enum.Enum):
    READ_UNCOMMITTED = 'read_uncommitted'
    READ_COMMITTED = 'read_committed'
    SERIALIZABLE = 'serializable'


class Transaction(abc.ABC, collections.abc.MutableMapping):
    def __init__(self, journal: Journal):
        self.journal = journal

    def __getitem__(self, item):
        value = self.state[item]
        if isinstance(value, Deleted):
            raise KeyError()
        return value

    @property
    @abc.abstractmethod
    def state(self) -> collections.Mapping:
        ...

    def __setitem__(self, key, value):
        if not self.is_setitem_allowed(key=key):
            raise IntegrityError()
        self.journal.uncommitted_pool[self][key] = value

    def is_setitem_allowed(self, key) -> bool:
        return (
                key in self.journal.uncommitted_pool[self]
                or key not in itertools.chain(*self.journal.uncommitted_pool.values())
        )

    def __delitem__(self, key):
        if key in self and self.is_setitem_allowed(key=key):
            self[key] = Deleted()
        else:
            raise KeyError()

    def __contains__(self, item):
        try:
            _ = self[item]
            return True
        except KeyError:
            return False

    def __iter__(self):
        return iter(self.state)
    
    def __len__(self):
        return len(self.state)

    def commit(self):
        self.journal.commit(transaction=self)

    def rollback(self):
        self.journal.rollback(transaction=self)

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
        self.journal.uncommitted_pool.register_transaction(transaction=self)

    def end(self):
        self.journal.uncommitted_pool.delete_transaction(transaction=self)


class ReadUncommittedTransaction(Transaction):
    @property
    def state(self) -> collections.ChainMap:
        return collections.ChainMap(
            *self.journal.uncommitted_pool.values(),
            *(i.payload for i in reversed(self.journal.committed_pool.items)),
        )


class ReadCommittedTransaction(Transaction):
    @property
    def state(self) -> collections.ChainMap:
        return collections.ChainMap(
            self.journal.uncommitted_pool[self],
            *(i.payload for i in reversed(self.journal.committed_pool.items)),
        )


class SerializableTransaction(Transaction):
    target_offset: int

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.update_target_offset()

    def update_target_offset(self):
        self.target_offset = self.journal.committed_pool.last_offset

    @property
    def state(self) -> collections.ChainMap:
        boarder = bisect.bisect_right(
            self.journal.committed_pool.items,
            self.target_offset,
            key=lambda x: x.offset
        )
        return collections.ChainMap(
            self.journal.uncommitted_pool[self],
            collections.ChainMap(*(i.payload for i in self.journal.committed_pool.items[boarder-1::-1]))
        )

    def is_setitem_allowed(self, key) -> bool:
        boarder = bisect.bisect_left(
            self.journal.committed_pool.items,
            self.target_offset,
            key=lambda x: x.offset
        )
        try:
            _ = collections.ChainMap(*(i.payload for i in self.journal.committed_pool.items[boarder:]))[key]
            return False
        except KeyError:
            return super().is_setitem_allowed(key=key)

    def commit(self):
        super().commit()
        self.update_target_offset()

    def rollback(self):
        super().rollback()
        self.update_target_offset()


class TransactionFactory:
    def __init__(self, journal: Journal):
        self.journal = journal

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        match isolation_level:
            case IsolationLevel.READ_UNCOMMITTED:
                return ReadUncommittedTransaction(self.journal)
            case IsolationLevel.READ_COMMITTED:
                return ReadCommittedTransaction(self.journal)
            case IsolationLevel.SERIALIZABLE:
                return SerializableTransaction(self.journal)
            case _:
                raise NotImplementedError()


class TransactionDict(collections.abc.MutableMapping):
    def __init__(self):
        self._transaction_factory = TransactionFactory(
            journal=Journal()
        )

    def __getitem__(self, item):
        with self.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return transaction[item]

    def __setitem__(self, key, value):
        with self.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            transaction[key] = value
            transaction.commit()

    def __delitem__(self, key):
        with self.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            del transaction[key]
            transaction.commit()

    def __contains__(self, item):
        with self.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return item in transaction

    def __iter__(self):
        with self.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return iter(transaction)

    def __len__(self):
        with self.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            return len(transaction)

    def transaction(self, isolation_level: IsolationLevel) -> Transaction:
        return self._transaction_factory.create_transaction(isolation_level=isolation_level)
