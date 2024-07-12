from __future__ import annotations

import abc
import collections
import dataclasses
import enum
from typing import Any, Hashable


class IntegrityError(Exception):
    pass


class Deleted:
    pass


@dataclasses.dataclass
class UncommittedItem:
    value: Any
    transaction: Transaction


class UncommittedJournal(collections.UserDict):
    def __setitem__(self, key: Hashable, item: UncommittedItem):
        assert isinstance(item, UncommittedItem), 'Value must be an instance of UncommittedItem'
        super().__setitem__(key, item)

    def pop_by_transaction(self, transaction: Transaction) -> dict[Hashable, UncommittedItem]:
        taken = {}
        rest = {}
        for key, uncommitted_item in self.data.items():
            if uncommitted_item.transaction is transaction:
                taken[key] = uncommitted_item
            else:
                rest[key] = uncommitted_item
        self.data = rest
        return taken


@dataclasses.dataclass
class CommittedItem:
    offset: int
    payload: dict
    prev: CommittedItem = None

    def __getitem__(self, key):
        return self.payload[key]


class CommittedJournal:
    def __init__(self):
        self._current_offset = 0
        self._item = CommittedItem(
            offset=self._current_offset,
            payload={}
        )

    def commit(self, payload: dict):
        self._current_offset += 1
        self._item = CommittedItem(
            offset=self._current_offset,
            payload=payload,
            prev=self._item
        )

    def get(self, key: Hashable, min_offset: int = 0, max_offset: int = None):
        # TODO: заменить на использование срезов
        max_offset = max_offset if max_offset is not None else self._current_offset
        journal = self._item
        while journal and journal.offset > max_offset:
            journal = journal.prev
        while journal and journal.offset >= min_offset:
            try:
                return journal[key]
            except KeyError:
                journal = journal.prev
        raise KeyError()

    def __iter__(self):
        ...

    def __len__(self):
        ...

    @property
    def current_offset(self):
        return self._current_offset


class Journal:
    def __init__(self):
        self.committed = CommittedJournal()
        self.uncommitted = UncommittedJournal()

    def commit(self, transaction: Transaction):
        uncommitted_items_map = self.uncommitted.pop_by_transaction(transaction=transaction)
        payload = {
            key: uncommitted_item.value
            for key, uncommitted_item in uncommitted_items_map.items()
        }
        self.committed.commit(payload=payload)

    def rollback(self, transaction: Transaction):
        self.uncommitted.pop_by_transaction(transaction=transaction)


class IsolationLevel(enum.Enum):
    READ_UNCOMMITTED = 'read_uncommitted'
    READ_COMMITTED = 'read_committed'
    SERIALIZABLE = 'serializable'


class Transaction(collections.MutableMapping):
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

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.rollback()

    def __getitem__(self, item):
        return self.access_strategy[item]

    def __setitem__(self, key, value):
        self.access_strategy[key] = value

    def __delitem__(self, key):
        del self.access_strategy[key]

    def __len__(self):
        return len(self.access_strategy)

    def __iter__(self):
        return iter(self.access_strategy)

    def commit(self):
        self.access_strategy.commit()

    def rollback(self):
        self.access_strategy.rollback()

    def set_isolation_level(self, isolation_level: IsolationLevel) -> None:
        access_strategy = self.initialize_access_strategy(isolation_level)
        self.access_strategy = access_strategy


class AccessStrategy(abc.ABC, collections.MutableMapping):
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
        uncommitted_item = UncommittedItem(
            value=value,
            transaction=self.transaction
        )
        self.transaction.journal.uncommitted[key] = uncommitted_item

    def is_setitem_allowed(self, key) -> bool:
        return (
                key not in self.transaction.journal.uncommitted
                or self.transaction.journal.uncommitted[key].transaction is self.transaction
        )

    def __delitem__(self, key):
        if key not in self or isinstance(self[key], Deleted):
            raise KeyError()
        self[key] = Deleted()

    def commit(self):
        self.transaction.journal.commit(transaction=self.transaction)

    def rollback(self):
        self.transaction.journal.rollback(transaction=self.transaction)


class ReadUncommittedAccessStrategy(AccessStrategy):
    def execute(self, item):
        try:
            uncommitted_item = self.transaction.journal.uncommitted[item]
            return uncommitted_item.value
        except KeyError:
            committed_value = self.transaction.journal.committed.get(key=item)
            return committed_value

    def __len__(self):
        # TODO: реализовать
        raise NotImplementedError()

    def __iter__(self):
        # TODO: реализовать
        raise NotImplementedError()


class ReadCommittedAccessStrategy(AccessStrategy):
    def execute(self, item):
        try:
            uncommitted_item = self.transaction.journal.uncommitted[item]
        except KeyError:
            committed_value = self.transaction.journal.committed.get(key=item)
            return committed_value
        else:
            if uncommitted_item.transaction is not self.transaction:
                raise KeyError()
            return uncommitted_item.value

    def __len__(self):
        # TODO: реализовать
        raise NotImplementedError()

    def __iter__(self):
        # TODO: реализовать
        raise NotImplementedError()


class SerializableAccessStrategy(AccessStrategy):
    target_offset: int

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.update_target_offset()

    def update_target_offset(self):
        self.target_offset = self.transaction.journal.committed.current_offset

    def execute(self, item):
        try:
            uncommitted_item = self.transaction.journal.uncommitted[item]
        except KeyError:
            committed_value = self.transaction.journal.committed.get(key=item, max_offset=self.target_offset)
            return committed_value
        else:
            if uncommitted_item.transaction is not self.transaction:
                raise KeyError()
            return uncommitted_item.value

    def is_setitem_allowed(self, key) -> bool:
        try:
            self.transaction.journal.committed.get(key=key, min_offset=self.target_offset)
            return False
        except KeyError:
            return super().is_setitem_allowed(key=key)

    def __len__(self):
        # TODO: реализовать
        raise NotImplementedError()

    def __iter__(self):
        # TODO: реализовать
        raise NotImplementedError()

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
        transaction = Transaction(
            journal=self.journal,
            isolation_level=isolation_level
        )
        return transaction


class TransactionDict(collections.MutableMapping):
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

    def transaction(self, isolation_level: IsolationLevel):
        return self._transaction_factory.create_transaction(isolation_level=isolation_level)
