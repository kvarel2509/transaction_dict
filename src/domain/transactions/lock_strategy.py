import abc
from typing import Hashable

from src.exceptions import AccessError
from src.domain.journals import CompositeJournal
from src.domain.core import JournalRepository, Transaction, BaseTransaction


class AnyKey:
    pass


class AccessProtector(abc.ABC):
    @abc.abstractmethod
    def add_key_lock(self, transaction: Transaction, key: Hashable) -> None:
        ...

    @abc.abstractmethod
    def add_full_lock(self, transaction: Transaction) -> None:
        ...

    @abc.abstractmethod
    def clear_locks_by_transaction(self, transaction: Transaction) -> None:
        ...


class AccessProtectorDecorator(AccessProtector):
    def __init__(self, access_protector: AccessProtector) -> None:
        self.access_protector = access_protector

    def add_key_lock(self, transaction: Transaction, key: Hashable) -> None:
        self.access_protector.add_key_lock(transaction=transaction, key=key)

    def add_full_lock(self, transaction: Transaction):
        self.access_protector.add_full_lock(transaction=transaction)

    def clear_locks_by_transaction(self, transaction: Transaction):
        self.access_protector.clear_locks_by_transaction(transaction=transaction)


class BaseAccessProtector(AccessProtector):
    def __init__(self):
        self.locks: dict[Hashable, Transaction] = {}
        self.any_key = AnyKey()

    def add_key_lock(self, transaction: Transaction, key: Hashable) -> None:
        if (
                key in self.locks and self.locks[key] != transaction
                or self.any_key in self.locks and self.locks[self.any_key] != transaction
        ):
            raise AccessError()
        self.locks[key] = transaction

    def add_full_lock(self, transaction: Transaction) -> None:
        if set(self.locks.values()) - {transaction}:
            raise AccessError()
        self.locks[self.any_key] = transaction

    def clear_locks_by_transaction(self, transaction: Transaction) -> None:
        self.locks = {
            key: locker
            for key, locker in self.locks.items()
            if locker is not transaction
        }


class LockStrategyTransaction(BaseTransaction, abc.ABC):
    def __init__(self, journal_repository: JournalRepository, access_protector: AccessProtector):
        self.journal_repository = journal_repository
        self.access_protector = access_protector

    def __setitem__(self, key, value):
        self.access_protector.add_key_lock(transaction=self, key=key)
        super().__setitem__(key, value)

    def __delitem__(self, key):
        self.access_protector.add_key_lock(transaction=self, key=key)
        super().__delitem__(key)

    def commit(self):
        super().commit()
        self.access_protector.clear_locks_by_transaction(transaction=self)

    def rollback(self):
        super().rollback()
        self.access_protector.clear_locks_by_transaction(transaction=self)

    def end(self):
        self.rollback()
        self.journal_repository.delete_uncommitted_journal(transaction=self)


class ReadUncommittedLockStrategyTransaction(LockStrategyTransaction):
    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_aggregated_uncommitted_journal(),
                self.journal_repository.get_committed_journal()
            )
        )


class ReadCommittedLockStrategyTransaction(LockStrategyTransaction):
    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
                self.journal_repository.get_committed_journal()
            )
        )


class RepeatableReadLockStrategyTransaction(LockStrategyTransaction):
    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
                self.journal_repository.get_committed_journal()
            )
        )

    def __getitem__(self, item):
        self.access_protector.add_key_lock(transaction=self, key=item)
        return super().__getitem__(item)

    def __contains__(self, item):
        self.access_protector.add_key_lock(transaction=item, key=item)
        return super().__contains__(item)

    def __iter__(self):
        keys = []
        for key in super().__iter__():
            self.access_protector.add_key_lock(transaction=self, key=key)
            keys.append(key)
        return iter(keys)


class SerializableLockStrategyTransaction(LockStrategyTransaction):
    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
                self.journal_repository.get_committed_journal()
            )
        )

    def __getitem__(self, item):
        self.access_protector.add_key_lock(transaction=self, key=item)
        return super().__getitem__(item)

    def __contains__(self, item):
        self.access_protector.add_key_lock(transaction=item, key=item)
        return super().__contains__(item)

    def __iter__(self):
        self.access_protector.add_full_lock(transaction=self)
        return super().__iter__()

    def __len__(self):
        self.access_protector.add_full_lock(transaction=self)
        return super().__len__()
