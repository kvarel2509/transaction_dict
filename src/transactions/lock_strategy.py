import abc
from typing import Hashable

from src.exceptions import AccessError
from src.model import Transaction, JournalRepository, CompositeJournal


class AnyKey:
    pass


class AccessProtector:
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

    def del_key_lock(self, key: Hashable) -> None:
        del self.locks[key]

    def del_full_lock(self) -> None:
        del self.locks[self.any_key]

    def clear_locks_by_transaction(self, transaction: Transaction) -> None:
        self.locks = {
            key: locker
            for key, locker in self.locks.items()
            if locker is not transaction
        }


class LockStrategyTransaction(Transaction, abc.ABC):
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

    def __iter__(self):
        self.access_protector.add_full_lock(transaction=self)
        return super().__iter__()

    def __len__(self):
        self.access_protector.add_full_lock(transaction=self)
        return super().__len__()
