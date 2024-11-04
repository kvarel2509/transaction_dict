import abc
from collections.abc import MutableMapping

from src.domain.transactions.lock_strategy import AccessProtector, AccessProtectorDecorator
from src.exceptions import SessionError
from src.domain.core import IsolationLevel, BaseTransaction, TransactionFactory, Transaction, TransactionDecorator


class AutoCommitFlagTransactionDecorator(TransactionDecorator):
    def __init__(self, is_autocommit: bool, *args, **kwargs):
        self.is_autocommit = is_autocommit
        super().__init__(*args, **kwargs)


class ResetAlertAccessProtectorDecorator(AccessProtectorDecorator):
    def __init__(self, transaction_supervisor, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transaction_supervisor = transaction_supervisor

    def clear_locks_by_transaction(self, transaction: Transaction) -> None:
        super().clear_locks_by_transaction(transaction)
        self.transaction_supervisor.run_deferred_commands_by_transaction(transaction=transaction)


class TransactionSupervisor:
    pass


class Command(abc.ABC):
    session: Session
              
    @abc.abstractmethod
    def execute

class Session(MutableMapping):
    def __init__(self, transaction_factory: TransactionFactory):
        self.transaction_factory = transaction_factory
        self.transaction = None

    def __getitem__(self, item):
        if not self.is_transaction_opened():
            with self.transaction_factory.create_transaction(
                    isolation_level=IsolationLevel.READ_COMMITTED
            ) as transaction:
                return transaction[item]
        else:
            return self.transaction[item]

    def __setitem__(self, key, value):
        if not self.is_transaction_opened():
            with self.transaction_factory.create_transaction(
                    isolation_level=IsolationLevel.READ_COMMITTED
            ) as transaction:
                transaction[key] = value
        else:
            self.transaction[key] = value

    def __delitem__(self, key):
        if not self.is_transaction_opened():
            with self.transaction_factory.create_transaction(
                    isolation_level=IsolationLevel.READ_COMMITTED
            ) as transaction:
                del transaction[key]
        else:
            del self.transaction[key]

    def __contains__(self, item):
        if not self.is_transaction_opened():
            with self.transaction_factory.create_transaction(
                    isolation_level=IsolationLevel.READ_COMMITTED
            ) as transaction:
                return item in transaction
        else:
            return item in self.transaction

    def __iter__(self):
        if not self.is_transaction_opened():
            with self.transaction_factory.create_transaction(
                    isolation_level=IsolationLevel.READ_COMMITTED
            ) as transaction:
                return iter(transaction)
        else:
            return iter(self.transaction)

    def __len__(self):
        if not self.is_transaction_opened():
            with self.transaction_factory.create_transaction(
                    isolation_level=IsolationLevel.READ_COMMITTED
            ) as transaction:
                return len(transaction)
        else:
            return len(self.transaction)

    def commit(self):
        self.transaction.commit()

    def rollback(self):
        self.transaction.rollback()

    def open_transaction(self, isolation_level: IsolationLevel) -> None:
        if self.is_transaction_opened():
            raise SessionError()
        self.transaction = self.transaction_factory.create_transaction(isolation_level=isolation_level)

    def close_transaction(self) -> None:
        if not self.is_transaction_opened():
            raise SessionError()
        self.transaction.rollback()
        self.transaction = None

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        return self.transaction_factory.create_transaction(isolation_level=isolation_level)

    def is_transaction_opened(self) -> bool:
        return self.transaction is not None
