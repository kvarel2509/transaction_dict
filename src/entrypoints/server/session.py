from collections.abc import MutableMapping

from src.exceptions import SessionError
from src.domain.core import IsolationLevel, Transaction, TransactionFactory


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
