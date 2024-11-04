from __future__ import annotations

from typing import MutableMapping

from src.domain.core import IsolationLevel, TransactionFactory, Transaction


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
