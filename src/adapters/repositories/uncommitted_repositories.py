from typing import Hashable, Any

from src.exceptions import RepositoryError
from src.domain.journals import LeafJournal, CompositeJournal, MutableJournal
from src.domain.core import Journal, UncommittedRepository, Transaction


class InMemoryUncommittedRepository(UncommittedRepository):
    def __init__(self):
        self.data: dict[Transaction, MutableJournal] = {}

    def create_journal(self, transaction: Transaction) -> None:
        if transaction in self.data:
            raise RepositoryError("Transaction journal already exists")
        self.data[transaction] = MutableJournal()

    def get_journal(self, transaction: Transaction = None) -> Journal:
        if transaction is None:
            return CompositeJournal(journals=self.data.values())
        else:
            return LeafJournal(journal=self.data[transaction])

    def add_value_to_journal(self, transaction: Transaction, key: Hashable, value: Any):
        self.data[transaction][key] = value

    def delete_journal(self, transaction: Transaction) -> None:
        del self.data[transaction]
