import abc

from src.exceptions import SerializationError
from src.journals.journals import CompositeJournal
from src.domain.core import Void, Journal, JournalRepository, Transaction


class MultiVersionStrategyTransaction(Transaction, abc.ABC):
    target_offset: int

    def __init__(self, journal_repository: JournalRepository):
        self.journal_repository = journal_repository
        self.update_target_offset()

    def update_target_offset(self):
        self.target_offset = self.journal_repository.last_offset

    def commit(self):
        self.check_integrity(
            transaction_journal=self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
            ahead_journal=self.journal_repository.get_committed_journal(start_offset=self.target_offset + 1)
        )
        super().commit()
        self.update_target_offset()

    def rollback(self):
        super().rollback()
        self.update_target_offset()

    def check_integrity(self, transaction_journal: Journal, ahead_journal: Journal):
        for key in transaction_journal:
            if key in ahead_journal and ahead_journal[key] != transaction_journal[key]:
                raise SerializationError()


class ReadCommittedMultiVersionStrategyTransaction(MultiVersionStrategyTransaction):
    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
                self.journal_repository.get_committed_journal()
            )
        )


class RepeatableReadMultiVersionStrategyTransaction(MultiVersionStrategyTransaction):
    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
                self.journal_repository.get_committed_journal(end_offset=self.target_offset)
            )
        )


class SerializableMultiVersionStrategyTransaction(MultiVersionStrategyTransaction):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.len_block = False
        self.full_block = False

    def __getitem__(self, item):
        try:
            value = super().__getitem__(item)
            self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=item, value=value)
            return value
        except KeyError:
            self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=item, value=Void())
            raise

    @property
    def state(self) -> CompositeJournal:
        return CompositeJournal(
            journals=(
                self.journal_repository.get_uncommitted_journal_by_transaction(transaction=self),
                self.journal_repository.get_committed_journal(end_offset=self.target_offset)
            )
        )

    def __delitem__(self, key):
        try:
            super().__delitem__(key)
        except KeyError:
            self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=key, value=Void())
            raise

    def __contains__(self, item):
        value = self.state.get(item, Void())
        self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=item, value=value)
        return super().__contains__(item)

    def __len__(self):
        self.len_block = True
        return super().__len__()

    def __iter__(self):
        for item in super().__iter__():
            value = self[item]
            self.journal_repository.add_value_to_uncommitted_journal(transaction=self, key=item, value=value)
            yield item
        self.full_block = True

    def check_integrity(self, transaction_journal: Journal, ahead_journal: Journal):
        if self.full_block and ahead_journal:
            raise SerializationError()
        counter = 0
        if self.len_block:
            for value in ahead_journal.values():
                counter = counter - 1 if isinstance(value, Void) else counter + 1
            if counter:
                raise SerializationError()
        return super().check_integrity(transaction_journal, ahead_journal)
