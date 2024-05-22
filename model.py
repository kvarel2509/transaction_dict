from __future__ import annotations

import abc
import dataclasses
from functools import lru_cache
from typing import Any
from collections import deque


class Deleted:
    pass


@dataclasses.dataclass
class Value:
    value: Any
    transaction_id: int
    is_committed: bool = False
    prev: Value = None


class Journal:
    def __init__(self):
        self._value = None

    def add_value(self, value: Value) -> None:
        if self._value is None:
            self._value = value
        else:
            value.prev = self._value
            self._value = value

    def get_value(self) -> Value:
        return self._value

    def clear_history(self, min_transaction_id: int) -> None:
        last_value = self._value
        if last_value.transaction_id < min_transaction_id:
            self._value = None
        else:
            while last_value.prev and last_value.prev.transaction_id >= min_transaction_id:
                last_value = last_value.prev
            last_value.prev = None

    def commit(self) -> None:
        value = self._value
        while value and not value.is_committed:
            value.is_committed = True
            value = value.prev

    def rollback(self) -> None:
        value = self._value
        while value and not value.is_committed:
            value = value.prev
        self._value = value


class JournalReader:
    def __init__(self, journal: Journal) -> None:
        self._journal = journal

    def get_value(self) -> Value:
        return self._journal.get_value()


class JournalWriter:
    def __init__(self, journal: Journal) -> None:
        self._journal = journal

    def add_value(self, value: Value) -> None:
        self._journal.add_value(value)

    def commit(self) -> None:
        self._journal.commit()

    def rollback(self) -> None:
        self._journal.rollback()


class JournalStaff:
    def __init__(self, journal: Journal) -> None:
        self._journal = journal

    def clear_history(self, min_transaction_id: int) -> None:
        self._journal.clear_history(min_transaction_id)


class JournalWriteAccessManager:
    def __init__(self, journal: Journal) -> None:
        self._journal = journal

    def get_journal_writer(self) -> JournalWriter:
        ...

    def create_journal_writer(self) -> JournalWriter:
        return JournalWriter(self._journal)


class JournalReadAccessManager:
    def __init__(self, journal: Journal) -> None:
        self._journal = journal

    def get_journal_reader(self) -> JournalReader:
        return self.create_journal_reader()

    @lru_cache
    def create_journal_reader(self) -> JournalReader:
        return JournalReader(journal=self._journal)


class JournalStaffAccessManager:
    def __init__(self, journal: Journal) -> None:
        self._journal = journal


class JournalAccessManager:
    def __init__(
            self,
            journal: Journal,
            journal_write_access_manager: JournalWriteAccessManager,
            journal_read_access_manager: JournalReadAccessManager,
            journal_staff_access_manager: JournalStaffAccessManager
    ) -> None:
        self._journal = journal
        self._journal_write_access_manager = journal_write_access_manager
        self._journal_read_access_manager = journal_read_access_manager
        self._journal_staff_access_manager = journal_staff_access_manager


class TransDict:
    def __init__(self):
        self.journals = {}

    def __getitem__(self, item):
        print('__getitem__', item)
        return self.journals[item]

    def __setitem__(self, key, value):
        print('__setitem__', key, value)
        if key in self.journals:
            self.journals[key] = value
        else:
            self.journals[key] = Journal()
            self.journals[key].add_entry(value)


a = TransDict()
a['1'] = 1
print()
a['1'] += 1

