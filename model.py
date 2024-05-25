from __future__ import annotations

import abc
import dataclasses
import uuid
from typing import Any, Hashable
from collections import deque


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
    def __init__(self, journal_writers: list[JournalWriter]) -> None:
        self._journal_writers = journal_writers
        self._candidates = deque()

    def get_journal_writer(self) -> JournalWriter:
        journal_writer = self._execute_journal_writer()
        return journal_writer

    def revert_journal_writer(self, journal_writer: JournalWriter):
        self._journal_writers.append(journal_writer)

    def _execute_journal_writer(self) -> JournalWriter:
        candidate_identifier = uuid.uuid4()
        self._candidates.append(candidate_identifier)
        while self._candidates.index(candidate_identifier) > 0:
            continue
        while True:
            try:
                journal_writer = self._journal_writers.pop()
            except IndexError:
                continue
            else:
                self._candidates.popleft()
                return journal_writer


class JournalReadAccessManager:
    def __init__(self, journal_reader: JournalReader) -> None:
        self._journal_reader = journal_reader

    def get_journal_reader(self) -> JournalReader:
        return self._journal_reader

    def revert_journal_reader(self, journal_reader: JournalReader):
        pass


class JournalStaffAccessManager:
    def __init__(self, journal_staff: JournalStaff) -> None:
        self._journal_staff = journal_staff

    def get_journal_staff(self) -> JournalStaff:
        return self._journal_staff

    def revert_journal_staff(self, journal_staff: JournalStaff):
        pass


@dataclasses.dataclass
class JournalAccessManagerSet:
    journal_write_access_manager: JournalWriteAccessManager
    journal_read_access_manager: JournalReadAccessManager
    journal_staff_access_manager: JournalStaffAccessManager


class AccessManager:
    def __init__(self):
        self._journal_access_manager_set: dict[Hashable, JournalAccessManagerSet] = {}

    def get_journal_reader(self, key) -> JournalReader:
        journal_access_manager_set = self._journal_access_manager_set[key]
        return journal_access_manager_set.journal_read_access_manager.get_journal_reader()

    def revert_journal_reader(self, key, journal_reader: JournalReader) -> None:
        journal_access_manager_set = self._journal_access_manager_set[key]
        journal_access_manager_set.journal_read_access_manager.revert_journal_reader(journal_reader=journal_reader)

    def get_journal_writer(self, key) -> JournalWriter:
        journal_access_manager_set = self._get_or_create_journal_access_manager_set(key=key)
        return journal_access_manager_set.journal_write_access_manager.get_journal_writer()

    def revert_journal_writer(self, key, journal_writer: JournalWriter) -> None:
        journal_access_manager_set = self._journal_access_manager_set[key]
        journal_access_manager_set.journal_write_access_manager.revert_journal_writer(journal_writer=journal_writer)

    def get_journal_staff(self, key) -> JournalStaff:
        journal_access_manager_set = self._journal_access_manager_set[key]
        return journal_access_manager_set.journal_staff_access_manager.get_journal_staff()

    def revert_journal_staff(self, key, journal_staff: JournalStaff) -> None:
        journal_access_manager_set = self._journal_access_manager_set[key]
        journal_access_manager_set.journal_staff_access_manager.revert_journal_staff(journal_staff=journal_staff)

    def _get_or_create_journal_access_manager_set(self, key) -> JournalAccessManagerSet:
        journal_access_manager_set = self._journal_access_manager_set.get(key)
        if not journal_access_manager_set:
            journal_access_manager_set = self._create_journal_access_manager_set()
            self._journal_access_manager_set[key] = journal_access_manager_set
        return journal_access_manager_set

    def _create_journal_access_manager_set(self) -> JournalAccessManagerSet:
        journal = Journal()
        return JournalAccessManagerSet(
            journal_read_access_manager=JournalReadAccessManager(
                journal_reader=JournalReader(journal=journal),
            ),
            journal_write_access_manager=JournalWriteAccessManager(
                journal_writers=[JournalWriter(journal=journal)]
            ),
            journal_staff_access_manager=JournalStaffAccessManager(
                journal_staff=JournalStaff(journal=journal)
            )
        )


class Transaction(abc.ABC):
    def __init__(self, access_manager: AccessManager, transaction_id):
        self._access_manager = access_manager
        self._transaction_id = transaction_id

    def __enter__(self):
        self._writers: dict[Hashable, JournalWriter] = {}
        self._readers: dict[Hashable, JournalReader] = {}
        return self

    def __exit__(self, *args, **kwargs):
        for key, writer in self._writers.items():
            writer.rollback()
            self._access_manager.revert_journal_writer(key=key, journal_writer=writer)
        for key, reader in self._readers.items():
            self._access_manager.revert_journal_reader(key=key, journal_reader=reader)

    def __getitem__(self, item):
        reader = self._readers.get(item)
        if not reader:
            reader = self._access_manager.get_journal_reader(item)
            self._readers[item] = reader
        value = reader.get_value()
        return self.execute_value(value=value)

    def __setitem__(self, key, value):
        writer = self._writers.get(key)
        if not writer:
            writer = self._access_manager.get_journal_writer(key)
            self._writers[key] = writer
        value = Value(
            value=value,
            transaction_id=self._transaction_id
        )
        writer.add_value(value=value)

    def commit(self):
        for writer in self._writers.values():
            writer.commit()

    def rollback(self):
        for writer in self._writers.values():
            writer.rollback()

    @abc.abstractmethod
    def execute_value(self, value: Value):
        ...


class ReadCommittedTransaction(Transaction):
    def execute_value(self, value: Value):
        cursor_value = value
        while cursor_value:
            if cursor_value.is_committed or cursor_value.transaction_id == self._transaction_id:
                return cursor_value.value
            else:
                cursor_value = value.prev
        raise KeyError()


access_manager = AccessManager()
with ReadCommittedTransaction(access_manager=access_manager, transaction_id=1) as d:
    d[1] = 1
    print(d[1])
    d.commit()

with ReadCommittedTransaction(access_manager=access_manager, transaction_id=2) as d:
    print(d[1])

