from unittest import TestCase

from src.exceptions import SerializationError
from src.factory import InMemoryJournalRepositoryFactory, MultiVersionStrategyTransactionFactory
from src.model import TransactionDict, IsolationLevel
from tests.generic import TransactionTestsMixin

factory = InMemoryJournalRepositoryFactory()


class MultiVersionTransactionTestsMixin(TransactionTestsMixin):
    def get_transaction_dict(self) -> TransactionDict:
        return TransactionDict(
            transaction_factory=MultiVersionStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
            )
        )


class CommonTransactionTestCase(MultiVersionTransactionTestsMixin):
    def test_cannot_occur_lost_update_when_concurrent_rewrite_diff_values(self):
        self.transaction1[self.key1] = self.value2
        self.transaction2[self.key1] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_lost_update_when_concurrent_rewrite_delete_some_key(self):
        self.transaction1[self.key1] = self.value2
        del self.transaction2[self.key1]
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_lost_update_when_concurrent_delete_rewrite_some_key(self):
        self.transaction1[self.key1] = self.value2
        del self.transaction2[self.key1]
        self.transaction1.commit()
        with self.assertRaises(SerializationError):
            self.transaction2.commit()

    def test_can_commit_when_concurrent_write_some_value(self):
        self.transaction1[self.key1] = self.value2
        self.transaction2[self.key1] = self.value2
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_can_commit_when_concurrent_delete_some_key(self):
        del self.transaction1[self.key1]
        del self.transaction2[self.key1]
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_can_commit_when_concurrent_write_diff_keys(self):
        self.transaction1[self.key2] = self.value3
        self.transaction2[self.key3] = self.value3
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_cannot_occur_dirty_read(self):
        self.transaction2[self.key1] = self.value2
        self.assertEqual(self.transaction1[self.key1], self.value1)


class ReadCommittedTransactionTestCase(CommonTransactionTestCase, MultiVersionTransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.READ_COMMITTED

    def test_can_occur_non_repeatable_read(self):
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value2)

    def test_can_occur_phantoms_read(self):
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        self.assertIn(self.key3, self.transaction1)
        self.assertEqual(self.transaction1[self.key3], self.value3)


class RepeatableReadTransactionTestCase(CommonTransactionTestCase, MultiVersionTransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.REPEATABLE_READ

    def test_cannot_occur_non_repeatable_read(self):
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value1)

    def test_cannot_occur_phantoms_read(self):
        self.transaction2[self.key3] = self.value2
        self.transaction2.commit()
        self.assertNotIn(self.key3, self.transaction1)


class SerializableTransactionTestCase(CommonTransactionTestCase, MultiVersionTransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.SERIALIZABLE

    def test_cannot_occur_non_repeatable_read(self):
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value1)

    def test_cannot_occur_phantoms_read(self):
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        self.assertNotIn(self.key3, self.transaction1)

    def test_cannot_occur_serializable_error_when_cross_write_keys(self):
        self.transaction1[self.key1] = self.value2
        _ = self.transaction1[self.key2]
        self.transaction2[self.key2] = self.value1
        _ = self.transaction2[self.key1]
        self.transaction1.commit()
        with self.assertRaises(SerializationError):
            self.transaction2.commit()

    def test_can_commit_when_concurrent_read_some_key(self):
        _ = self.transaction1[self.key2]
        _ = self.transaction2[self.key2]
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_cannot_occur_serializable_error_when_write_key_after_check_len(self):
        _ = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_serializable_error_when_cross_read_and_writing(self):
        _ = self.transaction1[self.key1]
        _ = self.transaction1[self.key2]
        self.transaction2[self.key2] = self.value1
        self.transaction2.commit()
        self.transaction1[self.key1] = self.value2
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_serializable_error_when_writing_after_full_iter(self):
        _ = list(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_can_commit_when_read_another_than_part_iter_key(self):
        it = iter(self.transaction1)
        _ = next(it)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        self.assertIsNone(self.transaction1.commit())

    def test_cannot_occur_serializable_error_when_read_part_iter_key(self):
        it = iter(self.transaction1)
        read_key = next(it)
        self.transaction2[read_key] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_can_commit_when_len_is_not_changed(self):
        _ = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        del self.transaction2[self.key2]
        self.transaction2.commit()
        self.assertIsNone(self.transaction1.commit())

    def test_cannot_occur_serializable_error_when_write_key_after_negative_check_contains(self):
        _ = self.key3 in self.transaction1
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_serializable_error_when_write_key_after_positive_check_contains(self):
        _ = self.key2 in self.transaction1
        self.transaction2[self.key2] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()
