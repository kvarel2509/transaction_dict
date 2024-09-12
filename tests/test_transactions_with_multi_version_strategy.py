from unittest import TestCase

from src.factory import InMemoryJournalRepositoryFactory, MultiVersionStrategyTransactionFactory
from src.model import TransactionDict, IsolationLevel
from src.exceptions import SerializationError

factory = InMemoryJournalRepositoryFactory()


class TransactionTestsMixin(TestCase):
    isolation_level: IsolationLevel

    def setUp(self):
        self.key1 = 'test_key1'
        self.key2 = 'test_key2'
        self.key3 = 'test_key3'
        self.key4 = 'test_key4'
        self.value1 = 'test_value1'
        self.value2 = 'test_value2'
        self.value3 = 'test_value3'
        self.value4 = 'test_value4'
        transaction_dict = self.get_transaction_dict()
        transaction_dict[self.key1] = self.value1
        transaction_dict[self.key2] = self.value2
        self.transaction1 = transaction_dict.create_transaction(isolation_level=self.isolation_level)
        self.transaction2 = transaction_dict.create_transaction(isolation_level=self.isolation_level)
        self.transaction1.start()
        self.transaction2.start()
        super().setUp()

    def get_transaction_dict(self) -> TransactionDict:
        return TransactionDict(
            transaction_factory=MultiVersionStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
            )
        )


class ReadCommittedTransactionTestCase(TransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.READ_COMMITTED

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

    def test_can_occur_non_repeatable_read(self):
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value2)

    def test_can_occur_phantoms_read(self):
        self.transaction2[self.key2] = self.value2
        self.transaction2.commit()
        self.assertIn(self.key2, self.transaction1)
        self.assertEqual(self.transaction1[self.key2], self.value2)


class RepeatableReadTransactionTestCase(TransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.REPEATABLE_READ

    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        self.transaction2[self.key1] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_dirty_read(self):
        self.transaction2[self.key1] = self.value2
        self.assertEqual(self.transaction1[self.key1], self.value1)

    def test_cannot_occur_non_repeatable_read(self):
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value1)

    def test_cannot_occur_phantoms_read(self):
        self.transaction2[self.key3] = self.value2
        self.transaction2.commit()
        self.assertNotIn(self.key3, self.transaction1)


class SerializableTransactionTestCase(TransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.SERIALIZABLE

    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        self.transaction2[self.key1] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_cannot_occur_dirty_read(self):
        self.transaction2[self.key1] = self.value2
        self.assertEqual(self.transaction1[self.key1], self.value1)

    def test_cannot_occur_non_repeatable_read(self):
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value1)

    def test_cannot_occur_phantoms_read(self):
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        self.assertNotIn(self.key3, self.transaction1)

    def test_cannot_occur_serializable_error_set_key(self):
        self.transaction1[self.key1] = self.value2
        _ = self.transaction1[self.key2]
        self.transaction2[self.key2] = self.value1
        _ = self.transaction2[self.key1]
        self.transaction1.commit()
        with self.assertRaises(SerializationError):
            self.transaction2.commit()

    def test_can_commit_after_read_some_key(self):
        _ = self.transaction1[self.key2]
        _ = self.transaction2[self.key2]
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_can_commit_after_write_some_key_and_eq_value(self):
        self.transaction1[self.key1] = self.value3
        self.transaction2[self.key1] = self.value3
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_cannot_add_key_after_check_len(self):
        _ = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_serializable_error_1(self):
        _ = self.transaction1[self.key1]
        _ = self.transaction1[self.key2]
        self.transaction2[self.key2] = self.value1
        self.transaction2.commit()
        self.transaction1[self.key1] = self.value2
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_serializable_error_2(self):
        self.transaction1[self.key1] = self.value3
        self.transaction2[self.key1] = self.value3
        self.transaction1.commit()
        self.assertIsNone(self.transaction2.commit())

    def test_serializable_error_3(self):
        _ = list(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_serializable_error_4(self):
        _ = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_serializable_error_5(self):
        it = iter(self.transaction1)
        _ = next(it)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        self.assertIsNone(self.transaction1.commit())

    def test_serializable_error_6(self):
        it = iter(self.transaction1)
        read_key = next(it)
        self.transaction2[read_key] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_serializable_error_7(self):
        _ = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        del self.transaction2[self.key2]
        self.transaction2.commit()
        self.assertIsNone(self.transaction1.commit())

    def test_serializable_error_8(self):
        _ = self.key3 in self.transaction1
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()

    def test_serializable_error_9(self):
        _ = self.key2 in self.transaction1
        self.transaction2[self.key2] = self.value3
        self.transaction2.commit()
        with self.assertRaises(SerializationError):
            self.transaction1.commit()
