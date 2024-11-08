from unittest import TestCase

from src.domain.transactions.lock_strategy import AccessProtector
from src.exceptions import AccessError
from src.factory import InMemoryJournalRepositoryFactory, LockStrategyTransactionFactory
from src.domain.core import IsolationLevel
from src.entrypoints.locallib.transaction_dict import TransactionDict
from tests.generic import TransactionTestsMixin

factory = InMemoryJournalRepositoryFactory()


class LockStrategyTransactionTestsMixin(TransactionTestsMixin):
    def setUp(self):
        self.access_protector = AccessProtector()
        super().setUp()

    def get_transaction_dict(self) -> TransactionDict:
        return TransactionDict(
            transaction_factory=LockStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
                access_protector=self.access_protector
            )
        )


class AccessProtectorTestCase(LockStrategyTransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.READ_COMMITTED

    def test_can_add_full_lock_to_protector_only_with_own_key_locks(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1)
        self.assertIsNone(self.access_protector.add_full_lock(transaction=self.transaction1))

    def test_can_add_full_lock_to_protector_only_with_own_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_full_lock(transaction=self.transaction1))

    def test_cannot_add_full_lock_to_protector_with_other_key_locks(self):
        self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key1)
        with self.assertRaises(AccessError):
            self.access_protector.add_full_lock(transaction=self.transaction1)

    def test_cannot_add_full_lock_to_protector_with_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction2)
        with self.assertRaises(AccessError):
            self.access_protector.add_full_lock(transaction=self.transaction1)

    def test_can_add_key_lock_to_protector_only_with_own_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1))

    def test_can_add_key_lock_to_protector_only_with_own_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1))

    def test_cannot_add_key_lock_to_protector_with_other_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key1)
        with self.assertRaises(AccessError):
            self.access_protector.add_full_lock(transaction=self.transaction1)

    def test_cannot_add_key_lock_to_protector_with_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction2)
        with self.assertRaises(AccessError):
            self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1)

    def test_can_add_key_lock_after_clear_other_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1)
        self.access_protector.clear_locks_by_transaction(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key1))

    def test_can_add_key_lock_after_clear_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.access_protector.clear_locks_by_transaction(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key1))

    def test_can_add_key_lock_after_del_other_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key1)
        self.access_protector.del_key_lock(key=self.key1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key1))

    def test_can_add_key_lock_after_del_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.access_protector.del_full_lock()
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key1))


class LostUpdateTestCase(LockStrategyTransactionTestsMixin):
    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value3
        with self.assertRaises(AccessError):
            del self.transaction2[self.key1]
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value2)
        self.assertEqual(second_read, self.value2)


class NonRepeatableReadTestCase(LockStrategyTransactionTestsMixin):
    def test_cannot_occur_non_repeatable_read_when_write_after_read_key(self):
        _ = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value2

    def test_cannot_occur_non_repeatable_read_when_write_after_read_new_key(self):
        with self.assertRaises(KeyError):
            _ = self.transaction1[self.key3]
        with self.assertRaises(AccessError):
            self.transaction2[self.key3] = self.value2

    def test_cannot_occur_non_repeatable_read_when_delete_after_read_key(self):
        _ = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            del self.transaction2[self.key1]

    def test_cannot_occur_non_repeatable_read_when_write_after_positive_check_contains(self):
        self.assertTrue(self.key1 in self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value2

    def test_cannot_occur_non_repeatable_read_when_write_after_negative_check_contains(self):
        self.assertFalse(self.key3 in self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key3] = self.value2


class ReadUncommittedTransactionTestCase(LostUpdateTestCase, LockStrategyTransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.READ_UNCOMMITTED

    def test_can_occur_dirty_read(self):
        first_read = self.transaction1[self.key1]
        self.transaction2[self.key1] = self.value2
        second_read = self.transaction1[self.key1]
        self.transaction2[self.key1] = self.value3
        third_read = self.transaction1[self.key1]
        self.transaction2.rollback()
        fourth_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value1)
        self.assertEqual(second_read, self.value2)
        self.assertEqual(third_read, self.value3)
        self.assertEqual(fourth_read, self.value1)

    def test_can_occur_non_repeatable_read(self):
        first = self.transaction1[self.key1]
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        second = self.transaction1[self.key1]
        self.assertEqual(first, self.value1)
        self.assertEqual(second, self.value2)

    def test_can_occur_phantoms_read(self):
        first_read = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(second_read, first_read + 1)


class ReadCommittedTransactionTestCase(LostUpdateTestCase, LockStrategyTransactionTestsMixin, TestCase):
    isolation_level = IsolationLevel.READ_COMMITTED

    def test_cannot_occur_dirty_read(self):
        first_read = self.transaction1[self.key1]
        self.transaction2[self.key1] = self.value2
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value1)
        self.assertEqual(second_read, self.value1)

    def test_can_occur_non_repeatable_read(self):
        first = self.transaction1[self.key1]
        self.transaction2[self.key1] = self.value2
        self.transaction2.commit()
        second = self.transaction1[self.key1]
        self.assertEqual(first, self.value1)
        self.assertEqual(second, self.value2)

    def test_can_occur_phantoms_read(self):
        first_read = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(second_read, first_read + 1)


class RepeatableReadTransactionTestCase(LostUpdateTestCase,
                                        NonRepeatableReadTestCase,
                                        LockStrategyTransactionTestsMixin,
                                        TestCase):
    isolation_level = IsolationLevel.REPEATABLE_READ

    def test_cannot_occur_non_repeatable_read_when_rewrite_key_after_full_iter(self):
        _ = list(self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value2

    def test_cannot_occur_non_repeatable_read_when_rewrite_key_after_part_iter(self):
        it = iter(self.transaction1)
        key = next(it)
        self.assertEqual(key, self.key1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key2] = self.value2

    def test_can_occur_phantoms_read_when_write_new_key_after_full_iter(self):
        first_read = list(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        second_read = list(self.transaction1)
        self.assertNotEqual(first_read, second_read)

    def test_can_occur_phantoms_read_when_write_new_key_after_check_len(self):
        first_read = len(self.transaction1)
        self.transaction2[self.key3] = self.value3
        self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(second_read, first_read + 1)

    def test_can_read_actual_value_after_commit_other_transaction(self):
        self.transaction2[self.key1] = self.value3
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value3)


class SerializableTransactionTestCase(LostUpdateTestCase,
                                      NonRepeatableReadTestCase,
                                      LockStrategyTransactionTestsMixin,
                                      TestCase):
    isolation_level = IsolationLevel.SERIALIZABLE

    def test_cannot_occur_phantoms_read_when_write_new_key_after_check_len(self):
        _ = len(self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key3] = self.value3

    def test_cannot_occur_phantoms_read_when_rewrite_key_after_check_len(self):
        _ = len(self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key2] = self.value2

    def test_cannot_occur_phantoms_read_when_write_new_key_after_full_iter(self):
        _ = list(self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key3] = self.value3

    def test_cannot_occur_phantoms_read_when_rewrite_key_after_full_iter(self):
        _ = list(self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key2] = self.value3

    def test_cannot_occur_phantoms_read_when_write_new_key_after_part_iter(self):
        it = iter(self.transaction1)
        _ = next(it)
        with self.assertRaises(AccessError):
            self.transaction2[self.key3] = self.value3

    def test_cannot_occur_phantoms_read_when_rewrite_key_after_part_iter(self):
        it = iter(self.transaction1)
        _ = next(it)
        with self.assertRaises(AccessError):
            self.transaction2[self.key2] = self.value2

    def test_can_read_actual_value_after_commit_other_transaction(self):
        self.transaction2[self.key1] = self.value3
        self.transaction2.commit()
        self.assertEqual(self.transaction1[self.key1], self.value3)
