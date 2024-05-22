from unittest import TestCase

from src.exceptions import AccessError
from src.factory import InMemoryJournalRepositoryFactory, LockStrategyTransactionFactory
from src.model import TransactionDict, IsolationLevel
from src.transactions.lock_strategy import AccessProtector

factory = InMemoryJournalRepositoryFactory()


class AccessProtectorTestCase(TestCase):
    def setUp(self):
        transaction_dict = TransactionDict(
            transaction_factory=LockStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
                access_protector=AccessProtector()
            )
        )
        self.transaction1 = transaction_dict.create_transaction(isolation_level=IsolationLevel.READ_UNCOMMITTED)
        self.transaction2 = transaction_dict.create_transaction(isolation_level=IsolationLevel.READ_UNCOMMITTED)
        self.access_protector = AccessProtector()
        self.key = 'key'

    def test_can_add_full_lock_to_protector_only_with_own_key_locks(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key)
        self.assertIsNone(self.access_protector.add_full_lock(transaction=self.transaction1))

    def test_can_add_full_lock_to_protector_only_with_own_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_full_lock(transaction=self.transaction1))

    def test_cannot_add_full_lock_to_protector_with_other_key_locks(self):
        self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key)
        with self.assertRaises(AccessError):
            self.access_protector.add_full_lock(transaction=self.transaction1)

    def test_cannot_add_full_lock_to_protector_with_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction2)
        with self.assertRaises(AccessError):
            self.access_protector.add_full_lock(transaction=self.transaction1)

    def test_can_add_key_lock_to_protector_only_with_own_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key))

    def test_can_add_key_lock_to_protector_only_with_own_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key))

    def test_cannot_add_key_lock_to_protector_with_other_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key)
        with self.assertRaises(AccessError):
            self.access_protector.add_full_lock(transaction=self.transaction1)

    def test_cannot_add_key_lock_to_protector_with_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction2)
        with self.assertRaises(AccessError):
            self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key)

    def test_can_add_key_lock_after_clear_other_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key)
        self.access_protector.clear_locks_by_transaction(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key))

    def test_can_add_key_lock_after_clear_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.access_protector.clear_locks_by_transaction(transaction=self.transaction1)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key))

    def test_can_add_key_lock_after_del_other_key_lock(self):
        self.access_protector.add_key_lock(transaction=self.transaction1, key=self.key)
        self.access_protector.del_key_lock(key=self.key)
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key))

    def test_can_add_key_lock_after_del_other_full_lock(self):
        self.access_protector.add_full_lock(transaction=self.transaction1)
        self.access_protector.del_full_lock()
        self.assertIsNone(self.access_protector.add_key_lock(transaction=self.transaction2, key=self.key))


class ReadUncommittedTransactionTestCase(TestCase):
    def setUp(self):
        self.key1 = 'test_key1'
        self.key2 = 'test_key2'
        self.value1 = 'test_value1'
        self.value2 = 'test_value2'
        self.value3 = 'test_value3'
        transaction_dict = TransactionDict(
            transaction_factory=LockStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
                access_protector=AccessProtector()
            )
        )
        transaction_dict[self.key1] = self.value1
        self.transaction1 = transaction_dict.create_transaction(isolation_level=IsolationLevel.READ_UNCOMMITTED)
        self.transaction2 = transaction_dict.create_transaction(isolation_level=IsolationLevel.READ_UNCOMMITTED)
        self.transaction1.start()
        self.transaction2.start()

    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value3
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value2)
        self.assertEqual(second_read, self.value2)

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
        self.transaction2[self.key2] = self.value2
        self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(second_read, first_read + 1)


class ReadCommittedTransactionTestCase(TestCase):
    def setUp(self):
        self.key1 = 'test_key1'
        self.key2 = 'test_key2'
        self.value1 = 'test_value1'
        self.value2 = 'test_value2'
        self.value3 = 'test_value3'
        transaction_dict = TransactionDict(
            transaction_factory=LockStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
                access_protector=AccessProtector()
            )
        )
        transaction_dict[self.key1] = self.value1
        self.transaction1 = transaction_dict.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED)
        self.transaction2 = transaction_dict.create_transaction(isolation_level=IsolationLevel.READ_COMMITTED)
        self.transaction1.start()
        self.transaction2.start()

    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value3
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value2)
        self.assertEqual(second_read, self.value2)

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
        self.transaction2[self.key2] = self.value2
        self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(second_read, first_read + 1)


class RepeatableReadTransactionTestCase(TestCase):
    def setUp(self):
        self.key1 = 'test_key1'
        self.key2 = 'test_key2'
        self.value1 = 'test_value1'
        self.value2 = 'test_value2'
        self.value3 = 'test_value3'
        transaction_dict = TransactionDict(
            transaction_factory=LockStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
                access_protector=AccessProtector()
            )
        )
        transaction_dict[self.key1] = self.value1
        self.transaction1 = transaction_dict.create_transaction(isolation_level=IsolationLevel.REPEATABLE_READ)
        self.transaction2 = transaction_dict.create_transaction(isolation_level=IsolationLevel.REPEATABLE_READ)
        self.transaction1.start()
        self.transaction2.start()

    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value3
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value2)
        self.assertEqual(second_read, self.value2)

    def test_cannot_occur_non_repeatable_read(self):
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value2
            self.transaction2.commit()
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value1)
        self.assertEqual(second_read, self.value1)

    def test_can_occur_phantoms_read(self):
        first_read = len(self.transaction1)
        self.transaction2[self.key2] = self.value2
        self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(second_read, first_read + 1)


class SerializableTransactionTestCase(TestCase):
    def setUp(self):
        self.key1 = 'test_key1'
        self.key2 = 'test_key2'
        self.value1 = 'test_value1'
        self.value2 = 'test_value2'
        self.value3 = 'test_value3'
        transaction_dict = TransactionDict(
            transaction_factory=LockStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
                access_protector=AccessProtector()
            )
        )
        transaction_dict[self.key1] = self.value1
        self.transaction1 = transaction_dict.create_transaction(isolation_level=IsolationLevel.SERIALIZABLE)
        self.transaction2 = transaction_dict.create_transaction(isolation_level=IsolationLevel.SERIALIZABLE)
        self.transaction1.start()
        self.transaction2.start()

    def test_cannot_occur_lost_update(self):
        self.transaction1[self.key1] = self.value2
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value3
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value2)
        self.assertEqual(second_read, self.value2)

    def test_cannot_occur_non_repeatable_read(self):
        first_read = self.transaction1[self.key1]
        with self.assertRaises(AccessError):
            self.transaction2[self.key1] = self.value2
            self.transaction2.commit()
        second_read = self.transaction1[self.key1]
        self.assertEqual(first_read, self.value1)
        self.assertEqual(second_read, self.value1)

    def test_can_occur_phantoms_read(self):
        first_read = len(self.transaction1)
        with self.assertRaises(AccessError):
            self.transaction2[self.key2] = self.value2
            self.transaction2.commit()
        second_read = len(self.transaction1)
        self.assertEqual(first_read, second_read)
