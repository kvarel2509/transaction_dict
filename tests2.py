from unittest import TestCase
from model2 import *


class TransactionDictTestCase(TestCase):
    def setUp(self):
        self.transaction_dict = TransactionDict()

    def test_can_set_value(self):
        key, value = 'test_key', 'test_value'
        self.transaction_dict[key] = value
        self.assertEqual(self.transaction_dict[key], value)

    def test_can_reset_value(self):
        key, value1, value2 = 'test_key', 'test_value1', 'test_value2'
        self.transaction_dict[key] = value1
        self.transaction_dict[key] = value2
        self.assertEqual(self.transaction_dict[key], value2)

    def test_can_del_value(self):
        key, value = 'test_key', 'test_value'
        self.transaction_dict[key] = value
        del self.transaction_dict[key]
        with self.assertRaises(KeyError):
            _ = self.transaction_dict[key]

    def test_cannot_del_nonexistent_key(self):
        nonexistent_key = 'nonexistent_key'
        with self.assertRaises(KeyError):
            del self.transaction_dict[nonexistent_key]


class ReadUncommittedTransactionTestCase(TestCase):
    def setUp(self):
        self.journal = Journal()
        self.transaction_factory = TransactionFactory(journal=self.journal)

    def test_can_read_installed_value(self):
        key, value = 'test_key', 'test_value'
        transaction1 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.READ_UNCOMMITTED
        )
        transaction2 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.READ_UNCOMMITTED
        )
        transaction1.start()
        transaction2.start()
        transaction1[key] = value
        self.assertEqual(transaction1[key], value)
        self.assertEqual(transaction2[key], value)


class ReadCommittedTransactionTestCase(TestCase):
    def setUp(self):
        self.journal = Journal()
        self.transaction_factory = TransactionFactory(journal=self.journal)

    def test_can_read_installed_value(self):
        key, value = 'test_key', 'test_value'
        transaction1 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.READ_COMMITTED
        )
        transaction2 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.READ_COMMITTED
        )
        transaction1.start()
        transaction2.start()
        transaction1[key] = value
        self.assertEqual(transaction1[key], value)
        with self.assertRaises(KeyError):
            _ = transaction2[key]

    def test_can_read_installed_value2(self):
        key, value = 'test_key', 'test_value'
        transaction1 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.READ_COMMITTED
        )
        transaction2 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.READ_COMMITTED
        )
        transaction1.start()
        transaction2.start()
        transaction1[key] = value
        transaction1.commit()
        self.assertEqual(transaction1[key], value)
        self.assertEqual(transaction2[key], value)


class SerializableTransactionTestCase(TestCase):
    def setUp(self):
        self.journal = Journal()
        self.transaction_factory = TransactionFactory(journal=self.journal)

    def test_can_read_installed_value(self):
        key, value = 'test_key', 'test_value'
        transaction1 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.SERIALIZABLE
        )
        transaction2 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.SERIALIZABLE
        )
        transaction1.start()
        transaction2.start()
        transaction1[key] = value
        self.assertEqual(transaction1[key], value)
        with self.assertRaises(KeyError):
            _ = transaction2[key]

    def test_can_read_installed_value2(self):
        key, value = 'test_key', 'test_value'
        transaction1 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.SERIALIZABLE
        )
        transaction2 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.SERIALIZABLE
        )
        transaction1.start()
        transaction2.start()
        transaction1[key] = value
        transaction1.commit()
        self.assertEqual(transaction1[key], value)
        with self.assertRaises(KeyError):
            _ = transaction2[key]

    def test_cannot_reset_installed_value(self):
        key, value = 'test_key', 'test_value'
        transaction1 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.SERIALIZABLE
        )
        transaction2 = self.transaction_factory.create_transaction(
            isolation_level=IsolationLevel.SERIALIZABLE
        )
        transaction1.start()
        transaction2.start()
        transaction1[key] = value
        transaction1.commit()
        with self.assertRaises(IntegrityError):
            transaction2[key] = value
