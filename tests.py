from unittest import TestCase
import threading

from model import TransactionDict, TransactionIsolationLevel


class TransactionDictTestCase(TestCase):
    def setUp(self):
        self.transaction_dict = TransactionDict()

    def test_can_add_values(self):
        key, value = 'key', 'value'
        self.transaction_dict[key] = value
        self.assertEqual(self.transaction_dict[key], value)

    def test_can_add_value_if_transaction__is_committed(self):
        key, value = 'key', 'value'
        with self.transaction_dict.begin() as transaction:
            transaction[key] = value
            transaction.commit()
        self.assertEqual(self.transaction_dict[key], value)

    def test_cannot_add_value_if_transaction_is_not_committed(self):
        key = 'key'
        old_value, new_value = 'old_value', 'new_value'
        self.transaction_dict[key] = old_value

        with self.transaction_dict.begin() as transaction:
            transaction[key] = new_value

        self.assertEqual(self.transaction_dict[key], old_value)

    def test_can_reset_changes_if_transaction_is_rollback(self):
        key = 'key'
        old_value, new_value = 'old_value', 'new_value'
        self.transaction_dict[key] = old_value

        with self.transaction_dict.begin() as transaction:
            transaction[key] = new_value
            transaction.rollback()
            transaction.commit()

        self.assertEqual(self.transaction_dict[key], old_value)


class ReadCommittedTransactionDictTestCase(TestCase):
    def setUp(self) -> None:
        self.transaction_dict = TransactionDict()
        self.isolation_level = TransactionIsolationLevel.READ_COMMITTED
        self.transaction_1 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )
        self.transaction_2 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )

    def test_can_read_only_their_or_fixed_values(self):
        key = 'key'
        old_value, new_value = 'old_value', 'new_value'
        self.transaction_dict[key] = old_value
        self.transaction_1.__enter__()
        self.transaction_2.__enter__()
        self.transaction_1[key] = new_value
        self.assertEqual(self.transaction_1[key], new_value)
        self.assertEqual(self.transaction_2[key], old_value)
        self.transaction_1.commit()
        self.assertEqual(self.transaction_2[key], new_value)


class ReadUncommittedTransactionDictTestCase(TestCase):
    def setUp(self) -> None:
        self.transaction_dict = TransactionDict()
        self.isolation_level = TransactionIsolationLevel.READ_UNCOMMITTED
        self.transaction_1 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )
        self.transaction_2 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )

    def test_can_read_only_their_or_fixed_values(self):
        key = 'key'
        old_value, new_value = 'old_value', 'new_value'
        self.transaction_dict[key] = old_value
        self.transaction_1.__enter__()
        self.transaction_2.__enter__()
        self.transaction_1[key] = new_value
        self.assertEqual(self.transaction_1[key], new_value)
        self.assertEqual(self.transaction_2[key], new_value)


class SerializableTransactionDictTestCase(TestCase):
    def setUp(self) -> None:
        self.transaction_dict = TransactionDict()
        self.isolation_level = TransactionIsolationLevel.SERIALIZABLE
        self.transaction_0 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )
        self.transaction_1 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )
        self.transaction_2 = self.transaction_dict.transaction_factory.create_transaction(
            isolation_level=self.isolation_level
        )

    def test_can_read_only_their_or_fixed_values(self):
        key = 'key'
        old_value, new_value = 'old_value', 'new_value'
        self.transaction_0.__enter__()
        self.transaction_0[key] = old_value
        self.transaction_0.commit()
        self.transaction_0.__exit__()

        self.transaction_1.__enter__()
        self.transaction_2.__enter__()
        self.transaction_1[key] = new_value
        self.assertEqual(self.transaction_1[key], new_value)
        self.assertEqual(self.transaction_2[key], old_value)
        self.transaction_1.commit()
        self.assertEqual(self.transaction_2[key], old_value)
