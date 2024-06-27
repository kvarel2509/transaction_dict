from unittest import TestCase
from model2 import *


class TransactionDictTestCase(TestCase):
    def setUp(self):
        self.transaction_dict = TransactionDict()

    def test_can_set_value_some_transaction(self):
        key, value = 'test_key', 'test_value'

        self.assertFalse(key in self.transaction_dict)
        with self.transaction_dict.transaction(isolation_level=IsolationLevel.READ_COMMITTED) as transaction:
            transaction[key] = value
            transaction.commit()
        self.assertTrue(key in self.transaction_dict)
        self.assertEqual(self.transaction_dict[key], value)
