from unittest import TestCase

from model import TransactionDict


class TestTransactionDict(TestCase):
    def setUp(self):
        self.transaction_dict_1 = TransactionDict()
        self.transaction_dict_2 = TransactionDict()

    def test_can_use_global_state(self):
        key, value = 'key', 'value'
        self.transaction_dict_1[key] = value
        self.assertEqual(self.transaction_dict_2[key], value)

    def test_can_use_begin_mode(self):
        key = 'key'
        value1, value2 = 'value1', 'value2'

        self.transaction_dict_2[key] = value2

        with self.transaction_dict_1:
            self.transaction_dict_1[key] = value1

            self.assertEqual(self.transaction_dict_1[key], value1)
            self.assertEqual(self.transaction_dict_2[key], value2)

            self.transaction_dict_1.commit()

        self.assertEqual(self.transaction_dict_2[key], value1)
