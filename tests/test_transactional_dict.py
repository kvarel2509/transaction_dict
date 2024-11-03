import itertools
from unittest import TestCase

from src.factory import InMemoryJournalRepositoryFactory, MultiVersionStrategyTransactionFactory
from src.entrypoints.locallib.transaction_dict import TransactionDict

factory = InMemoryJournalRepositoryFactory()


class TransactionDictTestCase(TestCase):
    def setUp(self):
        self.transaction_dict = TransactionDict(
            transaction_factory=MultiVersionStrategyTransactionFactory(
                journal_repository=factory.get_journal_repository(),
            )
        )
        self.key = 'test_key0'
        self.value1 = 'test_value1'
        self.value2 = 'test_value2'
        self.value3 = 'test_value3'
        self.keys_for_creating = ['test_key1', 'test_key2', 'test_key3']
        self.keys_for_removing = ['test_key4', 'test_key5', 'test_key6']
        self.keys_for_recreating = ['test_key7', 'test_key8', 'test_key9']
        self.keys_for_recreating_and_removing = ['test_key10', 'test_key11', 'test_key12']
        self.keys = [
            *self.keys_for_creating,
            *self.keys_for_removing,
            *self.keys_for_recreating,
            *self.keys_for_recreating_and_removing
        ]
        self.actual_keys = [
            *self.keys_for_creating,
            *self.keys_for_recreating
        ]
        self.removed_keys = [
            *self.keys_for_removing,
            *self.keys_for_recreating_and_removing
        ]

    def fill_dict(self) -> TransactionDict:
        for key in self.keys:
            self.transaction_dict[key] = self.value1
        for key in itertools.chain(self.keys_for_recreating, self.keys_for_recreating_and_removing):
            self.transaction_dict[key] = self.value2
        for key in itertools.chain(self.keys_for_removing, self.keys_for_recreating_and_removing):
            del self.transaction_dict[key]
        return self.transaction_dict

    def test_can_create_key(self):
        self.transaction_dict[self.key] = self.value1
        self.assertEqual(self.transaction_dict[self.key], self.value1)

    def test_cannot_read_nonexistent_key(self):
        with self.assertRaises(KeyError):
            _ = self.transaction_dict[self.key]

    def test_can_recreate_key(self):
        self.transaction_dict[self.key] = self.value1
        self.transaction_dict[self.key] = self.value2
        self.assertEqual(self.transaction_dict[self.key], self.value2)

    def test_can_del_key(self):
        self.transaction_dict[self.key] = self.value1
        del self.transaction_dict[self.key]
        with self.assertRaises(KeyError):
            _ = self.transaction_dict[self.key]

    def test_cannot_del_nonexistent_key(self):
        with self.assertRaises(KeyError):
            del self.transaction_dict[self.key]

    def test_can_del_recreated_key(self):
        self.transaction_dict[self.key] = self.value1
        self.transaction_dict[self.key] = self.value2
        del self.transaction_dict[self.key]
        with self.assertRaises(KeyError):
            _ = self.transaction_dict[self.key]

    def test_cannot_del_deleted_key(self):
        self.transaction_dict[self.key] = self.value1
        del self.transaction_dict[self.key]
        with self.assertRaises(KeyError):
            del self.transaction_dict[self.key]

    def test_can_recreate_deleted_key(self):
        self.transaction_dict[self.key] = self.value1
        del self.transaction_dict[self.key]
        self.transaction_dict[self.key] = self.value2
        self.assertEqual(self.transaction_dict[self.key], self.value2)

    def test_iter_method_for_filled_dict(self):
        d = self.fill_dict()
        self.assertListEqual(self.actual_keys, list(d))

    def test_iter_method_for_empty_dict(self):
        self.assertListEqual([], list(self.transaction_dict))

    def test_len_method_for_filled_dict(self):
        d = self.fill_dict()
        self.assertEqual(len(self.actual_keys), len(d))

    def test_len_method_for_empty_dict(self):
        self.assertEqual(len(self.transaction_dict), 0)

    def test_contains_method_for_filled_dict(self):
        d = self.fill_dict()
        for key in self.actual_keys:
            self.assertIn(key, d)
        for key in self.removed_keys:
            self.assertNotIn(key, d)

    def test_keys_method_for_filled_dict(self):
        d = self.fill_dict()
        self.assertListEqual(self.actual_keys, list(d.keys()))

    def test_keys_method_for_empty_dict(self):
        self.assertListEqual([], list(self.transaction_dict.keys()))

    def test_values_method_for_filled_dict(self):
        d = self.fill_dict()
        expected_values = [
            *[self.value1] * len(self.keys_for_creating),
            *[self.value2] * len(self.keys_for_recreating),
        ]
        self.assertListEqual(expected_values, list(d.values()))

    def test_values_method_for_empty_dict(self):
        self.assertListEqual([], list(self.transaction_dict.values()))

    def test_items_method_for_filled_dict(self):
        d = self.fill_dict()
        expected_values = [
            *[(key, self.value1) for key in self.keys_for_creating],
            *[(key, self.value2) for key in self.keys_for_recreating],
        ]
        self.assertListEqual(expected_values, list(d.items()))

    def test_items_method_for_empty_dict(self):
        self.assertListEqual([], list(self.transaction_dict.items()))

    def test_get_method(self):
        d = self.fill_dict()
        for key in self.keys_for_creating:
            self.assertEqual(d.get(key, self.value3), self.value1)
        for key in self.keys_for_recreating:
            self.assertEqual(d.get(key, self.value3), self.value2)
        for key in self.keys_for_removing:
            self.assertEqual(d.get(key, self.value3), self.value3)
        for key in self.keys_for_recreating_and_removing:
            self.assertEqual(d.get(key, self.value3), self.value3)

    def test_pop_method_for_filled_dict(self):
        d = self.fill_dict()
        for key in self.keys_for_creating:
            self.assertEqual(d.pop(key, self.value3), self.value1)
        for key in self.removed_keys:
            with self.assertRaises(KeyError):
                d.pop(key)
        for key in self.keys_for_creating:
            self.assertEqual(d.pop(key, self.value3), self.value3)

    def test_can_popitem_for_created_key(self):
        self.transaction_dict[self.key] = self.value1
        value = self.transaction_dict.popitem()
        self.assertEqual(value, (self.key, self.value1))
        self.assertNotIn(self.key, self.transaction_dict)

    def test_can_popitem_for_recreated_key(self):
        self.transaction_dict[self.key] = self.value1
        self.transaction_dict[self.key] = self.value2
        value = self.transaction_dict.popitem()
        self.assertEqual(value, (self.key, self.value2))
        self.assertNotIn(self.key, self.transaction_dict)

    def test_cannot_popitem_for_empty_dict(self):
        with self.assertRaises(KeyError):
            self.transaction_dict.popitem()

    def test_cannot_popitem_for_removed_key(self):
        self.transaction_dict[self.key] = self.value1
        del self.transaction_dict[self.key]
        with self.assertRaises(KeyError):
            self.transaction_dict.popitem()

    def test_cannot_popitem_for_recreated_and_removed_key(self):
        self.transaction_dict[self.key] = self.value1
        self.transaction_dict[self.key] = self.value2
        del self.transaction_dict[self.key]
        with self.assertRaises(KeyError):
            self.transaction_dict.popitem()

    def test_can_clear_filled_dict(self):
        d = self.fill_dict()
        d.clear()
        self.assertEqual(len(d), 0)

    def test_can_clear_empty_dict(self):
        self.transaction_dict.clear()
        self.assertEqual(len(self.transaction_dict), 0)

    def test_can_update_filled_dict(self):
        d = self.fill_dict()
        d.update({key: self.value3 for key in self.keys})
        self.assertEqual(len(d), len(self.keys))
        for key in self.keys:
            self.assertEqual(d[key], self.value3)

    def test_can_update_empty_dict(self):
        self.transaction_dict.update({self.key: self.value1})
        self.assertEqual(len(self.transaction_dict), 1)
        self.assertEqual(self.transaction_dict[self.key], self.value1)

    def test_setdefault_method_for_filled_dict(self):
        d = self.fill_dict()
        for key in self.keys_for_creating:
            value = d.setdefault(key, self.value3)
            self.assertEqual(value, self.value1)
            self.assertEqual(d[key], self.value1)
        for key in self.keys_for_recreating:
            value = d.setdefault(key, self.value3)
            self.assertEqual(value, self.value2)
            self.assertEqual(d[key], self.value2)
        for key in self.removed_keys:
            value = d.setdefault(key, self.value3)
            self.assertEqual(value, self.value3)
            self.assertEqual(d[key], self.value3)

    def test_setdefault_method_for_empty_dict(self):
        value = self.transaction_dict.setdefault(self.key, self.value3)
        self.assertEqual(value, self.value3)
        self.assertEqual(self.transaction_dict[self.key], self.value3)
