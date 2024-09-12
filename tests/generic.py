import abc

from src.model import TransactionDict, IsolationLevel


class TransactionTestsMixin(abc.ABC):
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
        self.transaction1 = transaction_dict.create_transaction(isolation_level=self.get_isolation_level())
        self.transaction2 = transaction_dict.create_transaction(isolation_level=self.get_isolation_level())
        self.transaction1.start()
        self.transaction2.start()
        super().setUp()

    @abc.abstractmethod
    def get_transaction_dict(self) -> TransactionDict:
        ...

    def get_isolation_level(self) -> IsolationLevel:
        return self.isolation_level
