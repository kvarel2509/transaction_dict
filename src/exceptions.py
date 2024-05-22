class TransactionDictException(Exception):
    pass


class TransactionException(TransactionDictException):
    pass


class AccessError(TransactionException):
    pass


class SerializationError(TransactionException):
    pass


class RepositoryError(TransactionDictException):
    pass
