class TransactionDictException(Exception):
    pass


class TransactionException(TransactionDictException):
    pass


class AccessProtectorException(TransactionException):
    pass


class AccessError(AccessProtectorException):
    pass


class SerializationError(TransactionException):
    pass


class RepositoryError(TransactionDictException):
    pass


class SessionError(TransactionDictException):
    pass
