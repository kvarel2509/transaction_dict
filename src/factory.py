from src.adapters.repositories.committed_repositories import InMemoryCommittedRepository
from src.adapters.repositories.uncommitted_repositories import InMemoryUncommittedRepository
from src.exceptions import TransactionLevelIsNotImplemented
from src.domain.core import JournalRepository, IsolationLevel, Transaction, TransactionFactory, JournalRepositoryFactory
from src.domain.transactions.lock_strategy import AccessProtector, ReadUncommittedLockStrategyTransaction, \
    ReadCommittedLockStrategyTransaction, RepeatableReadLockStrategyTransaction, SerializableLockStrategyTransaction
from src.domain.transactions.multi_version_strategy import ReadCommittedMultiVersionStrategyTransaction, \
    RepeatableReadMultiVersionStrategyTransaction, SerializableMultiVersionStrategyTransaction


class InMemoryJournalRepositoryFactory(JournalRepositoryFactory):
    def get_journal_repository(self) -> JournalRepository:
        return JournalRepository(
            uncommitted_repository=InMemoryUncommittedRepository(),
            committed_repository=InMemoryCommittedRepository(),
        )


class LockStrategyTransactionFactory(TransactionFactory):
    def __init__(self, journal_repository: JournalRepository, access_protector: AccessProtector):
        self._journal_repository = journal_repository
        self._access_protector = access_protector

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        match isolation_level:
            case IsolationLevel.READ_UNCOMMITTED:
                return ReadUncommittedLockStrategyTransaction(
                    journal_repository=self._journal_repository,
                    access_protector=self._access_protector,
                )
            case IsolationLevel.READ_COMMITTED:
                return ReadCommittedLockStrategyTransaction(
                    journal_repository=self._journal_repository,
                    access_protector=self._access_protector,
                )
            case IsolationLevel.REPEATABLE_READ:
                return RepeatableReadLockStrategyTransaction(
                    journal_repository=self._journal_repository,
                    access_protector=self._access_protector,
                )
            case IsolationLevel.SERIALIZABLE:
                return SerializableLockStrategyTransaction(
                    journal_repository=self._journal_repository,
                    access_protector=self._access_protector,
                )
            case _:
                raise TransactionLevelIsNotImplemented()


class MultiVersionStrategyTransactionFactory(TransactionFactory):
    def __init__(self, journal_repository: JournalRepository):
        self._journal_repository = journal_repository

    def create_transaction(self, isolation_level: IsolationLevel) -> Transaction:
        match isolation_level:
            case IsolationLevel.READ_COMMITTED:
                return ReadCommittedMultiVersionStrategyTransaction(
                    journal_repository=self._journal_repository,
                )
            case IsolationLevel.REPEATABLE_READ:
                return RepeatableReadMultiVersionStrategyTransaction(
                    journal_repository=self._journal_repository,
                )
            case IsolationLevel.SERIALIZABLE:
                return SerializableMultiVersionStrategyTransaction(
                    journal_repository=self._journal_repository,
                )
            case _:
                raise TransactionLevelIsNotImplemented()
