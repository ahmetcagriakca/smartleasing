from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommand
from pdip.data.decorators import transactionhandler
from pdip.dependency.container import DependencyContainer
from pdip.logging.loggers.database import SqlLogger


class JobService:
    @transactionhandler
    def start(self, command: ICommand):
        logger = DependencyContainer.Instance.get(SqlLogger)
        try:
            logger.debug("started")
            DependencyContainer.Instance.get(Dispatcher).dispatch(command)
        except Exception as ex:
            logger = DependencyContainer.Instance.get(SqlLogger)

            logger.exception(ex,f"Getting error on {command.__class__.__name__}")
        finally:
            logger.debug("finished")

    @staticmethod
    def job_start(command: ICommand):
        job_service = JobService()
        job_service.start(command=command)
        del job_service
