from injector import inject
from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommandHandler
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.StartOperation.StartOperationCommand import StartOperationCommand
from smartleasing.application.services.BuildingService import BuildingService
from smartleasing.application.services.OperationService import OperationService


class StartOperationCommandHandler(ICommandHandler[StartOperationCommand]):
    @inject
    def __init__(self,
                 dispatcher: Dispatcher,
                 building_service: BuildingService,
                 operation_service: OperationService,
                 logger: SqlLogger,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.building_service = building_service
        self.operation_service = operation_service
        self.dispatcher = dispatcher

    def handle(self, command: StartOperationCommand):
        self.logger.debug("Operation Started")

        self.operation_service.start(process_id=command.request.ProcessId, process_info=command.request.ProcessInfo, detail=command.request.Detail)
