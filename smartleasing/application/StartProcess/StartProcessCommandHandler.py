from injector import inject
from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommandHandler
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.StartProcess.StartProcessCommand import StartProcessCommand
from smartleasing.application.services.BuildingService import BuildingService
from smartleasing.application.services.LeasingService import LeasingService
from smartleasing.application.services.PredictionDataService import PredictionDataService
from smartleasing.application.services.ProcessService import ProcessService


class StartProcessCommandHandler(ICommandHandler[StartProcessCommand]):
    @inject
    def __init__(self,
                 dispatcher: Dispatcher,
                 process_service: ProcessService,
                 leasing_service: LeasingService,
                 building_service: BuildingService,
                 logger: SqlLogger,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.building_service = building_service
        self.leasing_service = leasing_service
        self.dispatcher = dispatcher
        self.process_service = process_service
        self.logger = logger

    def handle(self, command: StartProcessCommand):
        self.logger.debug("Process Operation Started")
        for process_info in command.request.ProcessInfos:
            self.logger.debug("Process prediction started")
            self.process_service.start(process_info=process_info)
            self.logger.debug("Process prediction finished")
        self.logger.debug("Process Operation Finished")
