from injector import inject
from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommandHandler
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.CreateProcessData.CreateProcessData import CreateProcessDataCommand
from smartleasing.application.services.BuildingService import BuildingService
from smartleasing.application.services.LeasingService import LeasingService
from smartleasing.application.services.ProcessService import ProcessService


class CreateProcessDataCommandHandler(ICommandHandler[CreateProcessDataCommand]):
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

    def handle(self, command: CreateProcessDataCommand):
        self.logger.debug("Process leasing data to csv started")
        self.leasing_service.write_leasing_data_to_csv(recreate=command.request.RecreateLeasingCsv)
        self.logger.debug("Process leasing data to csv finished")
        if command.request.RecreateBuildingCsv:
            self.logger.debug("Process building data to csv started")
            for process_info in command.request.ProcessInfos:
                for process_info_detail in process_info.Details:
                    self.building_service.write_building_data_to_csv(region=process_info.Region,
                                                                     city=process_info_detail.City,
                                                                     recreate=command.request.RecreateBuildingCsv)
            self.logger.debug("Process building data to csv finished")

