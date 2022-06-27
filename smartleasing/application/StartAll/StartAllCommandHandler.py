from injector import inject
from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommandHandler

from smartleasing.application.StartAll.StartAllCommand import StartAllCommand
from smartleasing.application.StartProcess.StartProcessCommand import StartProcessCommand
from smartleasing.application.StartProcess.StartProcessCommandRequest import StartProcessCommandRequest
from smartleasing.application.WriteBaseStationToDatabase.WriteBaseStationToDatabaseCommand import \
    WriteBaseStationToDatabaseCommand
from smartleasing.application.WriteBaseStationToDatabase.WriteBaseStationToDatabaseCommandRequest import \
    WriteBaseStationToDatabaseCommandRequest
from smartleasing.application.WritePredictionToDatabase.WritePredictionToDatabaseCommand import \
    WritePredictionToDatabaseCommand
from smartleasing.application.WritePredictionToDatabase.WritePredictionToDatabaseCommandRequest import \
    WritePredictionToDatabaseCommandRequest


class StartAllCommandHandler(ICommandHandler[StartAllCommand]):
    @inject
    def __init__(self,
                 dispatcher: Dispatcher,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispatcher = dispatcher

    def handle(self, command: StartAllCommand):
        # req = CreateBuildingDataCommandRequest(RecreateBuildingDetail=command.request.RecreateBuildingDetail)
        # create_building_data_command = CreateBuildingDataCommand(request=req)
        # self.dispatcher.dispatch(create_building_data_command)

        req = StartProcessCommandRequest(
            ProcessCount=command.request.ProcessCount, Region=command.request.Region, Cities=command.request.Cities,
            RecreateLeasingCsv=command.request.RecreateLeasingCsv,
            RecreateBuildingCsv=command.request.RecreateBuildingCsv,
            FindAgainAllData=command.request.FindAgainAllData)
        start_process_command = StartProcessCommand(request=req)
        self.dispatcher.dispatch(start_process_command)

        req = WriteBaseStationToDatabaseCommandRequest(
            ProcessCount=command.request.ProcessCount, Region=command.request.Region, Cities=command.request.Cities,
            TruncateBaseStationTable=command.request.TruncateBaseStationTable,
            TruncatePredictionTable=command.request.TruncatePredictionTable,
            WritePredictionToDatabase=command.request.WritePredictionToDatabase,
            WriteBaseStationToDatabase=command.request.WriteBaseStationToDatabase)
        start_write_to_database_command = WriteBaseStationToDatabaseCommand(request=req)
        self.dispatcher.dispatch(start_write_to_database_command)
        req = WritePredictionToDatabaseCommandRequest(
            ProcessCount=command.request.ProcessCount, Region=command.request.Region, Cities=command.request.Cities,
            TruncateBaseStationTable=command.request.TruncateBaseStationTable,
            TruncatePredictionTable=command.request.TruncatePredictionTable,
            WritePredictionToDatabase=command.request.WritePredictionToDatabase,
            WriteBaseStationToDatabase=command.request.WriteBaseStationToDatabase)
        start_write_to_database_command = WritePredictionToDatabaseCommand(request=req)
        self.dispatcher.dispatch(start_write_to_database_command)
