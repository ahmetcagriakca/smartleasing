from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.application.WriteBaseStationToDatabase.WriteBaseStationToDatabaseCommandRequest import \
    WriteBaseStationToDatabaseCommandRequest


@dataclass
class WriteBaseStationToDatabaseCommand(ICommand):
    request: WriteBaseStationToDatabaseCommandRequest = None
