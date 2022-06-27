from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.application.WritePredictionToDatabase.WritePredictionToDatabaseCommandRequest import \
    WritePredictionToDatabaseCommandRequest


@dataclass
class WritePredictionToDatabaseCommand(ICommand):
    request: WritePredictionToDatabaseCommandRequest = None
