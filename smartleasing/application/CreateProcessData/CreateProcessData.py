from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.application.CreateProcessData.CreateProcessDataCommandRequest import CreateProcessDataCommandRequest


@dataclass
class CreateProcessDataCommand(ICommand):
    request: CreateProcessDataCommandRequest = None
