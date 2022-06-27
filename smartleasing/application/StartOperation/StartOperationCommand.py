from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.application.StartOperation.StartOperationCommandRequest import StartOperationCommandRequest


@dataclass
class StartOperationCommand(ICommand):
    request: StartOperationCommandRequest = None
