from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.application.StartAll.StartAllCommandRequest import StartAllCommandRequest


@dataclass
class StartAllCommand(ICommand):
    request: StartAllCommandRequest = None
