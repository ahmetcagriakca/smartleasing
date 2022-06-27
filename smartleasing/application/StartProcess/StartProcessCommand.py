from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.application.StartProcess.StartProcessCommandRequest import StartProcessCommandRequest


@dataclass
class StartProcessCommand(ICommand):
    request: StartProcessCommandRequest = None
