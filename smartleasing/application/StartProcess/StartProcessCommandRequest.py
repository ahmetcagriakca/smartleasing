from datetime import datetime
from typing import List

from dataclasses import dataclass
from pdip.cqrs.decorators import requestclass

from smartleasing.domain.process.ProcessInfo import ProcessInfo


@requestclass
class StartProcessCommandRequest:
    RunDate: datetime = None
    ProcessInfos: List[ProcessInfo] = None
