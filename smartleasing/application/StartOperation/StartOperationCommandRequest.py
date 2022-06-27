from pdip.cqrs.decorators import requestclass

from smartleasing.domain.process.ProcessInfo import ProcessInfo
from smartleasing.domain.process.ProcessInfoDetail import ProcessInfoDetail


@requestclass
class StartOperationCommandRequest:
    ProcessId: int = None
    ProcessInfo: ProcessInfo = None
    Detail: ProcessInfoDetail = None
