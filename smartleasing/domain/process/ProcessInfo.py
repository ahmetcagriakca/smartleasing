from typing import List

from dataclasses import dataclass

from smartleasing.domain.process.ProcessInfoDetail import ProcessInfoDetail



@dataclass
class ProcessInfo:
    Name:str=None
    ProcessCount: int = None
    Region: str = None
    Columns: List[str] = None
    SaveFigures: bool = None
    Details: List[ProcessInfoDetail] = None
