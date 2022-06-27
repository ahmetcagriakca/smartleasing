from datetime import datetime
from typing import List

from pdip.cqrs import ICommand
from pdip.cqrs.decorators import requestclass


@requestclass
class StartAllCommandRequest:
    RunDate: datetime = None
    ProcessCount: int = None
    Region: str = None
    Cities: List[str] = None
    RecreateLeasingCsv: bool = False
    RecreateBuildingCsv: bool = False
    FindAgainAllData: bool = False
    TruncateBaseStationTable: bool = False
    TruncatePredictionTable: bool = False
    WritePredictionToDatabase: bool = False
    WriteBaseStationToDatabase: bool = False
