from datetime import datetime
from typing import List

from dataclasses import dataclass
from pdip.cqrs import ICommand

from smartleasing.domain.region.RegionInfo import RegionInfo


@dataclass
class WriteBaseStationToDatabaseCommandRequest(ICommand):
    RunDate: datetime = None
    SchemaAndTable: str = None
    Regions: List[RegionInfo] = None
    TruncateTable: bool = False
