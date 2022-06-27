from typing import List

from dataclasses import dataclass

from smartleasing.domain.region.CityInfo import CityInfo


@dataclass
class RegionInfo:
    Name: str = None
    ProcessCount: int = None
    Region: str = None
    Cities: List[CityInfo] = None
