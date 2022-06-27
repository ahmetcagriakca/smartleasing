from typing import List

from dataclasses import dataclass


@dataclass
class CityInfo:
    City: str = None
    Counties: List[str] = None

