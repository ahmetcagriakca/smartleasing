from typing import List

from dataclasses import dataclass


@dataclass
class ProcessInfoDetail:
    PredictForCity: bool = None
    City: str = None
    Counties: List[str] = None
    ClusterRate: int = None
    EnrichData: bool = None
