from dataclasses import dataclass
from pdip.configuration.models.base import BaseConfig


@dataclass
class BuildingConfig(BaseConfig):
    building_process_count: int = None
    base_table_name: str = None
    building_table_name: str = None
    building_detail_table_name: str = None
    building_query: str = None

