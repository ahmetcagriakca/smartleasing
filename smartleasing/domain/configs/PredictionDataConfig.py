from dataclasses import dataclass
from pdip.configuration.models.base import BaseConfig


@dataclass
class PredictionDataConfig(BaseConfig):
    prediction_data_process_count: int = None
    prediction_data_table_name: str = None
    prediction_sdo_data_table_name: str = None
