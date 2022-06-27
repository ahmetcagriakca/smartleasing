from dataclasses import dataclass
from pdip.configuration.models.base import BaseConfig


@dataclass
class ProcessConfig(BaseConfig):
    process_count: int = None
