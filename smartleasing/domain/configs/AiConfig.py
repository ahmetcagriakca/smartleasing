from dataclasses import dataclass
from pdip.configuration.models.base import BaseConfig


@dataclass
class AiConfig(BaseConfig):
    columns: int = None
