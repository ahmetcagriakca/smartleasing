from dataclasses import dataclass
from pdip.configuration.models.base import BaseConfig


@dataclass
class OneMapDatabaseConfig(BaseConfig):
    type: str = None
    host: str = None
    port: int = None
    sid: str = None
    service_name: str = None
    user: str = None
    password: str = None
