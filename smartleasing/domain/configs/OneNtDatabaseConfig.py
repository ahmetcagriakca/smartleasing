from dataclasses import dataclass
from pdip.configuration.models.base import BaseConfig


@dataclass
class OneNtDatabaseConfig(BaseConfig):
    type: str = None
    connection_string: str = None
    driver: str = None
    host: str = None
    port: int = None
    database: str = None
    user: str = None
    password: str = None
