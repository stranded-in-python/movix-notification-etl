from abc import ABC, abstractmethod
from typing import Iterable

from config.settings import settings

from .connections import ConnectionManager, PostgresConnectionManager
from .models import User


class Loader(ABC):
    def __init__(self, manager: ConnectionManager):
        self.manager = manager

    @abstractmethod
    def load(self, items: Iterable):
        ...


class PostgresLoader(Loader):
    def __init__(self, manager: PostgresConnectionManager):
        super().__init__(manager)
        self.manager = manager

    def load(self, table: str, items: Iterable[User]):
        user_dict = User.dict()
        field_values = ",".join(
            [" = ".join(item) for item in user_dict.items() if item[0] != "id"]
        )
        sql = (
            f"insert into {settings.schema_to}.{table} ({','.join(user_dict.keys())}) "
            f"values {tuple(user_dict.values())} on conflict (id) "
            f"do update set {field_values};"
        )

        self.manager.execute(sql)
