from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Entry(BaseModel):
    id: UUID
    modified: datetime


class User(BaseModel):
    id: UUID
    username: str
    first_name: str | None
    last_name: str | None
    is_verifed: bool
    is_active: bool
    updated_at: datetime
