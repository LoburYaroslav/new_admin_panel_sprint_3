from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class PgMergedData(BaseModel):
    fw_id: UUID  # id фильма
    id: Optional[UUID]  # id актера (может быть что их изначально не будет, но загрузить без них в принципе можно)
    title: str
    description: Optional[str] = ''
    rating: Optional[float] = 0.0
    type: Optional[str]
    role: Optional[str]
    full_name: Optional[str]
    name: Optional[str]