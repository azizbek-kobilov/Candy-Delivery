from pydantic import BaseModel
from typing import List
import enum
from fastapi import Path


class CourierType(str, enum.Enum):
    foot: str = 'foot'
    bike: str = 'bike'
    car: str = 'car'


class CourierIn(BaseModel):
    courier_id: int = Path(..., ge=0)
    courier_type: CourierType
    regions: List[int] = Path(..., ge=0)
    working_hours: List[str]
