from pydantic import BaseModel
from typing import List
from fastapi import Path


class OrderIn(BaseModel):
    order_id: int = Path(..., ge=0)
    weight: float = Path(..., ge=0.01, le=50)
    region: int = Path(..., ge=0)
    delivery_hours: List[str]
