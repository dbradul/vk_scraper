from pydantic import BaseModel
from typing import List, Optional


class Mapping(BaseModel):
    city: dict
    country: dict

class Config(BaseModel):
    # https://vk.com/dev/users.search
    search_criteria: dict
    search_count: Optional[int] = 100
    # https://vk.com/dev/fields
    fetch_fields: List[str]
    csv_fields: List[str]
    custom_csv_fields: Optional[List[str]] = []

class VkResponse(BaseModel):
    count: int
    items: List[dict]
