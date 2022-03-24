from pydantic import BaseModel
from typing import List, Optional

# class SearchCriteria(BaseModel):
#     q: Optional[str]
#     count: Optional[int] = 100
#     hometown: Optional[str]
#     hometown: Optional[str]
#     university_year: Optional[int]
#     online: Optional[int] = 0

class Mapping(BaseModel):
    city: dict
    country: dict

class Config(BaseModel):
    # https://vk.com/dev/users.search
    search_criteria: dict
    search_count: Optional[int] = 100
    # https://vk.com/dev/fields
    fetch_fields: str
    csv_fields: Optional[List[str]] = []
    mapping: Mapping
