from pydantic import BaseModel
from typing import Literal

class SchemaIntent(BaseModel):
    intent_rating: Literal["No Intent", "Low Intent", "Moderate Intent", "High Intent", "Hot Intent"]