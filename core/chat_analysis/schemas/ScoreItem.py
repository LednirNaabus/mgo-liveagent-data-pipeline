from typing_extensions import Annotated
from pydantic import BaseModel, Field

class ScoreItem(BaseModel):
    intent: str = Field(..., description="Intent intent name")
    score: Annotated[
        float, Field(ge=0.0, le=1.0)
    ] = Field(..., description="Confidence for this intent in [0,1]")
    model_config = {"extra": "forbid"}