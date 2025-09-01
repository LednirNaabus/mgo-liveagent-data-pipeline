from core.chat_analysis.schemas.ScoreItem import ScoreItem
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Annotated
from typing import List

class IntentEvaluation(BaseModel):
    """
    Matches the JSON schema:

    {
      "intents": string[] (min 1),
      "scorecard": [{"intent": string, "score": number in [0,1]}] (min 1),
      "top_intent": string,
      "top_confidence": number in [0,1],
      "rationale": string,
      "evidence": string[] (min 0)
    }
    """
    intents: Annotated[List[str], Field(min_length=1)] = Field(..., description="All intent levels from the rubric, ordered low+high")
    scorecard: Annotated[List[ScoreItem], Field(min_length=1)] = Field(..., description="Per-intent confidence scores")
    top_intent: str = Field(..., description="intent with the highest confidence")
    top_confidence: Annotated[float, Field(ge=0.0, le=1.0)] = Field(..., description="Confidence for top_intent")
    rationale: str = Field(..., description="Short explanation (≤5 lines)")
    evidence: List[str] = Field(default_factory=list, description="Concrete items citing fields/timestamps/snippets")

    class Config:
        extra = "forbid"

    @model_validator(mode="after")
    def _enforce_consistency(self):
        if self.top_intent not in self.intents:
            raise ValueError("top_intent must appear in intents")

        scores = {item.intent: float(item.score) for item in self.scorecard}
        missing = [lbl for lbl in self.intents if lbl not in scores]
        if missing:
            raise ValueError(f"scorecard missing intents: {missing}")

        extras = [lbl for lbl in scores if lbl not in self.intents]
        if extras:
            raise ValueError(f"scorecard has unknown intents not present in intens: {extras}")

        if scores.get(self.top_intent) is None:
            raise ValueError("scorecard must include top_intent")
        if abs(scores[self.top_intent] - float(self.top_confidence)) > 1e-9:
            raise ValueError("top_confidence must equal the score assigned to top_intent")
        
        return self