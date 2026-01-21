from pydantic import BaseModel
from typing import List, Optional, Dict

class IntentBlock(BaseModel):
    intent_rating: str  # No/Low/Moderate/High/Hot
    confidence: float
    reasoning: str
    missing_for_high_intent: List[str] = []

class FunnelBlock(BaseModel):
    stage: str                # Awareness|Consideration|Decision|Purchase
    dropoff_stage: str
    microconversions: List[str] = []
    microconversion_score: int

class LifecycleRecon(BaseModel):
    suggested_state: Optional[str] = None

class RaterOutputV1(BaseModel, extra='allow'):
    meta: Dict[str, str]      # { rater_version, source_schema_version }
    intent: IntentBlock
    funnel: FunnelBlock
    lifecycle_reconciliation: LifecycleRecon
    next_best_action: Dict