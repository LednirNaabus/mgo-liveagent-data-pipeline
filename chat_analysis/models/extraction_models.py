from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal

class BrandEvent(BaseModel):
    brand: Optional[str] = None
    sku: Optional[str] = None
    quantity: Optional[int] = None
    confirmed: Optional[bool] = None
    role: Optional[Literal["customer", "agent_human", "agent_bot"]] = None
    raw: Optional[str] = None


class ScheduleItem(BaseModel):
    request_raw: Optional[str] = None
    date: Optional[str] = None        # YYYY-MM-DD
    time: Optional[str] = None        # HH:MM:SS

class Followup(BaseModel):
    by: Literal["bot", "human"]
    channel: Literal["messenger", "sms", "email", "call", "viber", "other"]                 # ISO-8601
    template_or_reason: Optional[str] = None
    result: Optional[Literal["replied","no_response","opt_out","converted","other"]] = None
    notes: Optional[str] = None

class LostReason(BaseModel):
    code: Literal[
        "bought_from_competitor","too_pricey","too_far_unserviceable","timing_conflict",
        "stock_unavailable","payment_issue","irate_or_abusive","test_or_mistake",
        "no_longer_needed","unresponsive","other"
    ]
    weight: Optional[float] = None
    evidence: Optional[str] = None

class ExtractedEvent(BaseModel):
    type: str                # e.g., discount_request, price_objection
    role: Optional[Literal["customer","agent_human","agent_bot"]] = None
    raw: Optional[str] = None

# ---------- Namespaces ----------
class Meta(BaseModel, extra='allow'):
    """TOOL-OWNED: extracted_at from time_utils.now_iso()"""
    schema_version: str = "1.3.0"
    conversation_id: str
    extracted_at: str                 # ISO-8601
    model_version: str
    locale: Optional[str] = None
    notes: Optional[str] = None

class InitialInquiry(BaseModel, extra='allow'):
    relevant: bool
    topic: Literal["tires","location","addons","faq","mistake","test","payments","warranty","order_status","other"]
    raw: str
    _confidence: Dict[str, float] = {}
    _provenance: Dict[str, str] = {}

class CustomerDetails(BaseModel, extra='allow'):
    contact_number: Optional[str] = None
    name: Optional[str] = None
    reason_for_buying: Optional[Literal["canvassing","replacement","upgrade","emergency","wholesale","resale","fleet","other", "unknown"]] = "unknown"
    reason_source: Optional[Literal["stated","inferred","unknown"]] = "unknown"
    company_or_wholesale_flag: Optional[bool] = None
    _confidence: Dict[str, float] = {}
    _provenance: Dict[str, str] = {}

class Vehicle(BaseModel, extra='allow'):
    make: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    variant: Optional[str] = None
    _provenance: Dict[str, str] = {}

class Tires(BaseModel, extra='allow'):
    tire_size: Optional[str] = None           # 205/55/R16
    section_width: Optional[int] = None
    aspect_ratio: Optional[int] = None
    rim_size: Optional[int] = None
    requests: List[BrandEvent] = []
    recommendations: List[BrandEvent] = []
    selections: List[BrandEvent] = []
    brand_primary: Optional[str] = None       # denormalized "current best"
    sku_primary: Optional[str] = None
    price_category: Optional[Literal["budget","mid","premium","unknown"]] = "unknown"
    quantity: Optional[int] = None
    promos_requested: List[str] = []
    _confidence: Dict[str, float] = {}
    _provenance: Dict[str, str] = {}
    _candidates: Dict[str, List[str]] = {}
    extras: Dict[str, Any] = {}

class Services(BaseModel, extra='allow'):
    location: Dict[str, Any] = {}             # {city, province, barangay?, raw_text?}
    within_service_area: Optional[Literal["yes","no","unsure"]] = None
    service_type_requested: Optional[Literal["install_at_partner","home_service","delivery_only","pickup_branch","unknown"]] = "unknown"
    installation_partner_name: Optional[str] = None
    service_schedule: List[ScheduleItem] = []
    _provenance: Dict[str, str] = {}

class Payments(BaseModel):
    payment_type: Optional[Literal["pay_now","pay_later","none","unknown"]] = "unknown"
    payment_method: Optional[str] = None
    payment_amount: Optional[float] = None
    payment_confirmation_received: Optional[bool] = None
    _provenance: Dict[str, str] = {}

class ConversationStats(BaseModel, extra='allow'):
    """TOOL-OWNED: populate only via conversation_stats.compute_conversation_stats"""
    messages_total: int
    messages_from_customer: int
    messages_from_agent_human: int
    messages_from_agent_bot: int
    turns_total: int
    first_response_latency_sec: Optional[float] = None
    avg_agent_response_time_sec: Optional[float] = None
    _provenance: Dict[str, str] = {}

class Outcomes(BaseModel, extra='allow'):
    final_outcome: Literal["won","lost","open"] = "open"
    lost_reasons: List[LostReason] = []
    churn_risk: Optional[Literal["low","medium","high"]] = None

# ---------- Top-level ----------
class ConversationExtract(BaseModel, extra='allow'):
    initial_inquiry: InitialInquiry
    customer_details: CustomerDetails
    vehicle: Vehicle
    tires: Tires
    services: Services
    payments: Payments
    outcomes: Outcomes
    followups: List[Followup] = []