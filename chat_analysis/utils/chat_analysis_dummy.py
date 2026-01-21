"""
Utilities to build deterministic dummy chat-analysis payloads for tests.

The shape mirrors the nested schema used by CHAT_ANALYSIS_SCHEMA so test suites
can bypass the expensive OpenAI pipeline and feed synthetic data directly into
the persistence layer (build_chat_analysis_record / ChatAnalysisRepository).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from configs.log_utils import manila_tz


def make_dummy_analysis_payload(
    *,
    user_id: str = "user-123",
    platform: str = "manychat",
    evaluation_datetime: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Construct a representative chat-analysis payload that satisfies the BigQuery schema.

    Args:
        user_id: Contact identifier to embed in the payload.
        platform: Source platform (e.g. manychat / messenger).
        evaluation_datetime: Optional timestamp string; defaults to Manila now.
        overrides: Optional dict that is deep-merged on top of the base payload.

    Returns:
        Dict[str, Any]: Structured payload ready for build_chat_analysis_record.
    """
    base_eval = (
        evaluation_datetime
        or datetime.now(tz=manila_tz).strftime("%Y-%m-%d %H:%M:%S")
    )

    payload: Dict[str, Any] = {
        "evaluation_datetime": base_eval,
        "user_id": user_id,
        "platform": platform,
        "agent_data": {
            "agent_assigned": "Agent Smith",
            "assigned_datetime": base_eval,
            "agent_last_sender": "Agent Smith",
            "agent_last_sender_datetime": base_eval,
        },
        "extracted_data": {
            "vehicle_make": "Toyota",
            "vehicle_model": "Vios",
            "vehicle_type": "Sedan",
            "vehicle_resolution_source": "LLM",
            "vehicle_model_raw": "Toyota Vios 2019",
            "tire_size_primary": "205/55/R16",
            "tire_sizes_all": ["205/55/R16"],
            "tire_size_front": "205/55/R16",
            "tire_size_rear": "205/55/R16",
            "same_front_rear": "Yes",
            "tire_brand_primary": "Michelin",
            "tire_brand_preference": ["Michelin", "Bridgestone"],
            "asked_promo": "Yes",
            "promo_type": ["Discount"],
            "product_filter_types": ["Brand"],
            "product_filter_raw": "Any Michelin promo?",
            "quantity": 4,
            "contact_number": "+639171234567",
            "location_raw": "Quezon City, Metro Manila",
            "location_city_municipality": "Quezon City",
            "location_province": "Metro Manila",
            "service_type": "Installation",
            "service_schedule_raw": "Tomorrow afternoon",
            "service_schedule_precision": "day",
            "service_schedule_start_iso": "2025-11-24T13:00:00",
            "service_schedule_end_iso": "2025-11-24T15:00:00",
            "service_schedule_urgent": "No",
            "order_confirmed": "Pending",
            "payment_type": "Card",
            "payment_method": "Credit Card",
            "payment_confirmed": "No",
            "out_of_coverage_gma": "No",
            "unserviceable_vehicle_type": "No",
            "sales_stages_completed": ["Discovery", "Pitch"],
            "funnel_path": "lead > discovery > pitch",
            "dropoff_reasons": [],
            "dropoff_other_raw": None,
            "summary": "Customer inquired about tires and installation.",
            "extraction_rationale": "Based on chat transcript.",
        },
        "stats": {
            "num_user_messages": 5,
            "num_chatbot_messages": 3,
            "num_agent_messages": 2,
            "num_exchanges": 10,
            "conversation_start": base_eval,
            "conversation_end": base_eval,
            "avg_customer_response_seconds": 42.5,
            "avg_customer_response_hms": "00:00:42",
            "avg_agent_first_response_seconds": 30.0,
            "avg_agent_first_response_hms": "00:00:30",
            "avg_chatbot_first_response_seconds": 3.0,
            "avg_chatbot_first_response_hms": "00:00:03",
            "avg_agent_response_any_seconds": 45.0,
            "avg_agent_response_any_hms": "00:00:45",
            "avg_chatbot_response_any_seconds": 5.0,
            "avg_chatbot_response_any_hms": "00:00:05",
        },
        "gates": {
            "has_complete_purchase_basics": True,
            "schedule_discussed": True,
            "payment_discussed": True,
            "order_confirmed": False,
            "payment_confirmed": False,
            "unserviceable": False,
            "ooc": False,
        },
        "intent_rating": {
            "intents": ["Purchase"],
            "scorecard": [{"intent": "Purchase", "score": 0.84}],
            "top_intent": "Purchase",
            "top_confidence": 0.84,
            "rationale": "Customer asked about availability and scheduling.",
            "evidence": ["'Can I book installation tomorrow?'"],
        },
        "tokens": {
            "extraction": {
                "model": "gpt-4o-mini",
                "input_tokens": 1200,
                "output_tokens": 600,
                "total_tokens": 1800,
            },
            "intent_rating": {
                "model": "gpt-4o-mini",
                "input_tokens": 600,
                "output_tokens": 300,
                "total_tokens": 900,
            },
            "total": {
                "input_tokens": 1800,
                "output_tokens": 900,
                "total_tokens": 2700,
            },
        },
    }

    if overrides:
        _deep_update(payload, overrides)

    return payload


def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
    """Recursively update nested dicts without mutating override references."""
    for key, value in overrides.items():
        if (
            isinstance(value, dict)
            and isinstance(target.get(key), dict)
        ):
            _deep_update(target[key], value)
        else:
            target[key] = value
