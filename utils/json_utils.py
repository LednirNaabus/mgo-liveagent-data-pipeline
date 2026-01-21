from __future__ import annotations

import json
import re
from typing import Any

def clean_json_text(value: Any) -> str:
    """
    Normalize text that should contain JSON.

    - Strips fenced code blocks (```json ... ```).
    - Extracts the first JSON object/array if wrapped in extra text.
    - Preserves valid JSON strings.
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", "ignore")
        except Exception:
            return ""
    if not isinstance(value, str):
        return json.dumps(value)

    cleaned = value.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    if cleaned[0] in ("{", "["):
        return cleaned

    first_obj = cleaned.find("{")
    first_arr = cleaned.find("[")
    starts = [idx for idx in (first_obj, first_arr) if idx >= 0]
    if not starts:
        return cleaned

    start = min(starts)
    end = cleaned.rfind("}" if cleaned[start] == "{" else "]")
    if end > start:
        return cleaned[start : end + 1].strip()
    return cleaned