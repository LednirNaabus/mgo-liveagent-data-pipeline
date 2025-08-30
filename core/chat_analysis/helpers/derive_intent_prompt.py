from typing import Dict, Any
import json

def user_msg(rubric_text: str, signals: Dict[str, Any], require_evidence: bool):
    prompt = f"""
        INTENT RATING RUBRIC (authoritative):
    <<<
    {rubric_text}
    >>>

    SIGNALS (combined extracted + computed conversation data):
    <<<JSON
    {json.dumps(signals, ensure_ascii=False)}
    >>>

    TASK:
    1) Derive the full intent set from the rubric (preserve the intent text and order).
    2) Assign a confidence score in [0,1] to EACH intent; scorecard should sum to 1.0.
    3) Choose top_intent = argmax(scorecard) and top_confidence = scorecard[top_intent].
    4) Provide a short rationale and { '2–5' if require_evidence else '0–5' } evidence items
       (citing concrete fields/timestamps/snippets from SIGNALS).

    OUTPUT:
    Return a single JSON object with EXACTLY these keys and shapes:

    - intents: string[]
    • At least 1 item.
    • Must list all intent levels derived from the rubric in order (lowest → highest).

    - scorecard: [intent, score][]
    • An array (min 1 item), where each item is an object:
        {{ "intent": string, "score": number in [0,1] }}
    • Include one entry per intent in `intents`.
    • Scores should sum to ~1.0 (small rounding error acceptable).
    • Do not include any extra properties in each item.

    - top_intent: string
    • Must be one of the intents in `intents`.

    - top_confidence: number in [0,1]
    • Must equal the score assigned to `top_intent`.

    - rationale: string
    • Short explanation (≤5 lines).

    - evidence: string[]
    • Zero or more concise items citing concrete fields/timestamps/snippets from SIGNALS.

    No additional keys. Respond with JSON only.
    """
    return prompt