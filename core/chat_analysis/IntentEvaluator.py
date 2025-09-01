from core.chat_analysis.schemas.IntentEvaluation import IntentEvaluation
from core.chat_analysis.helpers.derive_intent_prompt import user_msg
from config.prompts import INTENT_EVALUATOR_PROMPT
from core.OpenAIClient import OpenAIClient
from typing import Dict, Any, Optional
from textwrap import dedent

class IntentEvaluator:
    def __init__(
        self,
        rubric_text: str,
        signals: Dict[str, Any],
        *,
        model: str = "gpt-4o-mini",
        client: Optional[OpenAIClient] = None,
        timeout_ms: int = 30_000,
        require_evidence: bool = True
    ) -> Dict[str, Any]:
        if not (rubric_text and rubric_text.strip()):
            raise ValueError("rubric_text must be non-empty.")
        if not isinstance(signals, dict) or not signals:
            raise ValueError("signals must be a non-empty dict.")
        self.rubric_text = rubric_text
        self.signals = signals
        self.model = model
        self.client = client
        self.timeout_ms = timeout_ms
        self.require_evidence = require_evidence
        self.system_message = dedent(INTENT_EVALUATOR_PROMPT)
        self.user_message = dedent(user_msg(self.rubric_text, self.signals, self.require_evidence))

    # call OpenAI Responses API
    async def call_responses_api(self):
        response = await self.client.responses.parse(
            model=self.model,
            input=[
                {"role": "system", "content": self.system_message},
                {"role": "user", "content": self.user_message}
            ],
            text_format=IntentEvaluation,
            temperature=0,
            max_output_tokens=2000,
            timeout=self.timeout_ms
        )
        return response