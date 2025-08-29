from config.constants import PROJECT_ID, DATASET_NAME, SYSTEM_MSG_2
from typing import Optional, List, Dict, Any
from core.OpenAIClient import OpenAIClient
from core.BigQueryManager import BigQuery
from config.config import OPENAI_API_KEY
from pydantic import BaseModel
from textwrap import dedent
import logging
import types
import json
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ConvoDataExtract:
    """
    Dynamic, schema-aware extractor for MechaniGo.ph conversations.

    - Reads an evolving intent rubric and a generated Pydantic model source (ConvoExtract).
    - Builds a prompt that tells the LLM to extract ONLY fields present in ConvoExtract.
    - Optionally enforces the output with Structured Outputs by compiling the class and deriving JSON Schema.
    """

    def __init__(
        self,
        rubric: str,
        schema_class: type[BaseModel],
        messages: List[Dict[str, str]] = [],
        model: str = "gpt-4.1-mini",
        client: Any = None
    ):
        """
        Args:
            rubric: Free-text intent rubric (can evolve).
            schema: Python source code string that defines `class ConvoExtract(BaseModel): ...`.
            messages: Conversation messages as a list of `{"role": "user | assistant | agent", "content": "..."}`.
            model: OpenAI model name.
            client: Optional OpenAI client. If `None`, tries `OpenAI()`; if you have `OpenAIClient().client` it will also be used.
        """
        self.model = model
        self.rubric = rubric
        self.schema_class = schema_class
        self.messages = list(messages) if messages is not None else []
        self.client = OpenAIClient(OPENAI_API_KEY).client if client is None else client
        self.bq_client = BigQuery()
        self._compiled_ns: Optional[Dict] = None
        self._chat_model_cls = None
        self._json_schema: Optional[Dict] = None

    def build_user_prompt(self) -> str:
        """
        Construct a schema-aware prompt that adapts to any rubric and `ConvoExtract` source.
        """
        if not self.rubric:
            raise ValueError("rubric is required")
        if not self.schema_class:
            raise ValueError("schema (ConvoExtract source) is required")

        conversation_json = json.dumps(self.messages, ensure_ascii=False)
        return dedent(f"""
        INTENT RUBRIC (authoritative, may evolve):
        <<<
        {self.rubric}
        >>>

        CHAT ANALYSIS DATA MODEL (authoritative source code to follow verbatim):
        <<< python
        {self.schema_class.model_json_schema()}
        >>>

        CONVERSATION (array):
        <<< JSON
        {conversation_json}
        >>>

        TASK:
        1) Read the rubric and the ConvoExtract source. Treat the model's Field descriptions as your extraction rules.
        2) Extract ONLY what the conversation supports with high confidence.
        3) Return EXACTLY ONE JSON object that conforms to ConvoExtract (no markdown, no comments).
        """
        ).strip()

    async def extract(self) -> Dict[str, Any]:
        """
        Call the LLM to extract a JSON object that conforms to `ConvoExtract`.

        Returns:
            dict representing `ConvoExtract`
        """

        messages = [
            {"role": "system", "content": dedent(SYSTEM_MSG_2).strip()},
            {"role": "user", "content": self.build_user_prompt()}
        ]

        resp = await self.client.responses.parse(
            model=self.model,
            input=messages,
            text_format=self.schema_class,
            temperature=0
        )

        try:
            text = resp.output_text
        except Exception:
            text = json.dumps(resp.output[0].content[0].text)

        return json.loads(text)

    async def extract_validated(self) -> Any:
        """
        Strictly validate by instantiating the compiled `ConvoExtract` class
        with the extracted payload.

        Returns:
            `ConvoExtract`: Pydantic model instance.
        """
        payload = await self.extract()
        ConvoExtract = self.schema_class
        return ConvoExtract(**payload)

    def compile_schema_class(self):
        """Compile the user-provided `ConvoExtract` source into a real class and rebuild types."""
        if self._chat_model_cls is not None:
            return self._chat_model_cls
        if not self.schema_class:
            raise ValueError("schema (ConvoExtract source) is required for strict mode.")

        mod = types.ModuleType("chat_schema_dynamic")
        exec(self.schema_class, mod.__dict__)
        sys.modules[mod.__name__] = mod

        if not hasattr(mod, "ConvoExtract"):
            raise ValueError("Provided schema does not define ConvoExtract.")

        ConvoExtract = getattr(mod, "ConvoExtract")

        try:
            ConvoExtract.model_rebuild()
        except TypeError:
            ConvoExtract.model_rebuild(_types_namespace=mod.__dict__)

        self._compiled_ns = mod.__dict__
        self._chat_model_cls = ConvoExtract
        return ConvoExtract

    def json_schema(self) -> Dict:
        """Derive JSON Schema from the compiled `ConvoExtract` class for Structured Outputs."""
        if self._json_schema is not None:
            return self._json_schema

        ConvoExtract = self.compile_schema_class()
        schema = ConvoExtract.model_json_schema()

        self._json_schema = {
            "name": "ConvoExtract",
            "schema": schema
        }

        return self._json_schema

    def get_convo_str(self, ticket_id: str) -> str:
        """Get messages from BigQuery messages table and convert them to type string."""
        query = """
        SELECT sender_type, message
        FROM `{}.{}.messages`
        WHERE ticket_id = '{}' AND message_format = 'T'
        ORDER BY datecreated
        """.format(PROJECT_ID, DATASET_NAME, ticket_id)
        df_messages = self.bq_client.sql_query_bq(query)
        s = [
            f"sender: {m['sender_type']}\nmessage: {m['message']}"
            for _, m in df_messages.iterrows()
        ]
        return "\n\n".join(s)