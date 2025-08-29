from core.schemas.specs.SpecSchemas import SchemaSpec
from config.constants import SYSTEM_MSG_1, USER_TMPL
from pydantic import BaseModel, ValidationError
from core.OpenAIClient import OpenAIClient
from typing import Optional, Tuple
from textwrap import dedent
import types
import json
import sys

class ConvoExtractSchema:
    """
    Generate a Pydantic class source from an evolving intent-rating rubric,
    using an OpenAI model. Supports a simple JSON-object path and a strict
    structured-outputs path that enforces the intermediate spec.
    """

    def __init__(
        self,
        intent_prompt: str,
        api_key: str = None,
        model: Optional[str] = "gpt-4o-mini",
        client: OpenAIClient = None
    ):
        self.intent_prompt = intent_prompt
        self.api_key = api_key
        self.model = model if model is not None else "gpt-4.1-mini"
        # self.openai_client = OpenAIClient(api_key).client if client is None else client
        self.openai_client = OpenAIClient(api_key) if client is None else client
        self.system_msg = SYSTEM_MSG_1
        self.user_tmpl = USER_TMPL.format(intent_prompt=self.intent_prompt)

    def create_system_user_prompts(self) -> Tuple[str, str]:
        if self.intent_prompt is None or self.intent_prompt.strip() == "":
            raise ValueError("Intent rubric must be provided and non-empty.")
        return self.system_msg, self.user_tmpl

    async def ask_openai_for_spec(self):
        """Parse rubric into a SchemaSpec using the OpenAI responses API."""
        print("==========user_tmpl=============")
        print(self.user_tmpl)
        prompt = dedent(self.user_tmpl).format(rubric=self.intent_prompt).strip()
        print("==========prompt=============")
        print(prompt)
        resp = await self.openai_client.responses.parse(
            model=self.model,
            input=[
                {"role": "system", "content": self.system_msg},
                {"role": "user", "content": prompt}
            ],
            text_format=SchemaSpec,
            temperature=0,
            max_output_tokens=5000
        )

        try:
            content_text = resp.output_text
        except Exception as e:
            blk = getattr(resp, "output", [])[0]
            content_text = blk.content[0].text

        try:
            return SchemaSpec.model_validate_json(content_text)
        except ValidationError as e:
            raise RuntimeError(f"LLM returned invalid spec.\nRaw\n:{content_text}\n\n{e}") from e

    def render_pydantic_class(self, spec: type[BaseModel]) -> str:
        lines = []
        lines.append("from __future__ import annotations")
        lines.append("from typing import Optional, Literal")
        lines.append("from pydantic import BaseModel, Field")
        lines.append("")
        lines.append(f"class {spec.class_name}(BaseModel):")
        if not spec.fields:
            lines.append("    pass")
            return "\n".join(lines)

        for f in spec.fields:
            py_type = f.py_type.strip()
            if py_type.lower() == "enum":
                # Convert enum spec to Literal['a','b',...]
                if not f.enum_values:
                    raise ValueError(f"Field {f.name} declared enum but no enum_values provided")
                py_type = "Literal[" + ",".join(repr(v) for v in f.enum_values) + "]"

            # Default handling
            default_expr = "..."  # required by default
            if f.default is not None:
                # For strings, quote; for others let json dumps handle safely
                default_expr = json.dumps(f.default)

            # Field line
            lines.append(
                f"    {f.name}: {py_type} = Field({default_expr}, description={json.dumps(f.description)})"
            )
        return "\n".join(lines)

    async def build_model_class_from_source(self):
        """Executes the generated class source in an isolated module and returns the ConvoExtract class object."""
        spec = await self.ask_openai_for_spec()
        source_code = self.render_pydantic_class(spec)
        print("===========source_code:=================\n")
        print(source_code)
        mod = types.ModuleType("convo_extract_schema")
        exec(source_code, mod.__dict__)
        sys.modules[mod.__name__] = mod

        if not hasattr(mod, "ConvoExtract"):
            raise ValueError("Generated source did not define ConvoExtract")
        ConvoExtract = getattr(mod, "ConvoExtract")

        try:
            ConvoExtract.model_rebuild()
        except TypeError:
            ConvoExtract.model_rebuild(_types_namespace=mod.__dict__)
        return ConvoExtract