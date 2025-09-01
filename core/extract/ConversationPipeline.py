from core.chat_analysis.SchemaAwareExtractor import SchemaAwareExtractor
from core.chat_analysis.ConvoExtractSchema import ConvoExtractSchema
from core.chat_analysis.ConversationExtractor import ConvoExtractor
from core.chat_analysis.IntentEvaluator import IntentEvaluator
from core.OpenAIClient import OpenAIClient
import json

class ConversationPipeline:
    def __init__(self, api_key, convo_id, intent_prompt, schema_model="gpt-4o-mini", eval_model="gpt-4.1-mini"):
        self.api_key = api_key
        self.convo_id = convo_id
        self.intent_prompt = intent_prompt
        self.schema_model = schema_model
        self.eval_model = eval_model
        self.openai_client = None
        self.schema_class = None

    async def init_client(self):
        """Initialize the OpenAI client asynchronously."""
        self.openai_client = await OpenAIClient(self.api_key).init_async_client()

    async def build_schema(self):
        """Generate a schema class from the rubric prompt."""
        schema_generator = ConvoExtractSchema(
            intent_prompt=self.intent_prompt,
            client=self.openai_client
        )
        self.schema_class = await schema_generator.build_model_class_from_source()

    async def extract_signals(self):
        """Extract structured data and conversation stats."""
        ce = ConvoExtractor(self.convo_id)
        conversation = ce.get_convo_str()
        conversation_parsed = ce.parse_conversation(conversation)

        extractor = SchemaAwareExtractor(
            rubric=self.intent_prompt,
            schema_class=self.schema_class,
            messages=conversation_parsed,
            client=self.openai_client,
            model=self.schema_model
        )

        extracted = await extractor.extract_validated()
        conversation_stats = ce.convo_stats(conversation_parsed)

        return {
            "extracted_data": extracted.model_dump(),
            "stats": conversation_stats
        }

    async def evaluate_intent(self, signals):
        """Run intent evaluation and generate scorecard."""
        intent_evaluator = IntentEvaluator(
            rubric_text=self.intent_prompt,
            signals=signals,
            client=self.openai_client,
            model=self.eval_model
        )

        response, result = await intent_evaluator.call_responses_api()
        cleaned = await intent_evaluator.generate_scorecard(response, result)
        return cleaned

    async def run(self):
        """Execute the full pipeline and return final results."""
        await self.init_client()
        await self.build_schema()
        signals = await self.extract_signals()
        results = await self.evaluate_intent(signals)
        return results