from core.extract.helpers.extraction_helpers import recent_tickets
from config.prompts import INTENT_RUBRIC, RUBRIC_PROMPT
from config.constants import PROJECT_ID, DATASET_NAME
from core.chat_analysis import (
    SchemaAwareExtractor,
    ConvoExtractSchema,
    ConvoExtractor,
    IntentEvaluator
)
from core.OpenAIClient import OpenAIClient
from core.BigQueryManager import BigQuery
from typing import List, Dict
import asyncio
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ConversationPipeline:
    def __init__(
        self,
        openai_client: OpenAIClient,
        conversation: str,
        conversation_stats: List[Dict],
        rubric: str,
        intent_prompt: str,
        model: str = "gpt-4o-mini"
    ):
        self.openai_client = openai_client
        self.conversation = conversation
        self.conversation_stats = conversation_stats
        self.rubric = rubric
        self.intent_prompt = intent_prompt
        self.model = model
        self.rendered = None

    async def build_schema(self):
        """Generate a schema class from the rubric prompt."""
        schema_generator = ConvoExtractSchema(
            intent_prompt=self.rubric,
            model="gpt-4.1-mini",
            client=self.openai_client
        )
        self.schema_class = await schema_generator.build_model_class_from_source()
        parsed = await schema_generator.ask_open_ai_for_spec()
        rendered = schema_generator.render_pydantic_class(parsed)
        self.rendered = rendered

    async def extract_signals(self):
        """Extract structured data and conversation stats."""
        extractor = SchemaAwareExtractor(
            rubric=self.intent_prompt,
            schema_class=self.schema_class,
            messages=self.conversation,
            client=self.openai_client,
            model=self.model
        )

        extracted = await extractor.extract_validated()

        return {
            "extracted_data": extracted.model_dump(),
            "stats": self.conversation_stats
        }

    async def evaluate_intent(self, signals):
        """Run intent evaluation and generate scorecard."""
        intent_evaluator = IntentEvaluator(
            rubric_text=self.intent_prompt,
            signals=signals,
            client=self.openai_client,
            model=self.model
        )

        response, result = await intent_evaluator.call_responses_api()
        score_card = await intent_evaluator.generate_scorecard(response, result)
        return score_card

    async def run(self):
        """Execute the full pipeline and return final results."""
        await self.build_schema()
        signals = await self.extract_signals()
        results = await self.evaluate_intent(signals)
        return results

async def process_tickets(
    openai_client: OpenAIClient,
    bq_client: BigQuery
):
    async def run_pipeline(ticket_id):
        logging.info(f"Ticket ID: {ticket_id}")
        convo_extractor = ConvoExtractor(bq_client, ticket_id)
        raw_convo = convo_extractor.get_convo_str()
        convo_parsed = convo_extractor.parse_conversation(raw_convo)
        convo_stats = convo_extractor.convo_stats(convo_parsed)

        pipeline = ConversationPipeline(
            openai_client=openai_client,
            conversation=convo_parsed,
            conversation_stats=convo_stats,
            rubric=RUBRIC_PROMPT,
            intent_prompt=INTENT_RUBRIC
        )

        try:
            result = await pipeline.run()
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return ticket_id, result
        except Exception as e:
            print(f"Exception occurred while running pipeline: {e}")
            return ticket_id, None

    chats = recent_tickets(
        bq_client=bq_client,
        project_id=PROJECT_ID,
        dataset_name=DATASET_NAME,
        table_name="messages",
        date_filter="datecreated",
        limit=1
    )
    ticket_ids = chats["ticket_id"].to_list()
    logging.info(f"Number of tickets: {len(ticket_ids)}")
    tasks = [run_pipeline(ticket_id) for ticket_id in ticket_ids]
    results = await asyncio.gather(*tasks)
    return {
        ticket_id: result
        for ticket_id, result in results
    }

async def start_pipeline(api_key: str, bq_client: BigQuery):
    openai = OpenAIClient(api_key)
    openai_client = await openai.init_async_client()
    return await process_tickets(openai_client, bq_client)