from openai import AsyncOpenAI, AuthenticationError, OpenAIError
from core.schemas.ConvoResponse import ResponseSchema
from config.constants import PROJECT_ID, DATASET_NAME
from config.constants import CHATGPT_PROMPT
from core.BigQueryManager import BigQuery
from config.config import OPENAI_API_KEY
from datetime import datetime
from typing import Dict
import tiktoken
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ConvoDataExtract:
    """Class for Conversation Data Extraction and Analysis."""
    def __init__(
        self,
        ticket_id: str = None,
        api_key: str = None,
        temperature: float = 0.8
    ):
        self.openai_client = None
        self.model = 'gpt-4.1-mini'
        self.temperature = temperature
        self.bq_client = BigQuery()
        self.ticket_id = ticket_id
        self.prompt = None
        self.data = None
        self.conversation_text = None
        self.api_key = api_key

    @classmethod
    async def create(cls, ticket_id: str = None, api_key: str = None, temperature: float = 0.8):
        self = cls(ticket_id=ticket_id, api_key=api_key, temperature=temperature)
        self.openai_client = await self.create_openai_client(api_key)
        if ticket_id:
            today = datetime.today().strftime("%Y-%m-%d")
            self.conversation_text = self.get_convo_str(ticket_id)
            logging.info(f"Conversation text length: {len(self.conversation_text) if self.conversation_text else 0}")
            self.prompt = CHATGPT_PROMPT.format(conversation_text=self.conversation_text, current_date=today)
        if self.conversation_text:
            self.data = await self.analyze_convo()
        return self

    def _count_tokens(self, text: str) -> int:
        try:
            encoder = tiktoken.encoding_for_model("gpt-4o-mini")
            num_tokens = len(encoder.encode(text))
        except Exception as e:
            logging.error(f"Exception occurred while counting tokens: {e}")
            logging.warning("'num_tokens' is set to 0.")
            num_tokens = 0
        return num_tokens

    async def create_openai_client(self, api_key: str = None) -> AsyncOpenAI:
        key_sources = [
            ("provided", api_key),
            ("env", OPENAI_API_KEY)
        ]

        for source, key in key_sources:
            if not key:
                continue
            try:
                client = AsyncOpenAI(api_key=key)
                await client.models.list()
                logging.info(f"OpenAI client initialized using {source} key.")
                logging.info(f"Client: {client}")
                return client
            except (AuthenticationError, OpenAIError) as e:
                logging.error(f"Failed with {source} key: {e}")
                continue

        raise RuntimeError("Failed to initialize OpenAI client with all available API keys.")

    async def analyze_convo(self) -> Dict:
        if not self.prompt:
            raise Exception("Prompt not specified.")

        messages = [
            {
                "role": "system",
                "content": self.prompt
            }
        ]

        try:
            response = await self.openai_client.beta.chat.completions.parse(
                model=self.model,
                messages=messages,
                response_format=ResponseSchema
            )

            return {
                "data": json.loads(response.choices[0].message.content),
                "tokens": response.usage.total_tokens
            }
        except Exception as e:
            output = {
                "data": {
                    "service_category": None,
                    "car": None,
                    "location": None,
                    "summary": None,
                    "intent_rating": None,
                    "engagement_rating": None,
                    "clarity_rating": None,
                    "resolution_rating": None,
                    "sentiment_rating": None
                },
                "tokens": self._count_tokens(self.prompt)
            }
            logging.error(f"Exception occurred while analyzing convo: {e}")
            return output

    def get_convo_str(self, ticket_id: str) -> str:
        """Get messages from BigQuery messages table and convert them to type string."""
        query = """
        SELECT sender_type, message
        FROM `{}.{}.messages`
        WHERE ticket_id = '{}' 
            AND message_type = 'M' AND message_format = 'T'
        ORDER BY datecreated
        """.format(PROJECT_ID, DATASET_NAME, ticket_id)
        df_messages = self.bq_client.sql_query_bq(query)
        s = [
            f"sender: {m['sender_type']}\nmessage: {m['message']}"
            for _, m in df_messages.iterrows()
        ]
        return "\n\n".join(s)