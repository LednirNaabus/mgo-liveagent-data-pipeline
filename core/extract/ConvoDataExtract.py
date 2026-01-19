from core.schemas.ConvoResponse import ResponseSchema
from config.constants import PROJECT_ID, DATASET_NAME
from config.constants import CHATGPT_PROMPT
from core.BigQueryManager import BigQuery
from config.config import OPENAI_API_KEY, GEMINI_API_KEY
from core.LLMGateway import LLMGateway
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
        gemini_api_key: str = None,
        temperature: float = 0.8
    ):
        self.llm_gateway = None
        self.temperature = temperature
        self.bq_client = BigQuery()
        self.ticket_id = ticket_id
        self.prompt = None
        self.data = None
        self.conversation_text = None
        self.api_key = api_key
        self.gemini_api_key = gemini_api_key

    @classmethod
    async def create(
        cls,
        ticket_id: str = None,
        api_key: str = None,
        gemini_api_key: str = None,
        temperature: float = 0.8
    ):
        self = cls(
            ticket_id=ticket_id,
            api_key=api_key,
            gemini_api_key=gemini_api_key,
            temperature=temperature
        )
        
        self.llm_gateway = await self.create_llm_gateway(
            api_key,
            gemini_api_key
        )
        
        if ticket_id:
            today = datetime.today().strftime("%Y-%m-%d")
            self.conversation_text = self.get_convo_str(ticket_id)
            logging.info(
                f"Conversation text length: "
                f"{len(self.conversation_text) if self.conversation_text else 0}"
            )
            self.prompt = CHATGPT_PROMPT.format(
                conversation_text=self.conversation_text,
                current_date=today
            )
        
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

    async def create_llm_gateway(
        self,
        api_key: str = None,
        gemini_api_key: str = None
    ) -> LLMGateway:
        """
        Create and initialize the LLM Gateway with provided or environment API keys.
        """
        openai_key = api_key or OPENAI_API_KEY
        gemini_key = gemini_api_key or GEMINI_API_KEY
        
        try:
            gateway = LLMGateway(
                openai_api_key=openai_key,
                gemini_api_key=gemini_key,
                temperature=self.temperature
            )
            logging.info("LLM Gateway initialized successfully.")
            return gateway
        except Exception as e:
            logging.error(f"Failed to initialize LLM Gateway: {e}")
            raise RuntimeError(
                "Failed to initialize LLM Gateway with available API keys."
            ) from e

    async def analyze_convo(self) -> Dict:
        if not self.prompt:
            raise Exception("Prompt not specified.")

        messages = [
            {
                "role": "user",
                "content": self.prompt
            }
        ]

        try:
            response = await self.llm_gateway.completion(
                messages=messages,
                response_format=ResponseSchema
            )
            
            parsed_data = json.loads(response["content"])
            
            if not isinstance(parsed_data, dict):
                logging.error(f"Parsed data is not a dict: {type(parsed_data)}")
                parsed_data = {}
            
            expected_fields = {
                "service_category": None,
                "summary": None,
                "intent_rating": None,
                "engagement_rating": None,
                "clarity_rating": None,
                "resolution_rating": None,
                "sentiment_rating": None,
                "location": None,
                "schedule_date": None,
                "schedule_time": None,
                "car": None,
                "contact_num": None,
                "payment": None,
                "inspection": None,
                "quotation": None,
                "model": None
            }
            
            complete_data = {**expected_fields, **{k: v for k, v in parsed_data.items() if k in expected_fields}}
            
            unexpected = set(parsed_data.keys()) - set(expected_fields.keys())
            if unexpected:
                logging.warning(f"Unexpected fields in LLM response (ignored): {unexpected}")
            
            return {
                "data": complete_data,
                "tokens": response["tokens"],
                "model": response["model"]
            }
            
        except Exception as e:
            output = {
                "data": {
                    "service_category": None,
                    "summary": None,
                    "intent_rating": None,
                    "engagement_rating": None,
                    "clarity_rating": None,
                    "resolution_rating": None,
                    "sentiment_rating": None,
                    "location": None,
                    "schedule_date": None,
                    "schedule_time": None,
                    "car": None,
                    "contact_num": None,
                    "payment": None,
                    "inspection": None,
                    "quotation": None,
                    "model": None
                },
                "tokens": self._count_tokens(self.prompt),
                "model": "fallback_error"
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