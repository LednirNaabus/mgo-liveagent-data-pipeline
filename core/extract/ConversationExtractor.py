from config.constants import PROJECT_ID, DATASET_NAME
from core.BigQueryManager import BigQuery
from typing import List, Dict, Any

import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ConvoExtractor:
    """Extracts a conversation from BigQuery using a specific ticket ID."""
    def __init__(self, ticket_id: str = None):
        self.ticket_id = ticket_id
        self.bq_client = BigQuery()

    def get_convo_str(self) -> str:
        """Get messages from BigQuery messages table and convert them to type string."""
        query = """
        SELECT sender_type, message, message_datecreated
        FROM `{}.{}.messages`
        WHERE ticket_id = '{}' AND message_format = 'T'
        ORDER BY datecreated
        """.format(PROJECT_ID, DATASET_NAME, self.ticket_id)
        df_messages = self.bq_client.sql_query_bq(query)
        s = [
            f"sender: {m['sender_type']}\nmessage: {m['message']}\ndate: {m['message_datecreated']}"
            for _, m in df_messages.iterrows()
        ]
        return "\n\n".join(s)

    @staticmethod
    def parse_conversation(conversation: str) -> List[Dict]:
        pattern = r"sender: (\w+)\nmessage: (.*?)\ndate: (.*?)\n"
        matches = re.findall(pattern, conversation, re.DOTALL)
        parsed = [
            {'role': match[0].lower(), 'content': match[1].strip(), 'datetime': match[2]}
            for match in matches
        ]
        return parsed

    @staticmethod
    def count_role(conversation_list: List[Dict[str, Any]], role: str):
        return sum(1 for r in conversation_list if r.get("role") == role)

    @staticmethod
    def convo_stats(conversation_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        from datetime import datetime
        def parse_dt(s: str) -> datetime:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

        recs = sorted(conversation_list, key=lambda r: parse_dt(r["datetime"]))
        
        num_agent = ConvoExtractor.count_role(recs, "agent")
        num_system = ConvoExtractor.count_role(recs, "system")
        num_user = ConvoExtractor.count_role(recs, "client")
        return num_agent, num_system, num_user