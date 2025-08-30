from config.constants import PROJECT_ID, DATASET_NAME
from typing import List, Dict, Any, Optional
from core.BigQueryManager import BigQuery
from datetime import datetime
from statistics import mean

import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ConvoExtractor:
    """
    Extracts a conversation from BigQuery using a specific ticket ID.

    Args:
        `ticket_id` (str): Expects a Ticket ID from BigQuery (`ticket_id` column)
    """
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
        pattern = r"sender: (\w+)\nmessage: (.*?)\ndate: (.*?)(?=\nsender:|\Z)"
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
    def parse_dt(s: str):
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def convo_stats(conversation_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        recs = sorted(conversation_list, key=lambda r: ConvoExtractor.parse_dt(r["datetime"]))

        # TODO:
        # - extract logic of counting message exchanges and computing average customer response time into helper functions

        # count messages for each sender
        num_agent = ConvoExtractor.count_role(recs, "agent")
        num_system = ConvoExtractor.count_role(recs, "system")
        num_user = ConvoExtractor.count_role(recs, "client")

        # exchanges: user message that gets at least one non-user reply before next user message
        exchanges = 0
        i = 0
        while i < len(recs):
            if recs[i].get("role") == "client":
                j, got_reply = i + 1, False
                while j < len(recs) and recs[j].get("role") != "client":
                    if recs[j].get("role") in ("system", "agent"):
                        got_reply = True
                    j += 1
                exchanges += 1 if got_reply else 0
                i = j
            else:
                i += 1

        start_dt = ConvoExtractor.parse_dt(recs[0]["datetime"]) if recs else None
        end_dt = ConvoExtractor.parse_dt(recs[-1]["datetime"]) if recs else None

        # compute average customer response time (seconds)
        deltas = []
        last_time: Optional[datetime] = None
        for r in recs:
            t = ConvoExtractor.parse_dt(r["datetime"])
            role = r.get("role")
            if role in ("system", "agent"):
                last_time = t
            elif role == "client" and last_time is not None:
                delta = (t - last_time).total_seconds()
                if delta >= 0:
                    deltas.append(delta)
                last_time = None # only count the first reply

        avg_secs = mean(deltas) if deltas else None
        avg_hms = (
            None if avg_secs is None else
            f"{int(avg_secs//3600):02d}:{int((avg_secs%3600)//60):02d}:{int(avg_secs%60):02d}"
        )

        return {
            "num_agent_messages": num_agent,
            "num_system_messages": num_system,
            "num_user_messages": num_user,
            "num_exchanges": exchanges,
            "start_message_datetime": start_dt.isoformat(sep=" ") if start_dt else None,
            "end_message_datetime": end_dt.isoformat(sep=" ") if end_dt else None,
            "avg_client_response_seconds": avg_secs,
            "avg_client_response_hms": avg_hms
        }