from core.extract.ConvoDataExtract import ConvoDataExtract
from config.prompts import SYSTEM_MSG_1, USER_TMPL
from typing import Optional

class ConvoExtractSchema:
    """"""
    def __init__(
        self,
        intent_prompt: str,
        model: Optional[str] = "gpt-4o-mini", # default model
        client: ConvoDataExtract = None
    ):
        self.intent_prompt = intent_prompt
        self.model = model if model is not None else "gpt-4.1.-mini"
        self.client = client
        self.system_msg = SYSTEM_MSG_1
        self.user_tmpl = USER_TMPL.format(intent_prompt=self.intent_prompt)