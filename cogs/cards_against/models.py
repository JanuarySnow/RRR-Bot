from enum import Enum
from typing import List

class GameState(Enum):
    PREGAME = 0
    PREROUND_DELAY = 1
    INROUND = 2
    POSTROUND_DELAY = 3
    ENDED = 4

class PromptCard:
    def __init__(self, text: str):
        self.text = text
        # Number of response slots is the count of "{}" placeholders
        self.num_responses = text.count("{}")

class ResponseCard:
    def __init__(self, text: str):
        self.text = text

class CardPack:
    def __init__(self, name: str, prompts: List[PromptCard], responses: List[ResponseCard]):
        self.name = name
        self.prompts = prompts
        self.responses = responses