# File: cards_against/manager.py
from .game import Game
from .models import CardPack
from typing import Optional

class GameManager:
    """
    Manages active games keyed by Discord channel ID.
    """
    def __init__(self):
        self.games: dict[int, Game] = {}

    def create_game(self, channel_id: int, packs: list[CardPack]) -> Game:
        if channel_id in self.games:
            raise RuntimeError("Game already exists in this channel.")
        game = Game(channel_id, packs)
        self.games[channel_id] = game
        return game

    def get_game(self, channel_id: int) -> Optional[Game]:
        return self.games.get(channel_id)

    def end_game(self, channel_id: int) -> Optional[Game]:
        return self.games.pop(channel_id, None)
