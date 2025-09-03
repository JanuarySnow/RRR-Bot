import random
import asyncio
from .models import GameState, PromptCard, ResponseCard
from typing import List, Tuple

class Player:
    def __init__(self, user):
        self.user = user              # discord.Member
        self.hand: List[ResponseCard] = []
        self.score: int = 0

class Game:
    def __init__(self, channel_id: int, packs: List):
        self.channel_id = channel_id
        self.packs = packs
        self.players: List[Player] = []
        self.state = GameState.PREGAME
        self.black_deck: List[PromptCard] = []
        self.white_deck: List[ResponseCard] = []
        self.current_prompt: PromptCard | None = None
        self.played_responses: List[Tuple[Player, List[ResponseCard]]] = []
        self.czar_index: int = 0
        self.loop = asyncio.get_event_loop()

    def add_player(self, user) -> bool:
        if any(p.user.id == user.id for p in self.players):
            return False
        self.players.append(Player(user))
        return True

    def remove_player(self, user) -> None:
        self.players = [p for p in self.players if p.user.id != user.id]

    def init_decks(self) -> None:
        all_prompts = []
        all_responses = []
        for pack in self.packs:
            all_prompts.extend(pack.prompts)
            all_responses.extend(pack.responses)
        random.shuffle(all_prompts)
        random.shuffle(all_responses)
        self.black_deck = all_prompts
        self.white_deck = all_responses

    def deal_hands(self, hand_size: int = 7) -> None:
        for player in self.players:
            while len(player.hand) < hand_size and self.white_deck:
                player.hand.append(self.white_deck.pop())

    def current_czar(self) -> Player:
        return self.players[self.czar_index % len(self.players)]

    async def start(self) -> None:
        self.init_decks()
        self.deal_hands()
        self.state = GameState.PREROUND_DELAY
        await asyncio.sleep(3)
        await self.start_round()

    async def start_round(self) -> None:
        if not self.black_deck:
            self.state = GameState.ENDED
            return
        self.state = GameState.INROUND
        self.current_prompt = self.black_deck.pop()
        self.played_responses.clear()
        # Here you should notify players of the prompt via your Discord cog
        # and wait for them to play cards via Game.submit_response

    def submit_response(self, user, indices: List[int]) -> Tuple[bool, str]:
        """
        Player plays `num_responses` cards by 1-based `indices` from their hand.
        Returns (success, message_or_empty).
        """
        if self.state != GameState.INROUND:
            return False, "No round is currently active."
        czar = self.current_czar().user.id
        if user.id == czar:
            return False, "The Czar cannot play cards."
        if any(p.user.id == user.id for p, _ in self.played_responses):
            return False, "You have already played this round."
        assert self.current_prompt is not None
        if len(indices) != self.current_prompt.num_responses:
            return False, f"This prompt requires {self.current_prompt.num_responses} response(s)."
        player = next(p for p in self.players if p.user.id == user.id)
        try:
            chosen = [player.hand[i-1] for i in indices]
        except Exception:
            return False, "Invalid card indices."
        for card in chosen:
            player.hand.remove(card)
        self.played_responses.append((player, chosen))
        return True, ""

    def pick_winner(self, winner_idx: int) -> Tuple[bool, str]:
        if self.state != GameState.INROUND:
            return False, "No round to judge."
        if not (0 <= winner_idx < len(self.played_responses)):
            return False, "Invalid choice."
        winning_player, _ = self.played_responses[winner_idx]
        winning_player.score += 1
        self.state = GameState.POSTROUND_DELAY
        return True, ""

    async def next_round(self) -> None:
        self.czar_index += 1
        self.deal_hands()
        self.state = GameState.PREROUND_DELAY
        await asyncio.sleep(3)
        await self.start_round()