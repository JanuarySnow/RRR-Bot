# File: cogs/cah.py
import os
import json
import re
import asyncio
import discord
from discord.ext import commands
from cogs.cards_against.manager import GameManager
from cogs.cards_against.models  import PromptCard, ResponseCard, CardPack


def load_packs() -> list[CardPack]:
    # this file is cogs/cah.py, so __file__ is ".../project_root/cogs/cah.py"
    base_folder = os.path.dirname(__file__)
    deck_folder = os.path.join(base_folder, "cards_against", "decks")

    packs: list[CardPack] = []
    for fname in os.listdir(deck_folder):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(deck_folder, fname), encoding="utf-8") as f:
            data = json.load(f)
        prompts   = [PromptCard(p["text"]) for p in data["prompts"]]
        responses = [ResponseCard(r)    for r in data["responses"]]
        packs.append(CardPack(data["name"], prompts, responses))
    return packs


class CAHCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.manager = GameManager()
        self.packs = load_packs()

    @commands.command()
    async def cah(self, ctx: commands.Context):
        """Start a new Cards Against Humanity game in this channel."""
        if self.manager.get_game(ctx.channel.id):
            return await ctx.send("A CAH game is already running here.")
        game = self.manager.create_game(ctx.channel.id, self.packs)
        await game.start()
        await ctx.send("Game started! Type `!join` to participate.")

    @commands.command()
    async def join(self, ctx: commands.Context):
        game = self.manager.get_game(ctx.channel.id)
        if not game:
            return await ctx.send("No active game. Start one with `!cah`.")
        if game.add_player(ctx.author):
            await ctx.send(f"{ctx.author.display_name} joined the game!")
        else:
            await ctx.send("You are already in the game.")

    @commands.command()
    async def hand(self, ctx: commands.Context):
        game = self.manager.get_game(ctx.channel.id)
        if not game or game.state == game.state.PREGAME:
            return await ctx.send("No cards in hand. Join or wait for the round.")
        player = next((p for p in game.players if p.user.id == ctx.author.id), None)
        if not player:
            return await ctx.send("You’re not in the game.")
        lines = [f"**Your hand:**"] + [f"{i+1}. {card.text}" for i, card in enumerate(player.hand)]
        await ctx.author.send("\n".join(lines))
        await ctx.send("I’ve DM’d your hand!")

    @commands.command()
    async def play(self, ctx: commands.Context, *card_indices: int):
        game = self.manager.get_game(ctx.channel.id)
        if not game or game.state != game.state.INROUND:
            return await ctx.send("No active round to play cards.")
        ok, msg = game.submit_response(ctx.author, list(card_indices))
        if not ok:
            return await ctx.send(msg)
        await ctx.send(f"{ctx.author.display_name} has played their card(s).")
        non_czar = [p for p in game.players if p.user.id != game.current_czar().user.id]
        if len(game.played_responses) >= len(non_czar):
            prompt = game.current_prompt.text
            options = []
            for i, (_, cards) in enumerate(game.played_responses):
                texts = [c.text for c in cards]
                options.append(f"**{i}.** {' / '.join(texts)}")
            embed = discord.Embed(title="Time to pick a winner!", description=prompt)
            embed.add_field(name="Options", value="\n".join(options), inline=False)
            await ctx.send(embed=embed)

    @commands.command()
    async def pick(self, ctx: commands.Context, choice: int):
        game = self.manager.get_game(ctx.channel.id)
        if not game or game.state != game.state.INROUND:
            return await ctx.send("Nothing to judge right now.")
        if ctx.author.id != game.current_czar().user.id:
            return await ctx.send("Only the Czar can pick a winner.")
        ok, msg = game.pick_winner(choice)
        if not ok:
            return await ctx.send(msg)
        winner, _ = game.played_responses[choice]
        await ctx.send(f"🎉 {winner.user.display_name} wins the round! Their score is now {winner.score}.")
        await game.next_round()

async def setup(bot: commands.Bot):
    await bot.add_cog(CAHCog(bot))
